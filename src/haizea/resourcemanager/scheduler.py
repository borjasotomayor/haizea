# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #


"""This module provides the main classes for Haizea's scheduler, particularly
the Scheduler class. Note that this class includes high-level scheduling
constructs, and that most of the magic happens in the slottable module. The
deployment scheduling code (everything that has to be done to prepare a lease)
happens in the modules inside the haizea.resourcemanager.deployment package.

This module provides the following classes:

* SchedException: A scheduling exception
* ReservationEventHandler: A simple wrapper class
* Scheduler: Do I really need to spell this one out for you?
"""

import haizea.resourcemanager.datastruct as ds
import haizea.common.constants as constants
from haizea.resourcemanager.slottable import SlotTable, SlotFittingException
from haizea.resourcemanager.deployment.unmanaged import UnmanagedDeployment
from haizea.resourcemanager.deployment.predeployed import PredeployedImagesDeployment
from haizea.resourcemanager.deployment.imagetransfer import ImageTransferDeployment
from haizea.resourcemanager.datastruct import ARLease, ImmediateLease, VMResourceReservation 
from haizea.resourcemanager.resourcepool import ResourcePool, ResourcePoolWithReusableImages
from operator import attrgetter, itemgetter

import logging

class SchedException(Exception):
    """A simple exception class used for scheduling exceptions"""
    pass

class ReservationEventHandler(object):
    """A wrapper for reservation event handlers.
    
    Reservations (in the slot table) can start and they can end. This class
    provides a convenient wrapper around the event handlers for these two
    events (see Scheduler.__register_handler for details on event handlers)
    """
    def __init__(self, on_start, on_end):
        self.on_start = on_start
        self.on_end = on_end

class Scheduler(object):
    """The Haizea Scheduler
    
    Public methods:
    schedule -- The scheduling function
    process_reservations -- Processes starting/ending reservations at a given time
    enqueue -- Queues a best-effort request
    is_queue_empty -- Is the queue empty?
    exists_scheduled_leases -- Are there any leases scheduled?
    
    Private methods:
    __schedule_ar_lease -- Schedules an AR lease
    __schedule_besteffort_lease -- Schedules a best-effort lease
    __preempt -- Preempts a lease
    __reevaluate_schedule -- Reevaluate the schedule (used after resources become
                             unexpectedly unavailable)
    _handle_* -- Reservation event handlers
    
    """
    def __init__(self, rm):
        self.rm = rm
        self.logger = logging.getLogger("SCHED")
        if self.rm.config.get("diskimage-reuse") == constants.REUSE_IMAGECACHES:
            self.resourcepool = ResourcePoolWithReusableImages(self)
        else:
            self.resourcepool = ResourcePool(self)
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        self.pending_leases = []
            
        for n in self.resourcepool.get_nodes() + self.resourcepool.get_aux_nodes():
            self.slottable.add_node(n)
            
        self.handlers = {}
        
        self.register_handler(type     = ds.VMResourceReservation, 
                              on_start = Scheduler._handle_start_vm,
                              on_end   = Scheduler._handle_end_vm)

        self.register_handler(type     = ds.SuspensionResourceReservation, 
                              on_start = Scheduler._handle_start_suspend,
                              on_end   = Scheduler._handle_end_suspend)

        self.register_handler(type     = ds.ResumptionResourceReservation, 
                              on_start = Scheduler._handle_start_resume,
                              on_end   = Scheduler._handle_end_resume)
            
        deploy_type = self.rm.config.get("lease-preparation")
        if deploy_type == constants.DEPLOYMENT_UNMANAGED:
            self.deployment = UnmanagedDeployment(self)
        elif deploy_type == constants.DEPLOYMENT_PREDEPLOY:
            self.deployment = PredeployedImagesDeployment(self)
        elif deploy_type == constants.DEPLOYMENT_TRANSFER:
            self.deployment = ImageTransferDeployment(self)

        backfilling = self.rm.config.get("backfilling")
        if backfilling == constants.BACKFILLING_OFF:
            self.maxres = 0
        elif backfilling == constants.BACKFILLING_AGGRESSIVE:
            self.maxres = 1
        elif backfilling == constants.BACKFILLING_CONSERVATIVE:
            self.maxres = 1000000 # Arbitrarily large
        elif backfilling == constants.BACKFILLING_INTERMEDIATE:
            self.maxres = self.rm.config.get("backfilling-reservations")

        self.numbesteffortres = 0
    
    def schedule(self, nexttime):        
        ar_leases = [req for req in self.pending_leases if isinstance(req, ARLease)]
        im_leases = [req for req in self.pending_leases if isinstance(req, ImmediateLease)]
        self.pending_leases = []
        
        # Process immediate requests
        for lease_req in im_leases:
            self.__process_im_request(lease_req, nexttime)

        # Process AR requests
        for lease_req in ar_leases:
            self.__process_ar_request(lease_req, nexttime)
            
        # Process best-effort requests
        self.__process_queue(nexttime)
        
    
    def process_reservations(self, nowtime):
        starting = [l for l in self.scheduledleases.entries.values() if l.has_starting_reservations(nowtime)]
        ending = [l for l in self.scheduledleases.entries.values() if l.has_ending_reservations(nowtime)]
        for l in ending:
            rrs = l.get_ending_reservations(nowtime)
            for rr in rrs:
                self._handle_end_rr(l, rr)
                self.handlers[type(rr)].on_end(self, l, rr)
        
        for l in starting:
            rrs = l.get_starting_reservations(nowtime)
            for rr in rrs:
                self.handlers[type(rr)].on_start(self, l, rr)

        util = self.slottable.getUtilization(nowtime)
        self.rm.accounting.append_stat(constants.COUNTER_CPUUTILIZATION, util)
        # TODO: Should be moved to rm.py
        self.rm.accounting.tick()
        
        
    def register_handler(self, type, on_start, on_end):
        handler = ReservationEventHandler(on_start=on_start, on_end=on_end)
        self.handlers[type] = handler        
    
    def enqueue(self, lease_req):
        """Queues a best-effort lease request"""
        self.rm.accounting.incr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
        self.queue.enqueue(lease_req)
        self.logger.info("Received (and queueing) best-effort lease request #%i, %i nodes for %s." % (lease_req.id, lease_req.numnodes, lease_req.duration.requested))

    def add_pending_lease(self, lease_req):
        """
        Adds a pending lease request, to be scheduled as soon as
        the scheduling function is called. Unlike best-effort leases,
        if one these leases can't be scheduled immediately, it is
        rejected (instead of being placed on a queue, in case resources
        become available later on).
        """
        self.pending_leases.append(lease_req)

    def is_queue_empty(self):
        """Return True is the queue is empty, False otherwise"""
        return self.queue.is_empty()

    
    def exists_scheduled_leases(self):
        """Return True if there are any leases scheduled in the future"""
        return not self.scheduledleases.is_empty()    

    def cancel_lease(self, lease_id):
        """Cancels a lease.
        
        Arguments:
        lease_id -- ID of lease to cancel
        """
        time = self.rm.clock.get_time()
        
        self.logger.info("Cancelling lease %i..." % lease_id)
        if self.scheduledleases.has_lease(lease_id):
            # The lease is either running, or scheduled to run
            lease = self.scheduledleases.get_lease(lease_id)
            
            if lease.state == constants.LEASE_STATE_ACTIVE:
                self.logger.info("Lease %i is active. Stopping active reservation..." % lease_id)
                rr = lease.get_active_reservations(time)[0]
                if isinstance(rr, VMResourceReservation):
                    self._handle_unscheduled_end_vm(lease, rr, enact=True)
                # TODO: Handle cancelations in middle of suspensions and
                # resumptions                
            elif lease.state in [constants.LEASE_STATE_SCHEDULED, constants.LEASE_STATE_DEPLOYED]:
                self.logger.info("Lease %i is scheduled. Cancelling reservations." % lease_id)
                rrs = lease.get_scheduled_reservations()
                for r in rrs:
                    lease.remove_rr(r)
                    self.slottable.removeReservation(r)
                lease.state = constants.LEASE_STATE_DONE
                self.completedleases.add(lease)
                self.scheduledleases.remove(lease)
        elif self.queue.has_lease(lease_id):
            # The lease is in the queue, waiting to be scheduled.
            # Cancelling is as simple as removing it from the queue
            self.logger.info("Lease %i is in the queue. Removing..." % lease_id)
            l = self.queue.get_lease(lease_id)
            self.queue.remove_lease(lease)
    
    def fail_lease(self, lease_id):
        """Transitions a lease to a failed state, and does any necessary cleaning up
        
        TODO: For now, just use the cancelling algorithm
        
        Arguments:
        lease -- Lease to fail
        """    
        try:
            raise
            self.cancel_lease(lease_id)
        except Exception, msg:
            # Exit if something goes horribly wrong
            self.logger.error("Exception when failing lease %i. Dumping state..." % lease_id)
            self.rm.print_stats(logging.getLevelName("ERROR"), verbose=True)
            raise          
    
    def notify_event(self, lease_id, event):
        time = self.rm.clock.get_time()
        if event == constants.EVENT_END_VM:
            lease = self.scheduledleases.get_lease(lease_id)
            rr = lease.get_active_reservations(time)[0]
            self._handle_unscheduled_end_vm(lease, rr, enact=False)

    
    def __process_ar_request(self, lease_req, nexttime):
        self.logger.info("Received AR lease request #%i, %i nodes from %s to %s." % (lease_req.id, lease_req.numnodes, lease_req.start.requested, lease_req.start.requested + lease_req.duration.requested))
        self.logger.debug("  Start   : %s" % lease_req.start)
        self.logger.debug("  Duration: %s" % lease_req.duration)
        self.logger.debug("  ResReq  : %s" % lease_req.requested_resources)
        
        accepted = False
        try:
            self.__schedule_ar_lease(lease_req, avoidpreempt=True, nexttime=nexttime)
            self.scheduledleases.add(lease_req)
            self.rm.accounting.incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
            accepted = True
        except SchedException, msg:
            # Our first try avoided preemption, try again
            # without avoiding preemption.
            # TODO: Roll this into the exact slot fitting algorithm
            try:
                self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))
                self.logger.debug("LEASE-%i Trying again without avoiding preemption" % lease_req.id)
                self.__schedule_ar_lease(lease_req, nexttime, avoidpreempt=False)
                self.scheduledleases.add(lease_req)
                self.rm.accounting.incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
                accepted = True
            except SchedException, msg:
                self.rm.accounting.incr_counter(constants.COUNTER_ARREJECTED, lease_req.id)
                self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))

        if accepted:
            self.logger.info("AR lease request #%i has been accepted." % lease_req.id)
        else:
            self.logger.info("AR lease request #%i has been rejected." % lease_req.id)
        
    
    def __schedule_ar_lease(self, lease_req, nexttime, avoidpreempt=True):
        start = lease_req.start.requested
        end = lease_req.start.requested + lease_req.duration.requested
        try:
            (nodeassignment, res, preemptions) = self.__fit_exact(lease_req, preemptible=False, canpreempt=True, avoidpreempt=avoidpreempt)
            
            if len(preemptions) > 0:
                leases = self.__find_preemptable_leases(preemptions, start, end)
                self.logger.info("Must preempt leases %s to make room for AR lease #%i" % ([l.id for l in leases], lease_req.id))
                for lease in leases:
                    self.preempt(lease, time=start)

            # Create VM resource reservations
            vmrr = ds.VMResourceReservation(lease_req, start, end, nodeassignment, res, constants.ONCOMPLETE_ENDLEASE, False)
            vmrr.state = constants.RES_STATE_SCHEDULED

            # Schedule deployment overhead
            self.deployment.schedule(lease_req, vmrr, nexttime)
            
            # Commit reservation to slot table
            # (we don't do this until the very end because the deployment overhead
            # scheduling could still throw an exception)
            lease_req.append_rr(vmrr)
            self.slottable.addReservation(vmrr)
        except SlotFittingException, msg:
            raise SchedException, "The requested AR lease is infeasible. Reason: %s" % msg

    def __process_queue(self, nexttime):
        done = False
        newqueue = ds.Queue(self)
        while not done and not self.is_queue_empty():
            if self.numbesteffortres == self.maxres and self.slottable.isFull(nexttime):
                self.logger.debug("Used up all reservations and slot table is full. Skipping rest of queue.")
                done = True
            else:
                lease_req = self.queue.dequeue()
                try:
                    self.logger.info("Next request in the queue is lease %i. Attempting to schedule..." % lease_req.id)
                    self.logger.debug("  Duration: %s" % lease_req.duration)
                    self.logger.debug("  ResReq  : %s" % lease_req.requested_resources)
                    self.__schedule_besteffort_lease(lease_req, nexttime)
                    self.scheduledleases.add(lease_req)
                    self.rm.accounting.decr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
                except SchedException, msg:
                    # Put back on queue
                    newqueue.enqueue(lease_req)
                    self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))
                    self.logger.info("Lease %i could not be scheduled at this time." % lease_req.id)
                    if not self.is_backfilling():
                        done = True
        
        for lease in self.queue:
            newqueue.enqueue(lease)
        
        self.queue = newqueue 
        

    def __schedule_besteffort_lease(self, req, nexttime):
        # Determine earliest start time in each node
        if req.state == constants.LEASE_STATE_PENDING:
            # Figure out earliest start times based on
            # image schedule and reusable images
            earliest = self.deployment.find_earliest_starting_times(req, nexttime)
        elif req.state == constants.LEASE_STATE_SUSPENDED:
            # No need to transfer images from repository
            # (only intra-node transfer)
            earliest = dict([(node+1, [nexttime, constants.REQTRANSFER_NO, None]) for node in range(req.numnodes)])
            
        susptype = self.rm.config.get("suspension")
        if susptype == constants.SUSPENSION_NONE or (susptype == constants.SUSPENSION_SERIAL and req.numnodes == 1):
            cansuspend = False
        else:
            cansuspend = True

        canmigrate = self.rm.config.get("migration")
        try:
            mustresume = (req.state == constants.LEASE_STATE_SUSPENDED)
            canreserve = self.canReserveBestEffort()
            (resmrr, vmrr, susprr, reservation) = self.__fit_besteffort(req, earliest, canreserve, suspendable=cansuspend, canmigrate=canmigrate, mustresume=mustresume)
            
            # Schedule deployment
            if req.state != constants.LEASE_STATE_SUSPENDED:
                self.deployment.schedule(req, vmrr, nexttime)
            
            # TODO: The following would be more correctly handled in the RR handle functions.
            # We need to have an explicit MigrationResourceReservation before doing that.
            if req.state == constants.LEASE_STATE_SUSPENDED:
                # Update VM image mappings, since we might be resuming
                # in different nodes.
                for vnode, pnode in req.vmimagemap.items():
                    self.resourcepool.remove_diskimage(pnode, req.id, vnode)
                req.vmimagemap = vmrr.nodes
                for vnode, pnode in req.vmimagemap.items():
                    self.resourcepool.add_diskimage(pnode, req.diskimage_id, req.diskimage_size, req.id, vnode)
                
                # Update RAM file mappings
                for vnode, pnode in req.memimagemap.items():
                    self.resourcepool.remove_ramfile(pnode, req.id, vnode)
                for vnode, pnode in vmrr.nodes.items():
                    self.resourcepool.add_ramfile(pnode, req.id, vnode, req.requested_resources.get_by_type(constants.RES_MEM))
                    req.memimagemap[vnode] = pnode
                    
            # Add resource reservations
            if resmrr != None:
                req.append_rr(resmrr)
                self.slottable.addReservation(resmrr)
            req.append_rr(vmrr)
            self.slottable.addReservation(vmrr)
            if susprr != None:
                req.append_rr(susprr)
                self.slottable.addReservation(susprr)
           
            if reservation:
                self.numbesteffortres += 1
                
            req.print_contents()
            
        except SlotFittingException, msg:
            raise SchedException, "The requested best-effort lease is infeasible. Reason: %s" % msg

        
    def __process_im_request(self, lease_req, nexttime):
        self.logger.info("Received immediate lease request #%i (%i nodes)" % (lease_req.id, lease_req.numnodes))
        self.logger.debug("  Duration: %s" % lease_req.duration)
        self.logger.debug("  ResReq  : %s" % lease_req.requested_resources)
        
        try:
            self.__schedule_immediate_lease(lease_req, nexttime=nexttime)
            self.scheduledleases.add(lease_req)
            self.rm.accounting.incr_counter(constants.COUNTER_IMACCEPTED, lease_req.id)
            self.logger.info("Immediate lease request #%i has been accepted." % lease_req.id)
        except SchedException, msg:
            self.rm.accounting.incr_counter(constants.COUNTER_IMREJECTED, lease_req.id)
            self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))
        
        
    def __schedule_immediate_lease(self, req, nexttime):
        # Determine earliest start time in each node
        earliest = self.deployment.find_earliest_starting_times(req, nexttime)
        try:
            (resmrr, vmrr, susprr, reservation) = self.__fit_besteffort(req, earliest, canreserve=False, suspendable=False, canmigrate=False, mustresume=False)
            # Schedule deployment
            self.deployment.schedule(req, vmrr, nexttime)
                        
            req.append_rr(vmrr)
            self.slottable.addReservation(vmrr)
                    
            req.print_contents()
        except SlotFittingException, msg:
            raise SchedException, "The requested immediate lease is infeasible. Reason: %s" % msg
        
    def __fit_exact(self, leasereq, preemptible=False, canpreempt=True, avoidpreempt=True):
        lease_id = leasereq.id
        start = leasereq.start.requested
        end = leasereq.start.requested + leasereq.duration.requested
        diskImageID = leasereq.diskimage_id
        numnodes = leasereq.numnodes
        resreq = leasereq.requested_resources

        availabilitywindow = self.slottable.availabilitywindow

        availabilitywindow.initWindow(start, resreq, canpreempt=canpreempt)
        availabilitywindow.printContents(withpreemption = False)
        availabilitywindow.printContents(withpreemption = True)

        mustpreempt = False
        unfeasiblewithoutpreemption = False
        
        fitatstart = availabilitywindow.fitAtStart(canpreempt = False)
        if fitatstart < numnodes:
            if not canpreempt:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                unfeasiblewithoutpreemption = True
        feasibleend, canfitnopreempt = availabilitywindow.findPhysNodesForVMs(numnodes, end, strictend=True, canpreempt = False)
        fitatend = sum([n for n in canfitnopreempt.values()])
        if fitatend < numnodes:
            if not canpreempt:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                unfeasiblewithoutpreemption = True

        canfitpreempt = None
        if canpreempt:
            fitatstart = availabilitywindow.fitAtStart(canpreempt = True)
            if fitatstart < numnodes:
                raise SlotFittingException, "Not enough resources in specified interval"
            feasibleendpreempt, canfitpreempt = availabilitywindow.findPhysNodesForVMs(numnodes, end, strictend=True, canpreempt = True)
            fitatend = sum([n for n in canfitpreempt.values()])
            if fitatend < numnodes:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                if unfeasiblewithoutpreemption:
                    mustpreempt = True
                else:
                    mustpreempt = False

        # At this point we know if the lease is feasible, and if
        # will require preemption.
        if not mustpreempt:
           self.logger.debug("The VM reservations for this lease are feasible without preemption.")
        else:
           self.logger.debug("The VM reservations for this lease are feasible but will require preemption.")

        # merge canfitnopreempt and canfitpreempt
        canfit = {}
        for node in canfitnopreempt:
            vnodes = canfitnopreempt[node]
            canfit[node] = [vnodes, vnodes]
        for node in canfitpreempt:
            vnodes = canfitpreempt[node]
            if canfit.has_key(node):
                canfit[node][1] = vnodes
            else:
                canfit[node] = [0, vnodes]

        orderednodes = self.__choose_nodes(canfit, start, canpreempt, avoidpreempt)
            
        self.logger.debug("Node ordering: %s" % orderednodes)
        
        # vnode -> pnode
        nodeassignment = {}
        
        # pnode -> resourcetuple
        res = {}
        
        # physnode -> how many vnodes
        preemptions = {}
        
        vnode = 1
        if avoidpreempt:
            # First pass, without preemption
            for physnode in orderednodes:
                canfitinnode = canfit[physnode][0]
                for i in range(1, canfitinnode+1):
                    nodeassignment[vnode] = physnode
                    if res.has_key(physnode):
                        res[physnode].incr(resreq)
                    else:
                        res[physnode] = ds.ResourceTuple.copy(resreq)
                    canfit[physnode][0] -= 1
                    canfit[physnode][1] -= 1
                    vnode += 1
                    if vnode > numnodes:
                        break
                if vnode > numnodes:
                    break
            
        # Second pass, with preemption
        if mustpreempt or not avoidpreempt:
            for physnode in orderednodes:
                canfitinnode = canfit[physnode][1]
                for i in range(1, canfitinnode+1):
                    nodeassignment[vnode] = physnode
                    if res.has_key(physnode):
                        res[physnode].incr(resreq)
                    else:
                        res[physnode] = ds.ResourceTuple.copy(resreq)
                    canfit[physnode][1] -= 1
                    vnode += 1
                    # Check if this will actually result in a preemption
                    if canfit[physnode][0] == 0:
                        if preemptions.has_key(physnode):
                            preemptions[physnode].incr(resreq)
                        else:
                            preemptions[physnode] = ds.ResourceTuple.copy(resreq)
                    else:
                        canfit[physnode][0] -= 1
                    if vnode > numnodes:
                        break
                if vnode > numnodes:
                    break

        if vnode <= numnodes:
            raise SchedException, "Availability window indicated that request but feasible, but could not fit it"

        return nodeassignment, res, preemptions


    def __find_preemptable_leases(self, mustpreempt, startTime, endTime):
        def comparepreemptability(rrX, rrY):
            if rrX.lease.submit_time > rrY.lease.submit_time:
                return constants.BETTER
            elif rrX.lease.submit_time < rrY.lease.submit_time:
                return constants.WORSE
            else:
                return constants.EQUAL        
            
        def preemptedEnough(amountToPreempt):
            for node in amountToPreempt:
                if not amountToPreempt[node].is_zero_or_less():
                    return False
            return True
        
        # Get allocations at the specified time
        atstart = set()
        atmiddle = set()
        nodes = set(mustpreempt.keys())
        
        reservationsAtStart = self.slottable.getReservationsAt(startTime)
        reservationsAtStart = [r for r in reservationsAtStart if r.is_preemptible()
                        and len(set(r.resources_in_pnode.keys()) & nodes)>0]
        
        reservationsAtMiddle = self.slottable.getReservationsStartingBetween(startTime, endTime)
        reservationsAtMiddle = [r for r in reservationsAtMiddle if r.is_preemptible()
                        and len(set(r.resources_in_pnode.keys()) & nodes)>0]
        
        reservationsAtStart.sort(comparepreemptability)
        reservationsAtMiddle.sort(comparepreemptability)
        
        amountToPreempt = {}
        for n in mustpreempt:
            amountToPreempt[n] = ds.ResourceTuple.copy(mustpreempt[n])

        # First step: CHOOSE RESOURCES TO PREEMPT AT START OF RESERVATION
        for r in reservationsAtStart:
            # The following will really only come into play when we have
            # multiple VMs per node
            mustpreemptres = False
            for n in r.resources_in_pnode.keys():
                # Don't need to preempt if we've already preempted all
                # the needed resources in node n
                if amountToPreempt.has_key(n) and not amountToPreempt[n].is_zero_or_less():
                    amountToPreempt[n].decr(r.resources_in_pnode[n])
                    mustpreemptres = True
            if mustpreemptres:
                atstart.add(r)
            if preemptedEnough(amountToPreempt):
                break
        
        # Second step: CHOOSE RESOURCES TO PREEMPT DURING RESERVATION
        if len(reservationsAtMiddle)>0:
            changepoints = set()
            for r in reservationsAtMiddle:
                changepoints.add(r.start)
            changepoints = list(changepoints)
            changepoints.sort()        
            
            for cp in changepoints:
                amountToPreempt = {}
                for n in mustpreempt:
                    amountToPreempt[n] = ds.ResourceTuple.copy(mustpreempt[n])
                reservations = [r for r in reservationsAtMiddle 
                                if r.start <= cp and cp < r.end]
                for r in reservations:
                    mustpreemptres = False
                    for n in r.resources_in_pnode.keys():
                        if amountToPreempt.has_key(n) and not amountToPreempt[n].is_zero_or_less():
                            amountToPreempt[n].decr(r.resources_in_pnode[n])
                            mustpreemptres = True
                    if mustpreemptres:
                        atmiddle.add(r)
                    if preemptedEnough(amountToPreempt):
                        break
            
        self.logger.debug("Preempting leases (at start of reservation): %s" % [r.lease.id for r in atstart])
        self.logger.debug("Preempting leases (in middle of reservation): %s" % [r.lease.id for r in atmiddle])
        
        leases = [r.lease for r in atstart|atmiddle]
        
        return leases


    def __fit_besteffort(self, lease, earliest, canreserve, suspendable, canmigrate, mustresume):
        lease_id = lease.id
        remdur = lease.duration.get_remaining_duration()
        numnodes = lease.numnodes
        resreq = lease.requested_resources
        preemptible = lease.preemptible
        suspendresumerate = self.resourcepool.info.get_suspendresume_rate()
        migration_bandwidth = self.resourcepool.info.get_migration_bandwidth()

        #
        # STEP 1: TAKE INTO ACCOUNT VM RESUMPTION (IF ANY)
        #
        
        curnodes=None
        # If we can't migrate, we have to stay in the
        # nodes where the lease is currently deployed
        if mustresume and not canmigrate:
            vmrr, susprr = lease.get_last_vmrr()
            curnodes = set(vmrr.nodes.values())
            suspendthreshold = lease.get_suspend_threshold(initial=False, suspendrate=suspendresumerate, migrating=False)
        
        if mustresume and canmigrate:
            # If we have to resume this lease, make sure that
            # we have enough time to transfer the images.
            migratetime = lease.estimate_migration_time(migration_bandwidth)
            earliesttransfer = self.rm.clock.get_time() + migratetime

            for n in earliest:
                earliest[n][0] = max(earliest[n][0], earliesttransfer)
            suspendthreshold = lease.get_suspend_threshold(initial=False, suspendrate=suspendresumerate, migrating=True)
                    
        if mustresume:
            resumetime = lease.estimate_suspend_resume_time(suspendresumerate)
            # Must allocate time for resumption too
            remdur += resumetime
        else:
            suspendthreshold = lease.get_suspend_threshold(initial=True, suspendrate=suspendresumerate)


        #
        # STEP 2: FIND THE CHANGEPOINTS
        #

        # Find the changepoints, and the nodes we can use at each changepoint
        # Nodes may not be available at a changepoint because images
        # cannot be transferred at that time.
        if not mustresume:
            cps = [(node, e[0]) for node, e in earliest.items()]
            cps.sort(key=itemgetter(1))
            curcp = None
            changepoints = []
            nodes = []
            for node, time in cps:
                nodes.append(node)
                if time != curcp:
                    changepoints.append([time, nodes[:]])
                    curcp = time
                else:
                    changepoints[-1][1] = nodes[:]
        else:
            changepoints = list(set([x[0] for x in earliest.values()]))
            changepoints.sort()
            changepoints = [(x, curnodes) for x in changepoints]

        # If we can make reservations for best-effort leases,
        # we also consider future changepoints
        # (otherwise, we only allow the VMs to start "now", accounting
        #  for the fact that vm images will have to be deployed)
        if canreserve:
            futurecp = self.slottable.findChangePointsAfter(changepoints[-1][0])
            futurecp = [(p,None) for p in futurecp]
        else:
            futurecp = []



        #
        # STEP 3: SLOT FITTING
        #

        # First, assuming we can't make reservations in the future
        start, end, canfit, mustsuspend = self.__find_fit_at_points(changepoints, numnodes, resreq, remdur, suspendable, suspendthreshold)

        if not canreserve:
            if start == None:
                # We did not find a suitable starting time. This can happen
                # if we're unable to make future reservations
                raise SlotFittingException, "Could not find enough resources for this request"
            elif mustsuspend and not suspendable:
                raise SlotFittingException, "Scheduling this lease would require preempting it, which is not allowed"

        if start != None and mustsuspend and not suspendable:
            start = None # No satisfactory start time
            
        # If we haven't been able to fit the lease, check if we can
        # reserve it in the future
        if start == None and canreserve:
            start, end, canfit, mustsuspend = self.__find_fit_at_points(futurecp, numnodes, resreq, remdur, suspendable, suspendthreshold)

        if mustsuspend and not suspendable:
            raise SlotFittingException, "Scheduling this lease would require preempting it, which is not allowed"

        if start in [p[0] for p in futurecp]:
            reservation = True
        else:
            reservation = False


        #
        # STEP 4: FINAL SLOT FITTING
        #
        # At this point, we know the lease fits, but we have to map it to
        # specific physical nodes.
        
        # Sort physical nodes
        physnodes = canfit.keys()
        if mustresume:
            # If we're resuming, we prefer resuming in the nodes we're already
            # deployed in, to minimize the number of transfers.
            vmrr, susprr = lease.get_last_vmrr()
            nodes = set(vmrr.nodes.values())
            availnodes = set(physnodes)
            deplnodes = availnodes.intersection(nodes)
            notdeplnodes = availnodes.difference(nodes)
            physnodes = list(deplnodes) + list(notdeplnodes)
        else:
            physnodes.sort() # Arbitrary, prioritize nodes, as in exact

        # Adjust times in case the lease has to be suspended/resumed
        if mustsuspend:
            suspendtime = lease.estimate_suspend_resume_time(suspendresumerate)
            end -= suspendtime
                
        if mustresume:
            start += resumetime
        
        # Map to physical nodes
        mappings = {}
        res = {}
        vmnode = 1
        while vmnode <= numnodes:
            for n in physnodes:
                if canfit[n]>0:
                    canfit[n] -= 1
                    mappings[vmnode] = n
                    if res.has_key(n):
                        res[n].incr(resreq)
                    else:
                        res[n] = ds.ResourceTuple.copy(resreq)
                    vmnode += 1
                    break



        #
        # STEP 5: CREATE RESOURCE RESERVATIONS
        #
        
        if mustresume:
            resmres = {}
            for n in mappings.values():
                r = ds.ResourceTuple.create_empty()
                r.set_by_type(constants.RES_MEM, resreq.get_by_type(constants.RES_MEM))
                r.set_by_type(constants.RES_DISK, resreq.get_by_type(constants.RES_DISK))
                resmres[n] = r
            resmrr = ds.ResumptionResourceReservation(lease, start-resumetime, start, resmres, mappings)
            resmrr.state = constants.RES_STATE_SCHEDULED
        else:
            resmrr = None
        if mustsuspend:
            suspres = {}
            for n in mappings.values():
                r = ds.ResourceTuple.create_empty()
                r.set_by_type(constants.RES_MEM, resreq.get_by_type(constants.RES_MEM))
                r.set_by_type(constants.RES_DISK, resreq.get_by_type(constants.RES_DISK))
                suspres[n] = r
            susprr = ds.SuspensionResourceReservation(lease, end, end + suspendtime, suspres, mappings)
            susprr.state = constants.RES_STATE_SCHEDULED
            oncomplete = constants.ONCOMPLETE_SUSPEND
        else:
            susprr = None
            oncomplete = constants.ONCOMPLETE_ENDLEASE

        vmrr = ds.VMResourceReservation(lease, start, end, mappings, res, oncomplete, reservation)
        vmrr.state = constants.RES_STATE_SCHEDULED
        
        susp_str = res_str = ""
        if mustresume:
            res_str = " (resuming)"
        if mustsuspend:
            susp_str = " (suspending)"
        self.logger.info("Lease #%i has been scheduled on nodes %s from %s%s to %s%s" % (lease.id, mappings.values(), start, res_str, end, susp_str))

        return resmrr, vmrr, susprr, reservation

    def __find_fit_at_points(self, changepoints, numnodes, resreq, remdur, suspendable, suspendthreshold):
        start = None
        end = None
        canfit = None
        mustsuspend = None
        availabilitywindow = self.slottable.availabilitywindow


        for p in changepoints:
            availabilitywindow.initWindow(p[0], resreq, p[1], canpreempt = False)
            availabilitywindow.printContents()
            
            if availabilitywindow.fitAtStart() >= numnodes:
                start=p[0]
                maxend = start + remdur
                end, canfit = availabilitywindow.findPhysNodesForVMs(numnodes, maxend)
        
                self.logger.debug("This lease can be scheduled from %s to %s" % (start, end))
                
                if end < maxend:
                    mustsuspend=True
                    self.logger.debug("This lease will require suspension (maxend = %s)" % (maxend))
                    
                    if suspendable:
                        # It the lease is suspendable...
                        if suspendthreshold != None:
                            if end-start > suspendthreshold:
                                break
                            else:
                                self.logger.debug("This starting time does not meet the suspend threshold (%s < %s)" % (end-start, suspendthreshold))
                                start = None
                        else:
                            pass
                    else:
                        # Keep looking
                        pass
                else:
                    mustsuspend=False
                    # We've found a satisfactory starting time
                    break        
                
        return start, end, canfit, mustsuspend

    def __choose_nodes(self, canfit, start, canpreempt, avoidpreempt):
        # TODO2: Choose appropriate prioritizing function based on a
        # config file, instead of hardcoding it)
        #
        # TODO3: Basing decisions only on CPU allocations. This is ok for now,
        # since the memory allocation is proportional to the CPU allocation.
        # Later on we need to come up with some sort of weighed average.
        
        nodes = canfit.keys()
        
        # TODO: The deployment module should just provide a list of nodes
        # it prefers
        nodeswithimg=[]
        #self.lease_deployment_type = self.rm.config.get("lease-preparation")
        #if self.lease_deployment_type == constants.DEPLOYMENT_TRANSFER:
        #    reusealg = self.rm.config.get("diskimage-reuse")
        #    if reusealg==constants.REUSE_IMAGECACHES:
        #        nodeswithimg = self.resourcepool.getNodesWithImgInPool(diskImageID, start)

        # Compares node x and node y. 
        # Returns "x is ??? than y" (???=BETTER/WORSE/EQUAL)
        def comparenodes(x, y):
            hasimgX = x in nodeswithimg
            hasimgY = y in nodeswithimg

            # First comparison: A node with no preemptible VMs is preferible
            # to one with preemptible VMs (i.e. we want to avoid preempting)
            canfitnopreemptionX = canfit[x][0]
            canfitpreemptionX = canfit[x][1]
            hasPreemptibleX = canfitpreemptionX > canfitnopreemptionX
            
            canfitnopreemptionY = canfit[y][0]
            canfitpreemptionY = canfit[y][1]
            hasPreemptibleY = canfitpreemptionY > canfitnopreemptionY

            # TODO: Factor out common code
            if avoidpreempt:
                if hasPreemptibleX and not hasPreemptibleY:
                    return constants.WORSE
                elif not hasPreemptibleX and hasPreemptibleY:
                    return constants.BETTER
                elif not hasPreemptibleX and not hasPreemptibleY:
                    if hasimgX and not hasimgY: 
                        return constants.BETTER
                    elif not hasimgX and hasimgY: 
                        return constants.WORSE
                    else:
                        if canfitnopreemptionX > canfitnopreemptionY: return constants.BETTER
                        elif canfitnopreemptionX < canfitnopreemptionY: return constants.WORSE
                        else: return constants.EQUAL
                elif hasPreemptibleX and hasPreemptibleY:
                    # If both have (some) preemptible resources, we prefer those
                    # that involve the less preemptions
                    preemptX = canfitpreemptionX - canfitnopreemptionX
                    preemptY = canfitpreemptionY - canfitnopreemptionY
                    if preemptX < preemptY:
                        return constants.BETTER
                    elif preemptX > preemptY:
                        return constants.WORSE
                    else:
                        if hasimgX and not hasimgY: return constants.BETTER
                        elif not hasimgX and hasimgY: return constants.WORSE
                        else: return constants.EQUAL
            elif not avoidpreempt:
                # First criteria: Can we reuse image?
                if hasimgX and not hasimgY: 
                    return constants.BETTER
                elif not hasimgX and hasimgY: 
                    return constants.WORSE
                else:
                    # Now we just want to avoid preemption
                    if hasPreemptibleX and not hasPreemptibleY:
                        return constants.WORSE
                    elif not hasPreemptibleX and hasPreemptibleY:
                        return constants.BETTER
                    elif hasPreemptibleX and hasPreemptibleY:
                        # If both have (some) preemptible resources, we prefer those
                        # that involve the less preemptions
                        preemptX = canfitpreemptionX - canfitnopreemptionX
                        preemptY = canfitpreemptionY - canfitnopreemptionY
                        if preemptX < preemptY:
                            return constants.BETTER
                        elif preemptX > preemptY:
                            return constants.WORSE
                        else:
                            if hasimgX and not hasimgY: return constants.BETTER
                            elif not hasimgX and hasimgY: return constants.WORSE
                            else: return constants.EQUAL
                    else:
                        return constants.EQUAL
        
        # Order nodes
        nodes.sort(comparenodes)
        return nodes        
        
    def preempt(self, req, time):
        self.logger.info("Preempting lease #%i..." % (req.id))
        self.logger.vdebug("Lease before preemption:")
        req.print_contents()
        vmrr, susprr  = req.get_last_vmrr()
        suspendresumerate = self.resourcepool.info.get_suspendresume_rate()
        
        if vmrr.state == constants.RES_STATE_SCHEDULED and vmrr.start >= time:
            self.logger.info("... lease #%i has been cancelled and requeued." % req.id)
            self.logger.debug("Lease was set to start in the middle of the preempting lease.")
            req.state = constants.LEASE_STATE_PENDING
            if vmrr.backfill_reservation == True:
                self.numbesteffortres -= 1
            req.remove_rr(vmrr)
            self.slottable.removeReservation(vmrr)
            if susprr != None:
                req.remove_rr(susprr)
                self.slottable.removeReservation(susprr)
            for vnode, pnode in req.vmimagemap.items():
                self.resourcepool.remove_diskimage(pnode, req.id, vnode)
            self.deployment.cancel_deployment(req)
            req.vmimagemap = {}
            self.scheduledleases.remove(req)
            self.queue.enqueue_in_order(req)
            self.rm.accounting.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
        else:
            susptype = self.rm.config.get("suspension")
            timebeforesuspend = time - vmrr.start
            # TODO: Determine if it is in fact the initial VMRR or not. Right now
            # we conservatively overestimate
            canmigrate = self.rm.config.get("migration")
            suspendthreshold = req.get_suspend_threshold(initial=False, suspendrate=suspendresumerate, migrating=canmigrate)
            # We can't suspend if we're under the suspend threshold
            suspendable = timebeforesuspend >= suspendthreshold
            if suspendable and (susptype == constants.SUSPENSION_ALL or (req.numnodes == 1 and susptype == constants.SUSPENSION_SERIAL)):
                self.logger.info("... lease #%i will be suspended at %s." % (req.id, time))
                self.slottable.suspend(req, time)
            else:
                self.logger.info("... lease #%i has been cancelled and requeued (cannot be suspended)" % req.id)
                req.state = constants.LEASE_STATE_PENDING
                if vmrr.backfill_reservation == True:
                    self.numbesteffortres -= 1
                req.remove_rr(vmrr)
                self.slottable.removeReservation(vmrr)
                if susprr != None:
                    req.remove_rr(susprr)
                    self.slottable.removeReservation(susprr)
                if req.state == constants.LEASE_STATE_SUSPENDED:
                    resmrr = req.prev_rr(vmrr)
                    req.remove_rr(resmrr)
                    self.slottable.removeReservation(resmrr)
                for vnode, pnode in req.vmimagemap.items():
                    self.resourcepool.remove_diskimage(pnode, req.id, vnode)
                self.deployment.cancel_deployment(req)
                req.vmimagemap = {}
                self.scheduledleases.remove(req)
                self.queue.enqueue_in_order(req)
                self.rm.accounting.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
        self.logger.vdebug("Lease after preemption:")
        req.print_contents()
        
    def reevaluate_schedule(self, endinglease, nodes, nexttime, checkedleases):
        self.logger.debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes, nexttime)) 
        leases = self.scheduledleases.getNextLeasesScheduledInNodes(nexttime, nodes)
        leases = [l for l in leases if isinstance(l, ds.BestEffortLease) and not l in checkedleases]
        for l in leases:
            self.logger.debug("Found lease %i" % l.id)
            l.print_contents()
            # Earliest time can't be earlier than time when images will be
            # available in node
            earliest = max(nexttime, l.imagesavail)
            self.slottable.slideback(l, earliest)
            checkedleases.append(l)
        #for l in leases:
        #    vmrr, susprr = l.getLastVMRR()
        #    self.reevaluateSchedule(l, vmrr.nodes.values(), vmrr.end, checkedleases)
          
            

    #-------------------------------------------------------------------#
    #                                                                   #
    #                  SLOT TABLE EVENT HANDLERS                        #
    #                                                                   #
    #-------------------------------------------------------------------#

    def _handle_start_vm(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartVM" % l.id)
        l.print_contents()
        if l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            now_time = self.rm.clock.get_time()
            l.start.actual = now_time
            
            try:
                self.deployment.check(l, rr)
                self.resourcepool.start_vms(l, rr)
                # The next two lines have to be moved somewhere more
                # appropriate inside the resourcepool module
                for (vnode, pnode) in rr.nodes.items():
                    l.vmimagemap[vnode] = pnode
            except Exception, e:
                self.logger.error("ERROR when starting VMs.")
                raise
        elif l.state == constants.LEASE_STATE_SUSPENDED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            # No enactment to do here, since all the suspend/resume actions are
            # handled during the suspend/resume RRs
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RUN, l.id)
        self.logger.debug("LEASE-%i End of handleStartVM" % l.id)
        self.logger.info("Started VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))

    # TODO: Replace enact with a saner way of handling leases that have failed or
    #       ended prematurely.
    #       Possibly factor out the "clean up" code to a separate function
    def _handle_end_vm(self, l, rr, enact=True):
        self.logger.debug("LEASE-%i Start of handleEndVM" % l.id)
        self.logger.vdebug("LEASE-%i Before:" % l.id)
        l.print_contents()
        now_time = self.rm.clock.get_time()
        diff = now_time - rr.start
        l.duration.accumulate_duration(diff)
        rr.state = constants.RES_STATE_DONE
        if rr.oncomplete == constants.ONCOMPLETE_ENDLEASE:
            self.resourcepool.stop_vms(l, rr)
            l.state = constants.LEASE_STATE_DONE
            l.duration.actual = l.duration.accumulated
            l.end = now_time
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            self.deployment.cleanup(l, rr)
            if isinstance(l, ds.BestEffortLease):
                self.rm.accounting.incr_counter(constants.COUNTER_BESTEFFORTCOMPLETED, l.id)
       
        if isinstance(l, ds.BestEffortLease):
            if rr.backfill_reservation == True:
                self.numbesteffortres -= 1
        self.logger.vdebug("LEASE-%i After:" % l.id)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.logger.debug("LEASE-%i End of handleEndVM" % l.id)
        self.logger.info("Stopped VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))

    def _handle_unscheduled_end_vm(self, l, rr, enact=False):
        self.logger.info("LEASE-%i The VM has ended prematurely." % l.id)
        self._handle_end_rr(l, rr)
        if rr.oncomplete == constants.ONCOMPLETE_SUSPEND:
            rrs = l.next_rrs(rr)
            for r in rrs:
                l.remove_rr(r)
                self.slottable.removeReservation(r)
        rr.oncomplete = constants.ONCOMPLETE_ENDLEASE
        rr.end = self.rm.clock.get_time()
        self._handle_end_vm(l, rr, enact=enact)
        nexttime = self.rm.clock.get_next_schedulable_time()
        if self.is_backfilling():
            # We need to reevaluate the schedule to see if there are any future
            # reservations that we can slide back.
            self.reevaluate_schedule(l, rr.nodes.values(), nexttime, [])

    def _handle_start_suspend(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartSuspend" % l.id)
        l.print_contents()
        rr.state = constants.RES_STATE_ACTIVE
        self.resourcepool.suspend_vms(l, rr)
        for vnode, pnode in rr.nodes.items():
            self.resourcepool.add_ramfile(pnode, l.id, vnode, l.requested_resources.get_by_type(constants.RES_MEM))
            l.memimagemap[vnode] = pnode
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_SUSPEND, l.id)
        self.logger.debug("LEASE-%i End of handleStartSuspend" % l.id)
        self.logger.info("Suspending lease %i..." % (l.id))

    def _handle_end_suspend(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndSuspend" % l.id)
        l.print_contents()
        # TODO: React to incomplete suspend
        self.resourcepool.verify_suspend(l, rr)
        rr.state = constants.RES_STATE_DONE
        l.state = constants.LEASE_STATE_SUSPENDED
        self.scheduledleases.remove(l)
        self.queue.enqueue_in_order(l)
        self.rm.accounting.incr_counter(constants.COUNTER_QUEUESIZE, l.id)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.logger.debug("LEASE-%i End of handleEndSuspend" % l.id)
        self.logger.info("Lease %i suspended." % (l.id))

    def _handle_start_resume(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartResume" % l.id)
        l.print_contents()
        self.resourcepool.resume_vms(l, rr)
        rr.state = constants.RES_STATE_ACTIVE
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RESUME, l.id)
        self.logger.debug("LEASE-%i End of handleStartResume" % l.id)
        self.logger.info("Resuming lease %i..." % (l.id))

    def _handle_end_resume(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndResume" % l.id)
        l.print_contents()
        # TODO: React to incomplete resume
        self.resourcepool.verify_resume(l, rr)
        rr.state = constants.RES_STATE_DONE
        for vnode, pnode in rr.nodes.items():
            self.resourcepool.remove_ramfile(pnode, l.id, vnode)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.logger.debug("LEASE-%i End of handleEndResume" % l.id)
        self.logger.info("Resumed lease %i" % (l.id))

    def _handle_end_rr(self, l, rr):
        self.slottable.removeReservation(rr)



    def __enqueue_in_order(self, req):
        self.rm.accounting.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
        self.queue.enqueue_in_order(req)

    
    
        
    def canReserveBestEffort(self):
        return self.numbesteffortres < self.maxres
    
                        
    def updateNodeVMState(self, nodes, state, lease_id):
        for n in nodes:
            self.resourcepool.get_node(n).vm_doing = state

    def updateNodeTransferState(self, nodes, state, lease_id):
        for n in nodes:
            self.resourcepool.get_node(n).transfer_doing = state
            
    def is_backfilling(self):
        return self.maxres > 0
