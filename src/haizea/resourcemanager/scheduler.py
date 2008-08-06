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
        self.logger = self.rm.logger
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        self.pending_leases = []
            
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
        self.rm.stats.append_stat(constants.COUNTER_CPUUTILIZATION, util)
        # TODO: Should be moved to rm.py
        self.rm.stats.tick()
        
        
    def register_handler(self, type, on_start, on_end):
        handler = ReservationEventHandler(on_start=on_start, on_end=on_end)
        self.handlers[type] = handler        
    
    def enqueue(self, lease_req):
        """Queues a best-effort lease request"""
        self.rm.stats.incr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
        self.queue.enqueue(lease_req)
        self.rm.logger.info("Received (and queueing) best-effort lease request #%i, %i nodes for %s." % (lease_req.id, lease_req.numnodes, lease_req.duration.requested), constants.SCHED)

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
        
        self.rm.logger.info("Cancelling lease %i..." % lease_id, constants.SCHED)
        if self.scheduledleases.has_lease(lease_id):
            # The lease is either running, or scheduled to run
            lease = self.scheduledleases.get_lease(lease_id)
            
            if lease.state == constants.LEASE_STATE_ACTIVE:
                self.rm.logger.info("Lease %i is active. Stopping active reservation..." % lease_id, constants.SCHED)
                rr = lease.get_active_reservations(time)[0]
                if isinstance(rr, VMResourceReservation):
                    self._handle_unscheduled_end_vm(lease, rr, enact=True)
                # TODO: Handle cancelations in middle of suspensions and
                # resumptions                
            elif lease.state in [constants.LEASE_STATE_SCHEDULED, constants.LEASE_STATE_DEPLOYED]:
                self.rm.logger.info("Lease %i is scheduled. Cancelling reservations." % lease_id, constants.SCHED)
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
            self.rm.logger.info("Lease %i is in the queue. Removing..." % lease_id, constants.SCHED)
            l = self.queue.get_lease(lease_id)
            self.queue.remove_lease(lease)
    
    
    def notify_event(self, lease_id, event):
        time = self.rm.clock.get_time()
        if event == constants.EVENT_END_VM:
            lease = self.scheduledleases.get_lease(lease_id)
            rr = lease.get_active_reservations(time)[0]
            self._handle_unscheduled_end_vm(lease, rr, enact=False)

    
    def __process_ar_request(self, lease_req, nexttime):
        self.rm.logger.info("Received AR lease request #%i, %i nodes from %s to %s." % (lease_req.id, lease_req.numnodes, lease_req.start.requested, lease_req.start.requested + lease_req.duration.requested), constants.SCHED)
        self.rm.logger.debug("  Start   : %s" % lease_req.start, constants.SCHED)
        self.rm.logger.debug("  Duration: %s" % lease_req.duration, constants.SCHED)
        self.rm.logger.debug("  ResReq  : %s" % lease_req.requested_resources, constants.SCHED)
        
        accepted = False
        try:
            self.__schedule_ar_lease(lease_req, avoidpreempt=True, nexttime=nexttime)
            self.scheduledleases.add(lease_req)
            self.rm.stats.incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
            accepted = True
        except SchedException, msg:
            # Our first try avoided preemption, try again
            # without avoiding preemption.
            # TODO: Roll this into the exact slot fitting algorithm
            try:
                self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg), constants.SCHED)
                self.rm.logger.debug("LEASE-%i Trying again without avoiding preemption" % lease_req.id, constants.SCHED)
                self.__schedule_ar_lease(lease_req, nexttime, avoidpreempt=False)
                self.scheduledleases.add(lease_req)
                self.rm.stats.incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
                accepted = True
            except SchedException, msg:
                self.rm.stats.incr_counter(constants.COUNTER_ARREJECTED, lease_req.id)
                self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg), constants.SCHED)

        if accepted:
            self.rm.logger.info("AR lease request #%i has been accepted." % lease_req.id, constants.SCHED)
        else:
            self.rm.logger.info("AR lease request #%i has been rejected." % lease_req.id, constants.SCHED)
        
    
    def __schedule_ar_lease(self, lease_req, nexttime, avoidpreempt=True):
        start = lease_req.start.requested
        end = lease_req.start.requested + lease_req.duration.requested
        try:
            (nodeassignment, res, preemptions) = self.slottable.fitExact(lease_req, preemptible=False, canpreempt=True, avoidpreempt=avoidpreempt)
            
            if len(preemptions) > 0:
                leases = self.slottable.findLeasesToPreempt(preemptions, start, end)
                self.rm.logger.info("Must preempt leases %s to make room for AR lease #%i" % ([l.id for l in leases], lease_req.id), constants.SCHED)
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
                self.rm.logger.debug("Used up all reservations and slot table is full. Skipping rest of queue.", constants.SCHED)
                done = True
            else:
                lease_req = self.queue.dequeue()
                try:
                    self.rm.logger.info("Next request in the queue is lease %i. Attempting to schedule..." % lease_req.id, constants.SCHED)
                    self.rm.logger.debug("  Duration: %s" % lease_req.duration, constants.SCHED)
                    self.rm.logger.debug("  ResReq  : %s" % lease_req.requested_resources, constants.SCHED)
                    self.__schedule_besteffort_lease(lease_req, nexttime)
                    self.scheduledleases.add(lease_req)
                    self.rm.stats.decr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
                except SchedException, msg:
                    # Put back on queue
                    newqueue.enqueue(lease_req)
                    self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg), constants.SCHED)
                    self.rm.logger.info("Lease %i could not be scheduled at this time." % lease_req.id, constants.SCHED)
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
            (resmrr, vmrr, susprr, reservation) = self.slottable.fitBestEffort(req, earliest, canreserve, suspendable=cansuspend, canmigrate=canmigrate, mustresume=mustresume)
            
            # Schedule deployment
            if req.state != constants.LEASE_STATE_SUSPENDED:
                self.deployment.schedule(req, vmrr, nexttime)
            
            # TODO: The following would be more correctly handled in the RR handle functions.
            # We need to have an explicit MigrationResourceReservation before doing that.
            if req.state == constants.LEASE_STATE_SUSPENDED:
                # Update VM image mappings, since we might be resuming
                # in different nodes.
                for vnode, pnode in req.vmimagemap.items():
                    self.rm.resourcepool.removeImage(pnode, req.id, vnode)
                req.vmimagemap = vmrr.nodes
                for vnode, pnode in req.vmimagemap.items():
                    self.rm.resourcepool.addTaintedImageToNode(pnode, req.diskimage_id, req.diskimage_size, req.id, vnode)
                
                # Update RAM file mappings
                for vnode, pnode in req.memimagemap.items():
                    self.rm.resourcepool.removeRAMFileFromNode(pnode, req.id, vnode)
                for vnode, pnode in vmrr.nodes.items():
                    self.rm.resourcepool.addRAMFileToNode(pnode, req.id, vnode, req.requested_resources.get_by_type(constants.RES_MEM))
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
        self.rm.logger.info("Received immediate lease request #%i (%i nodes)" % (lease_req.id, lease_req.numnodes), constants.SCHED)
        self.rm.logger.debug("  Duration: %s" % lease_req.duration, constants.SCHED)
        self.rm.logger.debug("  ResReq  : %s" % lease_req.requested_resources, constants.SCHED)
        
        try:
            self.__schedule_immediate_lease(lease_req, nexttime=nexttime)
            self.scheduledleases.add(lease_req)
            self.rm.stats.incr_counter(constants.COUNTER_IMACCEPTED, lease_req.id)
            self.rm.logger.info("Immediate lease request #%i has been accepted." % lease_req.id, constants.SCHED)
        except SchedException, msg:
            self.rm.stats.incr_counter(constants.COUNTER_IMREJECTED, lease_req.id)
            self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg), constants.SCHED)
        
        
    def __schedule_immediate_lease(self, req, nexttime):
        # Determine earliest start time in each node
        earliest = self.deployment.find_earliest_starting_times(req, nexttime)
        try:
            (resmrr, vmrr, susprr, reservation) = self.slottable.fitBestEffort(req, earliest, canreserve=False, suspendable=False, canmigrate=False, mustresume=False)
            # Schedule deployment
            self.deployment.schedule(req, vmrr, nexttime)
                        
            req.append_rr(vmrr)
            self.slottable.addReservation(vmrr)
                    
            req.print_contents()
        except SlotFittingException, msg:
            raise SchedException, "The requested immediate lease is infeasible. Reason: %s" % msg
        
    def preempt(self, req, time):
        self.rm.logger.info("Preempting lease #%i..." % (req.id), constants.SCHED)
        self.rm.logger.edebug("Lease before preemption:", constants.SCHED)
        req.print_contents()
        vmrr, susprr  = req.get_last_vmrr()
        suspendresumerate = self.rm.resourcepool.info.getSuspendResumeRate()
        
        if vmrr.state == constants.RES_STATE_SCHEDULED and vmrr.start >= time:
            self.rm.logger.info("... lease #%i has been cancelled and requeued." % req.id, constants.SCHED)
            self.rm.logger.debug("Lease was set to start in the middle of the preempting lease.", constants.SCHED)
            req.state = constants.LEASE_STATE_PENDING
            if vmrr.backfill_reservation == True:
                self.numbesteffortres -= 1
            req.remove_rr(vmrr)
            self.slottable.removeReservation(vmrr)
            if susprr != None:
                req.remove_rr(susprr)
                self.slottable.removeReservation(susprr)
            for vnode, pnode in req.vmimagemap.items():
                self.rm.resourcepool.removeImage(pnode, req.id, vnode)
            self.deployment.cancel_deployment(req)
            req.vmimagemap = {}
            self.scheduledleases.remove(req)
            self.queue.enqueue_in_order(req)
            self.rm.stats.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
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
                self.rm.logger.info("... lease #%i will be suspended at %s." % (req.id, time), constants.SCHED)
                self.slottable.suspend(req, time)
            else:
                self.rm.logger.info("... lease #%i has been cancelled and requeued (cannot be suspended)" % req.id, constants.SCHED)
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
                    self.rm.resourcepool.removeImage(pnode, req.id, vnode)
                self.deployment.cancel_deployment(req)
                req.vmimagemap = {}
                self.scheduledleases.remove(req)
                self.queue.enqueue_in_order(req)
                self.rm.stats.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
        self.rm.logger.edebug("Lease after preemption:", constants.SCHED)
        req.print_contents()
        
    def reevaluate_schedule(self, endinglease, nodes, nexttime, checkedleases):
        self.rm.logger.debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes, nexttime), constants.SCHED) 
        leases = self.scheduledleases.getNextLeasesScheduledInNodes(nexttime, nodes)
        leases = [l for l in leases if isinstance(l, ds.BestEffortLease) and not l in checkedleases]
        for l in leases:
            self.rm.logger.debug("Found lease %i" % l.id, constants.SCHED)
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
        self.rm.logger.debug("LEASE-%i Start of handleStartVM" % l.id, constants.SCHED)
        l.print_contents()
        if l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            now_time = self.rm.clock.get_time()
            l.start.actual = now_time
            
            try:
                self.rm.resourcepool.startVMs(l, rr)
                # The next two lines have to be moved somewhere more
                # appropriate inside the resourcepool module
                for (vnode, pnode) in rr.nodes.items():
                    l.vmimagemap[vnode] = pnode
            except Exception, e:
                self.rm.logger.error("ERROR when starting VMs.", constants.SCHED)
                raise

        elif l.state == constants.LEASE_STATE_SUSPENDED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            # No enactment to do here, since all the suspend/resume actions are
            # handled during the suspend/resume RRs
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RUN, l.id)
        self.rm.logger.debug("LEASE-%i End of handleStartVM" % l.id, constants.SCHED)
        self.rm.logger.info("Started VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()), constants.SCHED)

    # TODO: Replace enact with a saner way of handling leases that have failed or
    #       ended prematurely.
    #       Possibly factor out the "clean up" code to a separate function
    def _handle_end_vm(self, l, rr, enact=True):
        self.rm.logger.debug("LEASE-%i Start of handleEndVM" % l.id, constants.SCHED)
        self.rm.logger.edebug("LEASE-%i Before:" % l.id, constants.SCHED)
        l.print_contents()
        now_time = self.rm.clock.get_time()
        diff = now_time - rr.start
        l.duration.accumulate_duration(diff)
        rr.state = constants.RES_STATE_DONE
        if rr.oncomplete == constants.ONCOMPLETE_ENDLEASE:
            self.rm.resourcepool.stopVMs(l, rr)
            l.state = constants.LEASE_STATE_DONE
            l.duration.actual = l.duration.accumulated
            l.end = now_time
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            for vnode, pnode in l.vmimagemap.items():
                self.rm.resourcepool.removeImage(pnode, l.id, vnode)
            if isinstance(l, ds.BestEffortLease):
                self.rm.stats.incr_counter(constants.COUNTER_BESTEFFORTCOMPLETED, l.id)
       
        if isinstance(l, ds.BestEffortLease):
            if rr.backfill_reservation == True:
                self.numbesteffortres -= 1
        self.rm.logger.edebug("LEASE-%i After:" % l.id, constants.SCHED)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.rm.logger.debug("LEASE-%i End of handleEndVM" % l.id, constants.SCHED)
        self.rm.logger.info("Stopped VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()), constants.SCHED)

    def _handle_unscheduled_end_vm(self, l, rr, enact=False):
        self.rm.logger.info("LEASE-%i The VM has ended prematurely." % l.id, constants.SCHED)
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
        self.rm.logger.debug("LEASE-%i Start of handleStartSuspend" % l.id, constants.SCHED)
        l.print_contents()
        rr.state = constants.RES_STATE_ACTIVE
        self.rm.resourcepool.suspendVMs(l, rr)
        for vnode, pnode in rr.nodes.items():
            self.rm.resourcepool.addRAMFileToNode(pnode, l.id, vnode, l.requested_resources.get_by_type(constants.RES_MEM))
            l.memimagemap[vnode] = pnode
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_SUSPEND, l.id)
        self.rm.logger.debug("LEASE-%i End of handleStartSuspend" % l.id, constants.SCHED)
        self.rm.logger.info("Suspending lease %i..." % (l.id), constants.SCHED)

    def _handle_end_suspend(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleEndSuspend" % l.id, constants.SCHED)
        l.print_contents()
        # TODO: React to incomplete suspend
        self.rm.resourcepool.verifySuspend(l, rr)
        rr.state = constants.RES_STATE_DONE
        l.state = constants.LEASE_STATE_SUSPENDED
        self.scheduledleases.remove(l)
        self.queue.enqueue_in_order(l)
        self.rm.stats.incr_counter(constants.COUNTER_QUEUESIZE, l.id)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.rm.logger.debug("LEASE-%i End of handleEndSuspend" % l.id, constants.SCHED)
        self.rm.logger.info("Lease %i suspended." % (l.id), constants.SCHED)

    def _handle_start_resume(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleStartResume" % l.id, constants.SCHED)
        l.print_contents()
        self.rm.resourcepool.resumeVMs(l, rr)
        rr.state = constants.RES_STATE_ACTIVE
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RESUME, l.id)
        self.rm.logger.debug("LEASE-%i End of handleStartResume" % l.id, constants.SCHED)
        self.rm.logger.info("Resuming lease %i..." % (l.id), constants.SCHED)

    def _handle_end_resume(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleEndResume" % l.id, constants.SCHED)
        l.print_contents()
        # TODO: React to incomplete resume
        self.rm.resourcepool.verifyResume(l, rr)
        rr.state = constants.RES_STATE_DONE
        for vnode, pnode in rr.nodes.items():
            self.rm.resourcepool.removeRAMFileFromNode(pnode, l.id, vnode)
        l.print_contents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE, l.id)
        self.rm.logger.debug("LEASE-%i End of handleEndResume" % l.id, constants.SCHED)
        self.rm.logger.info("Resumed lease %i" % (l.id), constants.SCHED)

    def _handle_end_rr(self, l, rr):
        self.slottable.removeReservation(rr)



    def __enqueue_in_order(self, req):
        self.rm.stats.incr_counter(constants.COUNTER_QUEUESIZE, req.id)
        self.queue.enqueue_in_order(req)

    
    
        
    def canReserveBestEffort(self):
        return self.numbesteffortres < self.maxres
    
                        
    def updateNodeVMState(self, nodes, state, lease_id):
        for n in nodes:
            self.rm.resourcepool.getNode(n).vm_doing = state

    def updateNodeTransferState(self, nodes, state, lease_id):
        for n in nodes:
            self.rm.resourcepool.getNode(n).transfer_doing = state
            
    def is_backfilling(self):
        return self.maxres > 0
