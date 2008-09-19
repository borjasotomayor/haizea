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
the Scheduler class. The deployment scheduling code (everything that has to be 
done to prepare a lease) happens in the modules inside the 
haizea.resourcemanager.deployment package.

This module provides the following classes:

* SchedException: A scheduling exception
* ReservationEventHandler: A simple wrapper class
* Scheduler: Do I really need to spell this one out for you?

TODO: The Scheduler class is in need of some serious refactoring. The likely outcome is
that it will be divided into two classes: LeaseScheduler, which handles top-level
lease constructs and doesn't interact with the slot table, and VMScheduler, which
actually schedules the VMs. The slot table would be contained in VMScheduler and 
in the lease preparation scheduler. In turn, these two would be contained in
LeaseScheduler.
"""

import haizea.resourcemanager.datastruct as ds
import haizea.common.constants as constants
from haizea.common.utils import round_datetime_delta, round_datetime, estimate_transfer_time, get_config, get_accounting, get_clock
from haizea.resourcemanager.slottable import SlotTable, SlotFittingException
from haizea.resourcemanager.datastruct import Lease, ARLease, BestEffortLease, ImmediateLease, ResourceReservation, VMResourceReservation 
from haizea.resourcemanager.resourcepool import ResourcePool, ResourcePoolWithReusableImages
from operator import attrgetter, itemgetter
from mx.DateTime import TimeDelta

import logging

class SchedException(Exception):
    """A simple exception class used for scheduling exceptions"""
    pass

class NotSchedulableException(Exception):
    """A simple exception class used when a lease cannot be scheduled
    
    This exception must be raised when a lease cannot be scheduled
    (this is not necessarily an error condition, but the scheduler will
    have to react to it)
    """
    pass

class CriticalSchedException(Exception):
    """A simple exception class used for critical scheduling exceptions
    
    This exception must be raised when a non-recoverable error happens
    (e.g., when there are unexplained inconsistencies in the schedule,
    typically resulting from a code error)
    """
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
    def __init__(self, slottable, resourcepool, deployment_scheduler):
        self.slottable = slottable
        self.resourcepool = resourcepool
        self.deployment_scheduler = deployment_scheduler
        self.logger = logging.getLogger("SCHED")

        self.queue = ds.Queue(self)
        self.leases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        
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
        
        for (type, handler) in self.deployment_scheduler.handlers.items():
            self.handlers[type] = handler

        backfilling = get_config().get("backfilling")
        if backfilling == constants.BACKFILLING_OFF:
            self.maxres = 0
        elif backfilling == constants.BACKFILLING_AGGRESSIVE:
            self.maxres = 1
        elif backfilling == constants.BACKFILLING_CONSERVATIVE:
            self.maxres = 1000000 # Arbitrarily large
        elif backfilling == constants.BACKFILLING_INTERMEDIATE:
            self.maxres = get_config().get("backfilling-reservations")

        self.numbesteffortres = 0
        
    def schedule(self, nexttime):      
        pending_leases = self.leases.get_leases_by_state(Lease.STATE_PENDING)  
        ar_leases = [req for req in pending_leases if isinstance(req, ARLease)]
        im_leases = [req for req in pending_leases if isinstance(req, ImmediateLease)]
        be_leases = [req for req in pending_leases if isinstance(req, BestEffortLease)]
        
        # Queue best-effort requests
        for lease in be_leases:
            self.enqueue(lease)
        
        # Process immediate requests
        for lease_req in im_leases:
            self.__process_im_request(lease_req, nexttime)

        # Process AR requests
        for lease_req in ar_leases:
            self.__process_ar_request(lease_req, nexttime)
            
        # Process best-effort requests
        self.__process_queue(nexttime)
        
    
    def process_reservations(self, nowtime):
        starting = self.slottable.get_reservations_starting_at(nowtime)
        ending = self.slottable.get_reservations_ending_at(nowtime)
        for rr in ending:
            self._handle_end_rr(rr.lease, rr)
            self.handlers[type(rr)].on_end(self, rr.lease, rr)
        
        for rr in starting:
            self.handlers[type(rr)].on_start(self, rr.lease, rr)

        util = self.slottable.getUtilization(nowtime)
        get_accounting().append_stat(constants.COUNTER_CPUUTILIZATION, util)        
        
    def register_handler(self, type, on_start, on_end):
        handler = ReservationEventHandler(on_start=on_start, on_end=on_end)
        self.handlers[type] = handler        
    
    def enqueue(self, lease_req):
        """Queues a best-effort lease request"""
        get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
        lease_req.state = Lease.STATE_QUEUED
        self.queue.enqueue(lease_req)
        self.logger.info("Received (and queueing) best-effort lease request #%i, %i nodes for %s." % (lease_req.id, lease_req.numnodes, lease_req.duration.requested))

    def request_lease(self, lease):
        """
        Request a lease. At this point, it is simply marked as "Pending" and,
        next time the scheduling function is called, the fate of the
        lease will be determined (right now, AR+IM leases get scheduled
        right away, and best-effort leases get placed on a queue)
        """
        lease.state = Lease.STATE_PENDING
        self.leases.add(lease)

    def is_queue_empty(self):
        """Return True is the queue is empty, False otherwise"""
        return self.queue.is_empty()

    
    def exists_scheduled_leases(self):
        """Return True if there are any leases scheduled in the future"""
        return not self.slottable.is_empty()    

    def cancel_lease(self, lease_id):
        """Cancels a lease.
        
        Arguments:
        lease_id -- ID of lease to cancel
        """
        time = get_clock().get_time()
        
        self.logger.info("Cancelling lease %i..." % lease_id)
        if self.leases.has_lease(lease_id):
            # The lease is either running, or scheduled to run
            lease = self.leases.get_lease(lease_id)
            
            if lease.state == Lease.STATE_ACTIVE:
                self.logger.info("Lease %i is active. Stopping active reservation..." % lease_id)
                rr = lease.get_active_reservations(time)[0]
                if isinstance(rr, VMResourceReservation):
                    self._handle_unscheduled_end_vm(lease, rr, enact=True)
                # TODO: Handle cancelations in middle of suspensions and
                # resumptions                
            elif lease.state in [Lease.STATE_SCHEDULED, Lease.STATE_READY]:
                self.logger.info("Lease %i is scheduled. Cancelling reservations." % lease_id)
                rrs = lease.get_scheduled_reservations()
                for r in rrs:
                    lease.remove_rr(r)
                    self.slottable.removeReservation(r)
                lease.state = Lease.STATE_CANCELLED
                self.completedleases.add(lease)
                self.leases.remove(lease)
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
            raise CriticalSchedException()      
    
    def notify_event(self, lease_id, event):
        time = get_clock().get_time()
        if event == constants.EVENT_END_VM:
            lease = self.leases.get_lease(lease_id)
            vmrr = lease.get_last_vmrr()
            self._handle_unscheduled_end_vm(lease, vmrr, enact=False)

    
    def __process_ar_request(self, lease_req, nexttime):
        self.logger.info("Received AR lease request #%i, %i nodes from %s to %s." % (lease_req.id, lease_req.numnodes, lease_req.start.requested, lease_req.start.requested + lease_req.duration.requested))
        self.logger.debug("  Start   : %s" % lease_req.start)
        self.logger.debug("  Duration: %s" % lease_req.duration)
        self.logger.debug("  ResReq  : %s" % lease_req.requested_resources)
        
        accepted = False
        try:
            self.__schedule_ar_lease(lease_req, avoidpreempt=True, nexttime=nexttime)
            self.leases.add(lease_req)
            get_accounting().incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
            accepted = True
        except SchedException, msg:
            # Our first try avoided preemption, try again
            # without avoiding preemption.
            # TODO: Roll this into the exact slot fitting algorithm
            try:
                self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))
                self.logger.debug("LEASE-%i Trying again without avoiding preemption" % lease_req.id)
                self.__schedule_ar_lease(lease_req, nexttime, avoidpreempt=False)
                self.leases.add(lease_req)
                get_accounting().incr_counter(constants.COUNTER_ARACCEPTED, lease_req.id)
                accepted = True
            except SchedException, msg:
                get_accounting().incr_counter(constants.COUNTER_ARREJECTED, lease_req.id)
                self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))

        if accepted:
            self.logger.info("AR lease request #%i has been accepted." % lease_req.id)
        else:
            self.logger.info("AR lease request #%i has been rejected." % lease_req.id)
        
        
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
                    self.leases.add(lease_req)
                    get_accounting().decr_counter(constants.COUNTER_QUEUESIZE, lease_req.id)
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


    def __process_im_request(self, lease_req, nexttime):
        self.logger.info("Received immediate lease request #%i (%i nodes)" % (lease_req.id, lease_req.numnodes))
        self.logger.debug("  Duration: %s" % lease_req.duration)
        self.logger.debug("  ResReq  : %s" % lease_req.requested_resources)
        
        try:
            self.__schedule_immediate_lease(lease_req, nexttime=nexttime)
            self.leases.add(lease_req)
            get_accounting().incr_counter(constants.COUNTER_IMACCEPTED, lease_req.id)
            self.logger.info("Immediate lease request #%i has been accepted." % lease_req.id)
        except SchedException, msg:
            get_accounting().incr_counter(constants.COUNTER_IMREJECTED, lease_req.id)
            self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))
    
    
    def __schedule_ar_lease(self, lease_req, nexttime, avoidpreempt=True):
        start = lease_req.start.requested
        end = lease_req.start.requested + lease_req.duration.requested
        try:
            (nodeassignment, res, preemptions) = self.__fit_exact(lease_req, preemptible=False, canpreempt=True, avoidpreempt=avoidpreempt)
            
            if len(preemptions) > 0:
                leases = self.__find_preemptable_leases(preemptions, start, end)
                self.logger.info("Must preempt leases %s to make room for AR lease #%i" % ([l.id for l in leases], lease_req.id))
                for lease in leases:
                    self.__preempt(lease, preemption_time=start)

            # Create VM resource reservations
            vmrr = ds.VMResourceReservation(lease_req, start, end, nodeassignment, res, False)
            vmrr.state = ResourceReservation.STATE_SCHEDULED

            # Schedule deployment overhead
            self.deployment_scheduler.schedule(lease_req, vmrr, nexttime)
            
            # Commit reservation to slot table
            # (we don't do this until the very end because the deployment overhead
            # scheduling could still throw an exception)
            lease_req.append_vmrr(vmrr)
            self.slottable.addReservation(vmrr)
        except SlotFittingException, msg:
            raise SchedException, "The requested AR lease is infeasible. Reason: %s" % msg


    def __schedule_besteffort_lease(self, lease, nexttime):            
        try:
            # Schedule the VMs
            canreserve = self.__can_reserve_besteffort_in_future()
            (vmrr, in_future) = self.__fit_asap(lease, nexttime, allow_reservation_in_future = canreserve)
            
            # Schedule deployment
            if lease.state != Lease.STATE_SUSPENDED:
                self.deployment_scheduler.schedule(lease, vmrr, nexttime)
            else:
                # TODO: schedule migrations
                pass

            # At this point, the lease is feasible.
            # Commit changes by adding RRs to lease and to slot table
            
            # Add resource reservations to lease
            # TODO: deployment
            # TODO: migrations
            lease.append_vmrr(vmrr)
            

            # Add resource reservations to slottable
            
            # TODO: deployment
            
            # TODO: migrations
            
            # Resumptions (if any)
            for resmrr in vmrr.resm_rrs:
                self.slottable.addReservation(resmrr)
                
            # VM
            self.slottable.addReservation(vmrr)
            
            # Suspensions (if any)
            for susprr in vmrr.susp_rrs:
                self.slottable.addReservation(susprr)
           
            if in_future:
                self.numbesteffortres += 1
                
            lease.print_contents()

        except SchedException, msg:
            raise SchedException, "The requested best-effort lease is infeasible. Reason: %s" % msg

        
        
        
    def __schedule_immediate_lease(self, req, nexttime):
        try:
            (resmrr, vmrr, susprr, reservation) = self.__fit_asap(req, nexttime, allow_reservation_in_future=False)
            # Schedule deployment
            self.deployment_scheduler.schedule(req, vmrr, nexttime)
                        
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

    def __fit_asap(self, lease, nexttime, allow_reservation_in_future = False):
        lease_id = lease.id
        remaining_duration = lease.duration.get_remaining_duration()
        numnodes = lease.numnodes
        requested_resources = lease.requested_resources
        preemptible = lease.preemptible
        mustresume = (lease.state == Lease.STATE_SUSPENDED)
        susptype = get_config().get("suspension")
        if susptype == constants.SUSPENSION_NONE or (susptype == constants.SUSPENSION_SERIAL and lease.numnodes == 1):
            suspendable = False
        else:
            suspendable = True

        # Determine earliest start time in each node
        if lease.state == Lease.STATE_QUEUED:
            # Figure out earliest start times based on
            # image schedule and reusable images
            earliest = self.deployment_scheduler.find_earliest_starting_times(lease, nexttime)
        elif lease.state == Lease.STATE_SUSPENDED:
            # No need to transfer images from repository
            # (only intra-node transfer)
            earliest = dict([(node+1, [nexttime, constants.REQTRANSFER_NO, None]) for node in range(lease.numnodes)])


        canmigrate = get_config().get("migration")

        #
        # STEP 1: FIGURE OUT THE MINIMUM DURATION
        #
        
        min_duration = self.__compute_scheduling_threshold(lease)


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
            if not canmigrate:
                vmrr = lease.get_last_vmrr()
                curnodes = set(vmrr.nodes.values())
            else:
                curnodes=None
                # If we have to resume this lease, make sure that
                # we have enough time to transfer the images.
                migratetime = self.__estimate_migration_time(lease)
                earliesttransfer = get_clock().get_time() + migratetime
    
                for n in earliest:
                    earliest[n][0] = max(earliest[n][0], earliesttransfer)

            changepoints = list(set([x[0] for x in earliest.values()]))
            changepoints.sort()
            changepoints = [(x, curnodes) for x in changepoints]

        # If we can make reservations in the future,
        # we also consider future changepoints
        # (otherwise, we only allow the VMs to start "now", accounting
        #  for the fact that vm images will have to be deployed)
        if allow_reservation_in_future:
            futurecp = self.slottable.findChangePointsAfter(changepoints[-1][0])
            futurecp = [(p,None) for p in futurecp]
        else:
            futurecp = []



        #
        # STEP 3: SLOT FITTING
        #
        
        # If resuming, we also have to allocate enough for the resumption
        if mustresume:
            duration = remaining_duration + self.__estimate_resume_time(lease)
        else:
            duration = remaining_duration


        # First, assuming we can't make reservations in the future
        start, end, canfit = self.__find_fit_at_points(
                                                       changepoints, 
                                                       numnodes, 
                                                       requested_resources, 
                                                       duration, 
                                                       suspendable, 
                                                       min_duration)
        
        if start == None:
            if not allow_reservation_in_future:
                # We did not find a suitable starting time. This can happen
                # if we're unable to make future reservations
                raise SchedException, "Could not find enough resources for this request"
        else:
            mustsuspend = (end - start) < duration
            if mustsuspend and not suspendable:
                if not allow_reservation_in_future:
                    raise SchedException, "Scheduling this lease would require preempting it, which is not allowed"
                else:
                    start = None # No satisfactory start time
            
        # If we haven't been able to fit the lease, check if we can
        # reserve it in the future
        if start == None and allow_reservation_in_future:
            start, end, canfit = self.__find_fit_at_points(
                                                           futurecp, 
                                                           numnodes, 
                                                           requested_resources, 
                                                           duration, 
                                                           suspendable, 
                                                           min_duration
                                                           )


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
            vmrr = lease.get_last_vmrr()
            nodes = set(vmrr.nodes.values())
            availnodes = set(physnodes)
            deplnodes = availnodes.intersection(nodes)
            notdeplnodes = availnodes.difference(nodes)
            physnodes = list(deplnodes) + list(notdeplnodes)
        else:
            physnodes.sort() # Arbitrary, prioritize nodes, as in exact
        
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
                        res[n].incr(requested_resources)
                    else:
                        res[n] = ds.ResourceTuple.copy(requested_resources)
                    vmnode += 1
                    break


        vmrr = ds.VMResourceReservation(lease, start, end, mappings, res, reservation)
        vmrr.state = ResourceReservation.STATE_SCHEDULED

        if mustresume:
            self.__schedule_resumption(vmrr, start)

        mustsuspend = (vmrr.end - vmrr.start) < remaining_duration
        if mustsuspend:
            self.__schedule_suspension(vmrr, end)
        
        # Compensate for any overestimation
        if (vmrr.end - vmrr.start) > remaining_duration:
            vmrr.end = vmrr.start + remaining_duration
        
        susp_str = res_str = ""
        if mustresume:
            res_str = " (resuming)"
        if mustsuspend:
            susp_str = " (suspending)"
        self.logger.info("Lease #%i has been scheduled on nodes %s from %s%s to %s%s" % (lease.id, mappings.values(), start, res_str, end, susp_str))

        return vmrr, reservation

    def __find_fit_at_points(self, changepoints, numnodes, resources, duration, suspendable, min_duration):
        start = None
        end = None
        canfit = None
        availabilitywindow = self.slottable.availabilitywindow


        for p in changepoints:
            availabilitywindow.initWindow(p[0], resources, p[1], canpreempt = False)
            availabilitywindow.printContents()
            
            if availabilitywindow.fitAtStart() >= numnodes:
                start=p[0]
                maxend = start + duration
                end, canfit = availabilitywindow.findPhysNodesForVMs(numnodes, maxend)
        
                self.logger.debug("This lease can be scheduled from %s to %s" % (start, end))
                
                if end < maxend:
                    self.logger.debug("This lease will require suspension (maxend = %s)" % (maxend))
                    
                    if not suspendable:
                        pass
                        # If we can't suspend, this fit is no good, and we have to keep looking
                    else:
                        # If we can suspend, we still have to check if the lease will
                        # be able to run for the specified minimum duration
                        if end-start > min_duration:
                            break # We found a fit; stop looking
                        else:
                            self.logger.debug("This starting time does not allow for the requested minimum duration (%s < %s)" % (end-start, min_duration))
                            # Set start back to None, to indicate that we haven't
                            # found a satisfactory start time
                            start = None
                else:
                    # We've found a satisfactory starting time
                    break        
                
        return start, end, canfit
    
    def __compute_susprem_times(self, vmrr, time, direction, exclusion, rate):
        times = [] # (start, end, pnode, vnodes)

        if exclusion == constants.SUSPRES_EXCLUSION_GLOBAL:
            # Global exclusion (which represents, e.g., reading/writing the memory image files
            # from a global file system) meaning no two suspensions/resumptions can happen at 
            # the same time in the entire resource pool.
            
            t = time
            t_prev = None
                
            for (vnode,pnode) in vmrr.nodes.items():
                mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                op_time = self.__compute_suspend_resume_time(mem, rate)
                t_prev = t
                
                if direction == constants.DIRECTION_FORWARD:
                    t += op_time
                    times.append((t_prev, t, pnode, [vnode]))
                elif direction == constants.DIRECTION_BACKWARD:
                    t -= op_time
                    times.append((t, t_prev, pnode, [vnode]))

        elif exclusion == constants.SUSPRES_EXCLUSION_LOCAL:
            # Local exclusion (which represents, e.g., reading the memory image files
            # from a local file system) means no two resumptions can happen at the same
            # time in the same physical node.
            vnodes_in_pnode = {}
            for (vnode,pnode) in vmrr.nodes.items():
                vnodes_in_pnode.setdefault(pnode, []).append(vnode)
            for pnode in vnodes_in_pnode:
                t = time
                t_prev = None
                for vnode in vnodes_in_pnode[pnode]:
                    mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                    op_time = self.__compute_suspend_resume_time(mem, rate)
                    t_prev = t
                    
                    if direction == constants.DIRECTION_FORWARD:
                        t += op_time
                        times.append((t_prev, t, pnode, [vnode]))
                    elif direction == constants.DIRECTION_BACKWARD:
                        t -= op_time
                        times.append((t, t_prev, pnode, [vnode]))
            # TODO: "consolidate" times (i.e., figure out what operations can be grouped
            # into a single RR. This will not be an issue when running with real hardware,
            # but might impact simulation performance.
        
        return times


    def __schedule_resumption(self, vmrr, resume_at):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        resm_exclusion = config.get("suspendresume-exclusion")        
        rate = self.resourcepool.info.get_suspendresume_rate()

        if resume_at < vmrr.start or resume_at > vmrr.end:
            raise SchedException, "Tried to schedule a resumption at %s, which is outside the VMRR's duration (%s-%s)" % (resume_at, vmrr.start, vmrr.end)

        times = self.__compute_susprem_times(vmrr, resume_at, constants.DIRECTION_FORWARD, resm_exclusion, rate)
        resume_rrs = []
        for (start, end, pnode, vnodes) in times:
            r = ds.ResourceTuple.create_empty()
            mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
            r.set_by_type(constants.RES_MEM, mem)
            r.set_by_type(constants.RES_DISK, mem)
            resmres = {pnode: r}
            resmrr = ds.ResumptionResourceReservation(vmrr.lease, start, end, resmres, vnodes, vmrr)
            resmrr.state = ResourceReservation.STATE_SCHEDULED
            resume_rrs.append(resmrr)
                
        resume_rrs.sort(key=attrgetter("start"))
            
        resm_end = resume_rrs[-1].end
        if resm_end > vmrr.end:
            raise SchedException, "Determined resumption would end at %s, after the VMRR's end (%s) -- Resume time not being properly estimated?" % (resm_end, vmrr.end)
        
        vmrr.update_start(resm_end)
        for resmrr in resume_rrs:
            vmrr.resm_rrs.append(resmrr)        
           
    
    def __schedule_suspension(self, vmrr, suspend_by):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        susp_exclusion = config.get("suspendresume-exclusion")        
        rate = self.resourcepool.info.get_suspendresume_rate()

        if suspend_by < vmrr.start or suspend_by > vmrr.end:
            raise SchedException, "Tried to schedule a suspension by %s, which is outside the VMRR's duration (%s-%s)" % (suspend_by, vmrr.start, vmrr.end)

        times = self.__compute_susprem_times(vmrr, suspend_by, constants.DIRECTION_BACKWARD, susp_exclusion, rate)
        suspend_rrs = []
        for (start, end, pnode, vnodes) in times:
            r = ds.ResourceTuple.create_empty()
            mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
            r.set_by_type(constants.RES_MEM, mem)
            r.set_by_type(constants.RES_DISK, mem)
            suspres = {pnode: r}
            susprr = ds.SuspensionResourceReservation(vmrr.lease, start, end, suspres, vnodes, vmrr)
            susprr.state = ResourceReservation.STATE_SCHEDULED
            suspend_rrs.append(susprr)
                
        suspend_rrs.sort(key=attrgetter("start"))
            
        susp_start = suspend_rrs[0].start
        if susp_start < vmrr.start:
            raise SchedException, "Determined suspension should start at %s, before the VMRR's start (%s) -- Suspend time not being properly estimated?" % (susp_start, vmrr.start)
        
        vmrr.update_end(susp_start)
        for susprr in suspend_rrs:
            vmrr.susp_rrs.append(susprr)        

    def __compute_suspend_resume_time(self, mem, rate):
        time = float(mem) / rate
        time = round_datetime_delta(TimeDelta(seconds = time))
        return time
    
    def __estimate_suspend_resume_time(self, lease):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        susp_exclusion = config.get("suspendresume-exclusion")        
        rate = self.resourcepool.info.get_suspendresume_rate()
        mem = lease.requested_resources.get_by_type(constants.RES_MEM)
        if susp_exclusion == constants.SUSPRES_EXCLUSION_GLOBAL:
            return lease.numnodes * self.__compute_suspend_resume_time(mem, rate)
        elif susp_exclusion == constants.SUSPRES_EXCLUSION_LOCAL:
            # Overestimating
            return lease.numnodes * self.__compute_suspend_resume_time(mem, rate)

    def __estimate_suspend_time(self, lease):
        return self.__estimate_suspend_resume_time(lease)

    def __estimate_resume_time(self, lease):
        return self.__estimate_suspend_resume_time(lease)


    def __estimate_migration_time(self, lease):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        whattomigrate = config.get("what-to-migrate")
        bandwidth = self.resourcepool.info.get_migration_bandwidth()
        if whattomigrate == constants.MIGRATE_NONE:
            return TimeDelta(seconds=0)
        else:
            if whattomigrate == constants.MIGRATE_MEM:
                mbtotransfer = lease.requested_resources.get_by_type(constants.RES_MEM)
            elif whattomigrate == constants.MIGRATE_MEMDISK:
                mbtotransfer = lease.diskimage_size + lease.requested_resources.get_by_type(constants.RES_MEM)
            return estimate_transfer_time(mbtotransfer, bandwidth)

    # TODO: Take into account other things like boot overhead, migration overhead, etc.
    def __compute_scheduling_threshold(self, lease):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        threshold = config.get("force-scheduling-threshold")
        if threshold != None:
            # If there is a hard-coded threshold, use that
            return threshold
        else:
            factor = config.get("scheduling-threshold-factor")
            susp_overhead = self.__estimate_suspend_time(lease)
            safe_duration = susp_overhead
            
            if lease.state == Lease.STATE_SUSPENDED:
                resm_overhead = self.__estimate_resume_time(lease)
                safe_duration += resm_overhead
            
            # TODO: Incorporate other overheads into the minimum duration
            min_duration = safe_duration
            
            # At the very least, we want to allocate enough time for the
            # safe duration (otherwise, we'll end up with incorrect schedules,
            # where a lease is scheduled to suspend, but isn't even allocated
            # enough time to suspend). 
            # The factor is assumed to be non-negative. i.e., a factor of 0
            # means we only allocate enough time for potential suspend/resume
            # operations, while a factor of 1 means the lease will get as much
            # running time as spend on the runtime overheads involved in setting
            # it up
            threshold = safe_duration + (min_duration * factor)
            return threshold

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
        #self.lease_deployment_type = get_config().get("lease-preparation")
        #if self.lease_deployment_type == constants.DEPLOYMENT_TRANSFER:
        #    reusealg = get_config().get("diskimage-reuse")
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
        
        reservationsAtMiddle = self.slottable.get_reservations_starting_between(startTime, endTime)
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
        
    def __preempt(self, lease, preemption_time):
        
        self.logger.info("Preempting lease #%i..." % (lease.id))
        self.logger.vdebug("Lease before preemption:")
        lease.print_contents()
        vmrr = lease.get_last_vmrr()
        
        if vmrr.state == ResourceReservation.STATE_SCHEDULED and vmrr.start >= preemption_time:
            self.logger.debug("Lease was set to start in the middle of the preempting lease.")
            must_cancel_and_requeue = True
        else:
            susptype = get_config().get("suspension")
            time_until_suspend = preemption_time - vmrr.start
            min_duration = self.__compute_scheduling_threshold(lease)
            can_suspend = time_until_suspend >= min_duration        
            if not can_suspend:
                self.logger.debug("Suspending the lease does not meet scheduling threshold.")
                must_cancel_and_requeue = True
            else:
                if lease.numnodes > 1 and susptype == constants.SUSPENSION_SERIAL:
                    self.logger.debug("Can't suspend lease because only suspension of single-node leases is allowed.")
                    must_cancel_and_requeue = True
                else:
                    self.logger.debug("Lease can be suspended")
                    must_cancel_and_requeue = False
                    
        if must_cancel_and_requeue:
            self.logger.info("... lease #%i has been cancelled and requeued." % lease.id)
            if vmrr.backfill_reservation == True:
                self.numbesteffortres -= 1
            if vmrr.is_suspending():
                for susprr in vmrr.susp_rrs:
                    self.slottable.removeReservation(susprr)
            lease.remove_vmrr(vmrr)
            self.slottable.removeReservation(vmrr)
            for vnode, pnode in lease.vmimagemap.items():
                self.resourcepool.remove_diskimage(pnode, lease.id, vnode)
            self.deployment_scheduler.cancel_deployment(lease)
            lease.vmimagemap = {}
            lease.state = Lease.STATE_QUEUED
            self.__enqueue_in_order(lease)
            get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        else:
            self.logger.info("... lease #%i will be suspended at %s." % (lease.id, preemption_time))
            # Save original start and end time of the vmrr
            old_start = vmrr.start
            old_end = vmrr.end
            self.__schedule_suspension(vmrr, preemption_time)
            self.slottable.update_reservation_with_key_change(vmrr, old_start, old_end)
            for susprr in vmrr.susp_rrs:
                self.slottable.addReservation(susprr)
            
            
        self.logger.vdebug("Lease after preemption:")
        lease.print_contents()
        
    def __reevaluate_schedule(self, endinglease, nodes, nexttime, checkedleases):
        self.logger.debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes, nexttime)) 
        leases = []
        vmrrs = self.slottable.get_next_reservations_in_nodes(nexttime, nodes, rr_type=VMResourceReservation, immediately_next=True)
        leases = set([rr.lease for rr in vmrrs])
        leases = [l for l in leases if isinstance(l, ds.BestEffortLease) and not l in checkedleases]
        for lease in leases:
            self.logger.debug("Found lease %i" % l.id)
            l.print_contents()
            # Earliest time can't be earlier than time when images will be
            # available in node
            earliest = max(nexttime, lease.imagesavail)
            self.__slideback(lease, earliest)
            checkedleases.append(l)
        #for l in leases:
        #    vmrr, susprr = l.getLastVMRR()
        #    self.reevaluateSchedule(l, vmrr.nodes.values(), vmrr.end, checkedleases)
          
    def __slideback(self, lease, earliest):
        vmrr = lease.get_last_vmrr()
        # Save original start and end time of the vmrr
        old_start = vmrr.start
        old_end = vmrr.end
        nodes = vmrr.nodes.values()
        if lease.state == Lease.STATE_SUSPENDED:
            originalstart = vmrr.resm_rrs[0].start
        else:
            originalstart = vmrr.start
        cp = self.slottable.findChangePointsAfter(after=earliest, until=originalstart, nodes=nodes)
        cp = [earliest] + cp
        newstart = None
        for p in cp:
            self.slottable.availabilitywindow.initWindow(p, lease.requested_resources, canpreempt=False)
            self.slottable.availabilitywindow.printContents()
            if self.slottable.availabilitywindow.fitAtStart(nodes=nodes) >= lease.numnodes:
                (end, canfit) = self.slottable.availabilitywindow.findPhysNodesForVMs(lease.numnodes, originalstart)
                if end == originalstart and set(nodes) <= set(canfit.keys()):
                    self.logger.debug("Can slide back to %s" % p)
                    newstart = p
                    break
        if newstart == None:
            # Can't slide back. Leave as is.
            pass
        else:
            diff = originalstart - newstart
            if lease.state == Lease.STATE_SUSPENDED:
                for resmrr in vmrr.resm_rrs:
                    resmrr_old_start = resmrr.start
                    resmrr_old_end = resmrr.end
                    resmrr.start -= diff
                    resmrr.end -= diff
                    self.slottable.update_reservation_with_key_change(resmrr, resmrr_old_start, resmrr_old_end)
            vmrr.update_start(vmrr.start - diff)
            
            # If the lease was going to be suspended, check to see if
            # we don't need to suspend any more.
            remdur = lease.duration.get_remaining_duration()
            if vmrr.is_suspending() and vmrr.end - newstart >= remdur: 
                vmrr.update_end(vmrr.start + remdur)
                for susprr in vmrr.susp_rrs:
                    self.slottable.removeReservation(susprr)
                vmrr.susp_rrs = []
            else:
                vmrr.update_end(vmrr.end - diff)

            self.slottable.update_reservation_with_key_change(vmrr, old_start, old_end)
            self.logger.vdebug("New lease descriptor (after slideback):")
            lease.print_contents()
    
          

    #-------------------------------------------------------------------#
    #                                                                   #
    #                  SLOT TABLE EVENT HANDLERS                        #
    #                                                                   #
    #-------------------------------------------------------------------#

    def _handle_start_vm(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartVM" % l.id)
        l.print_contents()
        if l.state == Lease.STATE_READY:
            l.state = Lease.STATE_ACTIVE
            rr.state = ResourceReservation.STATE_ACTIVE
            now_time = get_clock().get_time()
            l.start.actual = now_time
            
            try:
                self.deployment_scheduler.check(l, rr)
                self.resourcepool.start_vms(l, rr)
                # The next two lines have to be moved somewhere more
                # appropriate inside the resourcepool module
                for (vnode, pnode) in rr.nodes.items():
                    l.diskimagemap[vnode] = pnode
            except Exception, e:
                self.logger.error("ERROR when starting VMs.")
                raise
        elif l.state == Lease.STATE_RESUMED_READY:
            l.state = Lease.STATE_ACTIVE
            rr.state = ResourceReservation.STATE_ACTIVE
            # No enactment to do here, since all the suspend/resume actions are
            # handled during the suspend/resume RRs
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartVM" % l.id)
        self.logger.info("Started VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))

    # TODO: Replace enact with a saner way of handling leases that have failed or
    #       ended prematurely.
    #       Possibly factor out the "clean up" code to a separate function
    def _handle_end_vm(self, l, rr, enact=True):
        self.logger.debug("LEASE-%i Start of handleEndVM" % l.id)
        self.logger.vdebug("LEASE-%i Before:" % l.id)
        l.print_contents()
        now_time = round_datetime(get_clock().get_time())
        diff = now_time - rr.start
        l.duration.accumulate_duration(diff)
        rr.state = ResourceReservation.STATE_DONE
        if not rr.is_suspending():
            self.resourcepool.stop_vms(l, rr)
            l.state = Lease.STATE_DONE
            l.duration.actual = l.duration.accumulated
            l.end = now_time
            self.completedleases.add(l)
            self.leases.remove(l)
            if isinstance(l, ds.BestEffortLease):
                get_accounting().incr_counter(constants.COUNTER_BESTEFFORTCOMPLETED, l.id)
       
        if isinstance(l, ds.BestEffortLease):
            if rr.backfill_reservation == True:
                self.numbesteffortres -= 1
        self.logger.vdebug("LEASE-%i After:" % l.id)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndVM" % l.id)
        self.logger.info("Stopped VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))

    def _handle_unscheduled_end_vm(self, l, rr, enact=False):
        self.logger.info("LEASE-%i The VM has ended prematurely." % l.id)
        self._handle_end_rr(l, rr)
        if rr.is_suspending():
            rrs = l.next_rrs(rr)
            for r in rrs:
                l.remove_rr(r)
                self.slottable.removeReservation(r)
        rr.end = get_clock().get_time()
        self._handle_end_vm(l, rr, enact=enact)
        nexttime = get_clock().get_next_schedulable_time()
        if self.is_backfilling():
            # We need to reevaluate the schedule to see if there are any future
            # reservations that we can slide back.
            self.__reevaluate_schedule(l, rr.nodes.values(), nexttime, [])

    def _handle_start_suspend(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartSuspend" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        self.resourcepool.suspend_vms(l, rr)
        for vnode in rr.vnodes:
            pnode = rr.vmrr.nodes[vnode]
            l.memimagemap[vnode] = pnode
        if rr.is_first():
            l.state = Lease.STATE_SUSPENDING
            l.print_contents()
            self.logger.info("Suspending lease %i..." % (l.id))
        self.logger.debug("LEASE-%i End of handleStartSuspend" % l.id)

    def _handle_end_suspend(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndSuspend" % l.id)
        l.print_contents()
        # TODO: React to incomplete suspend
        self.resourcepool.verify_suspend(l, rr)
        rr.state = ResourceReservation.STATE_DONE
        if rr.is_last():
            l.state = Lease.STATE_SUSPENDED
            self.__enqueue_in_order(l)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndSuspend" % l.id)
        self.logger.info("Lease %i suspended." % (l.id))

    def _handle_start_resume(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartResume" % l.id)
        l.print_contents()
        self.resourcepool.resume_vms(l, rr)
        rr.state = ResourceReservation.STATE_ACTIVE
        if rr.is_first():
            l.state = Lease.STATE_RESUMING
            l.print_contents()
            self.logger.info("Resuming lease %i..." % (l.id))
        self.logger.debug("LEASE-%i End of handleStartResume" % l.id)

    def _handle_end_resume(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndResume" % l.id)
        l.print_contents()
        # TODO: React to incomplete resume
        self.resourcepool.verify_resume(l, rr)
        rr.state = ResourceReservation.STATE_DONE
        if rr.is_last():
            l.state = Lease.STATE_RESUMED_READY
            self.logger.info("Resumed lease %i" % (l.id))
        for vnode, pnode in rr.vmrr.nodes.items():
            self.resourcepool.remove_ramfile(pnode, l.id, vnode)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndResume" % l.id)

    def _handle_start_migrate(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartMigrate" % l.id)
        l.print_contents()

        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartMigrate" % l.id)
        self.logger.info("Migrating lease %i..." % (l.id))

    def _handle_end_migrate(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndMigrate" % l.id)
        l.print_contents()

#        if lease.state == Lease.STATE_SUSPENDED:
#            # Update VM image mappings, since we might be resuming
#            # in different nodes.
#            for vnode, pnode in lease.vmimagemap.items():
#                self.resourcepool.remove_diskimage(pnode, lease.id, vnode)
#            lease.vmimagemap = vmrr.nodes
#            for vnode, pnode in lease.vmimagemap.items():
#                self.resourcepool.add_diskimage(pnode, lease.diskimage_id, lease.diskimage_size, lease.id, vnode)
#            
#            # Update RAM file mappings
#            for vnode, pnode in lease.memimagemap.items():
#                self.resourcepool.remove_ramfile(pnode, lease.id, vnode)
#            for vnode, pnode in vmrr.nodes.items():
#                self.resourcepool.add_ramfile(pnode, lease.id, vnode, lease.requested_resources.get_by_type(constants.RES_MEM))
#                lease.memimagemap[vnode] = pnode

        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndMigrate" % l.id)
        self.logger.info("Migrated lease %i..." % (l.id))

    def _handle_end_rr(self, l, rr):
        self.slottable.removeReservation(rr)

    def __enqueue_in_order(self, lease):
        get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        self.queue.enqueue_in_order(lease)
        
    def __can_reserve_besteffort_in_future(self):
        return self.numbesteffortres < self.maxres
                
    def is_backfilling(self):
        return self.maxres > 0
