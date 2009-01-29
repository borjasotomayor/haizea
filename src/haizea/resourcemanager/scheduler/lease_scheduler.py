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
"""
import haizea.common.constants as constants
from haizea.common.utils import round_datetime_delta, round_datetime, estimate_transfer_time, get_config, get_accounting, get_clock
from haizea.resourcemanager.leases import Lease, ARLease, BestEffortLease, ImmediateLease
from haizea.resourcemanager.scheduler import SchedException, RescheduleLeaseException, NormalEndLeaseException
from haizea.resourcemanager.scheduler.slottable import SlotTable, SlotFittingException, ResourceReservation
from haizea.resourcemanager.scheduler.resourcepool import ResourcePool, ResourcePoolWithReusableImages
from haizea.resourcemanager.scheduler.vm_scheduler import VMResourceReservation, SuspensionResourceReservation, ResumptionResourceReservation, ShutdownResourceReservation
from operator import attrgetter, itemgetter
from mx.DateTime import TimeDelta

import logging

class LeaseScheduler(object):
    """The Haizea Lease Scheduler
    
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
    def __init__(self, vm_scheduler, preparation_scheduler, slottable):
        self.vm_scheduler = vm_scheduler
        self.preparation_scheduler = preparation_scheduler
        self.slottable = slottable
        self.logger = logging.getLogger("LSCHED")

        self.queue = Queue(self)
        self.leases = LeaseTable(self)
        self.completedleases = LeaseTable(self)

        self.handlers = {}
        for (type, handler) in self.vm_scheduler.handlers.items():
            self.handlers[type] = handler

        for (type, handler) in self.preparation_scheduler.handlers.items():
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
        starting = [res for res in starting if res.state == ResourceReservation.STATE_SCHEDULED]
        ending = self.slottable.get_reservations_ending_at(nowtime)
        ending = [res for res in ending if res.state == ResourceReservation.STATE_ACTIVE]
        
        for rr in ending:
            lease = rr.lease
            self._handle_end_rr(lease, rr)
            try:
                self.handlers[type(rr)].on_end(lease, rr)
            except RescheduleLeaseException, msg:
                if isinstance(rr.lease, BestEffortLease):
                    self.__enqueue_in_order(lease)
            except NormalEndLeaseException, msg:
                self._handle_end_lease(lease)
        
        for rr in starting:
            self.handlers[type(rr)].on_start(rr.lease, rr)
            

        # TODO: Should be in VMScheduler
        util = self.__get_utilization(nowtime)
        if not util.has_key(VMResourceReservation):
            cpuutil = 0.0
        else:
            cpuutil = util[VMResourceReservation]
        get_accounting().append_stat(constants.COUNTER_CPUUTILIZATION, cpuutil)        
        get_accounting().append_stat(constants.COUNTER_UTILIZATION, util)        

    
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
            self._handle_end_rr(lease, vmrr)
            self.vm_scheduler._handle_unscheduled_end_vm(lease, vmrr, enact=False)
            self._handle_end_lease(lease)
            nexttime = get_clock().get_next_schedulable_time()
            # We need to reevaluate the schedule to see if there are any future
            # reservations that we can slide back.
            self.vm_scheduler.reevaluate_schedule(lease, vmrr.nodes.values(), nexttime, [])

            
        

    
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
                raise
                get_accounting().incr_counter(constants.COUNTER_ARREJECTED, lease_req.id)
                self.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.id, msg))

        if accepted:
            self.logger.info("AR lease request #%i has been accepted." % lease_req.id)
        else:
            self.logger.info("AR lease request #%i has been rejected." % lease_req.id)
            lease_req.state = Lease.STATE_REJECTED
            self.completedleases.add(lease_req)
            self.leases.remove(lease_req)
        
        
    def __process_queue(self, nexttime):
        done = False
        newqueue = Queue(self)
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
        try:
            (vmrr, preemptions) = self.vm_scheduler.fit_exact(lease_req, preemptible=False, canpreempt=True, avoidpreempt=avoidpreempt)
            
            if len(preemptions) > 0:
                leases = self.vm_scheduler.find_preemptable_leases(preemptions, vmrr.start, vmrr.end)
                self.logger.info("Must preempt leases %s to make room for AR lease #%i" % ([l.id for l in leases], lease_req.id))
                for lease in leases:
                    self.__preempt(lease, preemption_time=vmrr.start)

            # Schedule deployment overhead
            self.preparation_scheduler.schedule(lease_req, vmrr, nexttime)
            
            # Commit reservation to slot table
            # (we don't do this until the very end because the deployment overhead
            # scheduling could still throw an exception)
            lease_req.append_vmrr(vmrr)
            self.slottable.addReservation(vmrr)
            
            # Post-VM RRs (if any)
            for rr in vmrr.post_rrs:
                self.slottable.addReservation(rr)
        except Exception, msg:
            raise SchedException, "The requested AR lease is infeasible. Reason: %s" % msg


    def __schedule_besteffort_lease(self, lease, nexttime):            
        try:
            # Schedule the VMs
            canreserve = self.vm_scheduler.can_reserve_besteffort_in_future()
            
            # Determine earliest start time in each node
            if lease.state == Lease.STATE_QUEUED or lease.state == Lease.STATE_PENDING:
                # Figure out earliest start times based on
                # image schedule and reusable images
                earliest = self.preparation_scheduler.find_earliest_starting_times(lease, nexttime)
            elif lease.state == Lease.STATE_SUSPENDED:
                # No need to transfer images from repository
                # (only intra-node transfer)
                earliest = dict([(node+1, [nexttime, constants.REQTRANSFER_NO, None]) for node in range(lease.numnodes)])

            (vmrr, in_future) = self.vm_scheduler.fit_asap(lease, nexttime, earliest, allow_reservation_in_future = canreserve)
            
            # Schedule deployment
            if lease.state != Lease.STATE_SUSPENDED:
                self.preparation_scheduler.schedule(lease, vmrr, nexttime)
            else:
                self.vm_scheduler.schedule_migration(lease, vmrr, nexttime)

            # At this point, the lease is feasible.
            # Commit changes by adding RRs to lease and to slot table
            
            # Add VMRR to lease
            lease.append_vmrr(vmrr)
            

            # Add resource reservations to slottable
            
            # TODO: deployment RRs should be added here, not in the preparation scheduler
            
            # Pre-VM RRs (if any)
            for rr in vmrr.pre_rrs:
                self.slottable.addReservation(rr)
                
            # VM
            self.slottable.addReservation(vmrr)
            
            # Post-VM RRs (if any)
            for rr in vmrr.post_rrs:
                self.slottable.addReservation(rr)
           
            if in_future:
                self.numbesteffortres += 1
                
            lease.print_contents()

        except SchedException, msg:
            raise SchedException, "The requested best-effort lease is infeasible. Reason: %s" % msg


    def __schedule_immediate_lease(self, req, nexttime):
        try:
            (vmrr, in_future) = self.__fit_asap(req, nexttime, allow_reservation_in_future=False)
            # Schedule deployment
            self.preparation_scheduler.schedule(req, vmrr, nexttime)
                        
            req.append_rr(vmrr)
            self.slottable.addReservation(vmrr)
            
            # Post-VM RRs (if any)
            for rr in vmrr.post_rrs:
                self.slottable.addReservation(rr)
                    
            req.print_contents()
        except SlotFittingException, msg:
            raise SchedException, "The requested immediate lease is infeasible. Reason: %s" % msg
        
        
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
            if susptype == constants.SUSPENSION_NONE:
                must_cancel_and_requeue = True
            else:
                can_suspend = self.vm_scheduler.can_suspend_at(lease, preemption_time)
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
            # If there are any post RRs, remove them
            for rr in vmrr.post_rrs:
                self.slottable.removeReservation(rr)
            lease.remove_vmrr(vmrr)
            self.slottable.removeReservation(vmrr)
            for vnode, pnode in lease.diskimagemap.items():
                self.resourcepool.remove_diskimage(pnode, lease.id, vnode)
            self.preparation_scheduler.cancel_deployment(lease)
            lease.diskimagemap = {}
            lease.state = Lease.STATE_QUEUED
            self.__enqueue_in_order(lease)
            get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        else:
            self.logger.info("... lease #%i will be suspended at %s." % (lease.id, preemption_time))
            self.vm_scheduler.preempt(vmrr, preemption_time)            
            
        self.logger.vdebug("Lease after preemption:")
        lease.print_contents()
        
    # TODO: Should be in VMScheduler
    def __get_utilization(self, time):
        total = self.slottable.get_total_capacity()
        util = {}
        reservations = self.slottable.getReservationsAt(time)
        for r in reservations:
            for node in r.resources_in_pnode:
                if isinstance(r, VMResourceReservation):
                    use = r.resources_in_pnode[node].get_by_type(constants.RES_CPU)
                    util[type(r)] = use + util.setdefault(type(r),0.0)
                elif isinstance(r, SuspensionResourceReservation) or isinstance(r, ResumptionResourceReservation) or isinstance(r, ShutdownResourceReservation):
                    use = r.vmrr.resources_in_pnode[node].get_by_type(constants.RES_CPU)
                    util[type(r)] = use + util.setdefault(type(r),0.0)
        util[None] = total - sum(util.values())
        for k in util:
            util[k] /= total
            
        return util        

    def __enqueue_in_order(self, lease):
        get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        self.queue.enqueue_in_order(lease)

    def _handle_end_rr(self, l, rr):
        self.slottable.removeReservation(rr)
        
    def _handle_end_lease(self, l):
        l.state = Lease.STATE_DONE
        l.duration.actual = l.duration.accumulated
        l.end = round_datetime(get_clock().get_time())
        self.completedleases.add(l)
        self.leases.remove(l)
        if isinstance(l, BestEffortLease):
            get_accounting().incr_counter(constants.COUNTER_BESTEFFORTCOMPLETED, l.id)
        

class Queue(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.__q = []
        
    def is_empty(self):
        return len(self.__q)==0
    
    def enqueue(self, r):
        self.__q.append(r)
    
    def dequeue(self):
        return self.__q.pop(0)
    
    def enqueue_in_order(self, r):
        self.__q.append(r)
        self.__q.sort(key=attrgetter("submit_time"))

    def length(self):
        return len(self.__q)
    
    def has_lease(self, lease_id):
        return (1 == len([l for l in self.__q if l.id == lease_id]))
    
    def get_lease(self, lease_id):
        return [l for l in self.__q if l.id == lease_id][0]
    
    def remove_lease(self, lease):
        self.__q.remove(lease)
    
    def __iter__(self):
        return iter(self.__q)
        
class LeaseTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.entries = {}
        
    def has_lease(self, lease_id):
        return self.entries.has_key(lease_id)
        
    def get_lease(self, lease_id):
        return self.entries[lease_id]
    
    def is_empty(self):
        return len(self.entries)==0
    
    def remove(self, lease):
        del self.entries[lease.id]
        
    def add(self, lease):
        self.entries[lease.id] = lease
        
    def get_leases(self, type=None):
        if type==None:
            return self.entries.values()
        else:
            return [e for e in self.entries.values() if isinstance(e, type)]

    def get_leases_by_state(self, state):
        return [e for e in self.entries.values() if e.state == state]
        