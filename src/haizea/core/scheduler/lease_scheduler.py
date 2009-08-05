# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
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


"""This module provides the main classes for Haizea's lease scheduler, particularly
the LeaseScheduler class. This module does *not* contain VM scheduling code (i.e.,
the code that decides what physical hosts a VM should be mapped to), which is
located in the vm_scheduler module. Lease preparation code (e.g., image transfer 
scheduling) is located in the preparation_schedulers package. In fact, the
main purpose of the lease schedule is to orchestrate these preparation and VM
schedulers.

This module also includes a Queue class and a LeaseTable class, which are used
by the lease scheduler.
"""

import haizea.common.constants as constants
from haizea.common.utils import round_datetime, get_config, get_accounting, get_clock, get_policy
from haizea.core.leases import Lease
from haizea.core.scheduler import RescheduleLeaseException, NormalEndLeaseException, InconsistentLeaseStateError, EnactmentError, UnrecoverableError, NotSchedulableException, EarliestStartingTime
from haizea.core.scheduler.slottable import ResourceReservation
from haizea.core.scheduler.vm_scheduler import VMResourceReservation
from operator import attrgetter

import logging

class LeaseScheduler(object):
    """The Haizea Lease Scheduler
    
    This is the main scheduling class in Haizea. It handles lease scheduling which,
    in turn, involves VM scheduling, preparation scheduling (such as transferring
    a VM image), and numerous bookkeeping operations. All these operations are
    handled by other classes, so this class acts mostly as an orchestrator that
    coordinates all the different operations involved in scheduling a lease.    
    """
    
    def __init__(self, vm_scheduler, preparation_scheduler, slottable):
        """Constructor
        
        The constructor does little more than create the lease scheduler's
        attributes. However, it does expect (in the arguments) a fully-constructed 
        VMScheduler, PreparationScheduler, SlotTable, and PolicyManager (these are 
        constructed in the Manager's constructor). 
        
        Arguments:
        vm_scheduler -- VM scheduler
        preparation_scheduler -- Preparation scheduler
        slottable -- Slottable
        """
        
        # Logger
        self.logger = logging.getLogger("LSCHED")
        
        # Assign schedulers and slottable
        self.vm_scheduler = vm_scheduler
        self.preparation_scheduler = preparation_scheduler
        self.slottable = slottable

        # Create other data structures
        self.queue = Queue()
        self.leases = LeaseTable()
        self.completed_leases = LeaseTable()

        # Handlers are callback functions that get called whenever a type of
        # resource reservation starts or ends. Each scheduler publishes the
        # handlers it supports through its "handlers" attributes. For example,
        # the VMScheduler provides _handle_start_vm and _handle_end_vm that
        # must be called when a VMResourceReservation start or end is encountered
        # in the slot table.
        #
        # Handlers are called from the process_reservations method of this class
        self.handlers = {}
        for (type, handler) in self.vm_scheduler.handlers.items():
            self.handlers[type] = handler

        for (type, handler) in self.preparation_scheduler.handlers.items():
            self.handlers[type] = handler


    def request_lease(self, lease):
        """Requests a leases. This is the entry point of leases into the scheduler.
        
        Request a lease. The decision on whether to accept or reject a
        lease is deferred to the policy manager (through its admission
        control policy). 
        
        If the policy determines the lease can be
        accepted, it is marked as "Pending". This still doesn't
        guarantee that the lease will be scheduled (e.g., an AR lease
        could still be rejected if the scheduler determines there are no
        resources for it; but that is a *scheduling* decision, not a admission
        control policy decision). The ultimate fate of the lease is determined
        the next time the scheduling function is called.
        
        If the policy determines the lease cannot be accepted, it is marked
        as rejected.

        Arguments:
        lease -- Lease object. Its state must be STATE_NEW.
        """
        self.logger.info("Lease #%i has been requested." % lease.id)
        if lease.submit_time == None:
            lease.submit_time = round_datetime(get_clock().get_time())
        lease.print_contents()
        lease.set_state(Lease.STATE_PENDING)
        if get_policy().accept_lease(lease):
            self.logger.info("Lease #%i has been marked as pending." % lease.id)
            self.leases.add(lease)
        else:
            self.logger.info("Lease #%i has not been accepted" % lease.id)
            lease.set_state(Lease.STATE_REJECTED)
            self.completed_leases.add(lease)

        
    def schedule(self, nexttime):
        """ The main scheduling function
        
        The scheduling function looks at all pending requests and schedules them.
        Note that most of the actual scheduling code is contained in the
        __schedule_lease method and in the VMScheduler and PreparationScheduler classes.
        
        Arguments:
        nexttime -- The next time at which the scheduler can allocate resources.
        """
        
        # Get pending leases
        pending_leases = self.leases.get_leases_by_state(Lease.STATE_PENDING)  
        ar_leases = [req for req in pending_leases if req.get_type() == Lease.ADVANCE_RESERVATION]
        im_leases = [req for req in pending_leases if req.get_type() == Lease.IMMEDIATE]
        be_leases = [req for req in pending_leases if req.get_type() == Lease.BEST_EFFORT]
        
        # Queue best-effort leases
        for lease in be_leases:
            self.__enqueue(lease)
            lease.set_state(Lease.STATE_QUEUED)
            self.logger.info("Queued best-effort lease request #%i, %i nodes for %s." % (lease.id, lease.numnodes, lease.duration.requested))

        # Schedule immediate leases
        for lease in im_leases:
            self.logger.info("Scheduling immediate lease #%i (%i nodes)" % (lease.id, lease.numnodes))
            lease.print_contents()
       
            try:
                self.__schedule_lease(lease, nexttime=nexttime)
                self.logger.info("Immediate lease #%i has been scheduled." % lease.id)
                get_accounting().incr_counter(constants.COUNTER_IMACCEPTED, lease.id)
                lease.print_contents()
            except NotSchedulableException, exc:
                get_accounting().incr_counter(constants.COUNTER_IMREJECTED, lease.id)
                self.logger.info("Immediate lease request #%i cannot be scheduled: %s" % (lease.id, exc.reason))
                lease.set_state(Lease.STATE_REJECTED)
                self.completed_leases.add(lease)
                self.leases.remove(lease)            

        # Schedule AR requests
        for lease in ar_leases:
            self.logger.info("Scheduling AR lease #%i, %i nodes from %s to %s." % (lease.id, lease.numnodes, lease.start.requested, lease.start.requested + lease.duration.requested))
            lease.print_contents()
            
            try:
                self.__schedule_lease(lease, nexttime)
                self.logger.info("AR lease #%i has been scheduled." % lease.id)
                get_accounting().incr_counter(constants.COUNTER_ARACCEPTED, lease.id)
                lease.print_contents()
            except NotSchedulableException, exc:
                get_accounting().incr_counter(constants.COUNTER_ARREJECTED, lease.id)
                self.logger.info("AR lease request #%i cannot be scheduled: %s" % (lease.id, exc.reason))
                lease.set_state(Lease.STATE_REJECTED)
                self.completed_leases.add(lease)
                self.leases.remove(lease)            
            
        # Process queue (i.e., traverse queue in search of leases that can be scheduled)
        self.__process_queue(nexttime)
        
    
    def process_starting_reservations(self, nowtime):
        """Processes starting reservations
        
        This method checks the slottable to see if there are any reservations that are
        starting at "nowtime". If so, the appropriate handler is called.

        Arguments:
        nowtime -- Time at which to check for starting reservations.
        """

        # Find starting/ending reservations
        starting = self.slottable.get_reservations_starting_at(nowtime)
        starting = [res for res in starting if res.state == ResourceReservation.STATE_SCHEDULED]
        
        # Process starting reservations
        for rr in starting:
            lease = rr.lease
            # Call the appropriate handler, and catch exceptions and errors.
            try:
                self.handlers[type(rr)].on_start(lease, rr)
                
            # An InconsistentLeaseStateError is raised when the lease is in an inconsistent
            # state. This is usually indicative of a programming error, but not necessarily
            # one that affects all leases, so we just fail this lease. Note that Haizea can also
            # be configured to stop immediately when a lease fails.
            except InconsistentLeaseStateError, exc:
                self.fail_lease(lease, exc)
            # An EnactmentError is raised when the handler had to perform an enactment action
            # (e.g., stopping a VM), and that enactment action failed. This is currently treated
            # as a non-recoverable error for the lease, and the lease is failed.
            except EnactmentError, exc:
                self.fail_lease(lease, exc)

            # Other exceptions are not expected, and generally indicate a programming error.
            # Thus, they are propagated upwards to the Manager where they will make
            # Haizea crash and burn.

    def process_ending_reservations(self, nowtime):
        """Processes ending reservations
        
        This method checks the slottable to see if there are any reservations that are
        ending at "nowtime". If so, the appropriate handler is called.

        Arguments:
        nowtime -- Time at which to check for starting/ending reservations.
        """

        # Find starting/ending reservations
        ending = self.slottable.get_reservations_ending_at(nowtime)
        ending = [res for res in ending if res.state == ResourceReservation.STATE_ACTIVE]

        # Process ending reservations
        for rr in ending:
            lease = rr.lease
            self._handle_end_rr(rr)
            
            # Call the appropriate handler, and catch exceptions and errors.
            try:
                self.handlers[type(rr)].on_end(lease, rr)
                
            # A RescheduleLeaseException indicates that the lease has to be rescheduled
            except RescheduleLeaseException, exc:
                # Currently, the only leases that get rescheduled are best-effort leases,
                # once they've been suspended.
                if rr.lease.get_type() == Lease.BEST_EFFORT:
                    if lease.get_state() == Lease.STATE_SUSPENDED_PENDING:
                        # Put back in the queue, in the same order it arrived
                        self.__enqueue_in_order(lease)
                        lease.set_state(Lease.STATE_SUSPENDED_QUEUED)
                    else:
                        raise InconsistentLeaseStateError(lease, doing = "rescheduling best-effort lease")
                    
            # A NormalEndLeaseException indicates that the end of this reservations marks
            # the normal end of the lease.
            except NormalEndLeaseException, msg:
                self._handle_end_lease(lease)
                
            # An InconsistentLeaseStateError is raised when the lease is in an inconsistent
            # state. This is usually indicative of a programming error, but not necessarily
            # one that affects all leases, so we just fail this lease. Note that Haizea can also
            # be configured to stop immediately when a lease fails.
            except InconsistentLeaseStateError, exc:
                self.fail_lease(lease, exc)
                
            # An EnactmentError is raised when the handler had to perform an enactment action
            # (e.g., stopping a VM), and that enactment action failed. This is currently treated
            # as a non-recoverable error for the lease, and the lease is failed.
            except EnactmentError, exc:
                self.fail_lease(lease, exc)
                
            # Other exceptions are not expected, and generally indicate a programming error.
            # Thus, they are propagated upwards to the Manager where they will make
            # Haizea crash and burn.

    def get_lease_by_id(self, lease_id):
        """Gets a lease with the given ID
        
        This method is useful for UIs (like the CLI) that operate on the lease ID.
        If no lease with a given ID is found, None is returned.

        Arguments:
        lease_id -- The ID of the lease
        """
        if not self.leases.has_lease(lease_id):
            return None
        else:
            return self.leases.get_lease(lease_id)

    def cancel_lease(self, lease):
        """Cancels a lease.
        
        Arguments:
        lease -- Lease to cancel
        """
        time = get_clock().get_time()
        
        self.logger.info("Cancelling lease %i..." % lease.id)
            
        lease_state = lease.get_state()
        
        if lease_state == Lease.STATE_PENDING:
            # If a lease is pending, we just need to change its state and
            # remove it from the lease table. Since this is done at the
            # end of this method, we do nothing here.
            pass

        elif lease_state == Lease.STATE_ACTIVE:
            # If a lease is active, that means we have to shut down its VMs to cancel it.
            self.logger.info("Lease %i is active. Stopping active reservation..." % lease.id)
            vmrr = lease.get_active_vmrrs(time)[0]
            self.vm_scheduler._handle_unscheduled_end_vm(lease, vmrr)

        elif lease_state in [Lease.STATE_SCHEDULED, Lease.STATE_SUSPENDED_SCHEDULED, Lease.STATE_READY, Lease.STATE_RESUMED_READY]:
            # If a lease is scheduled or ready, we just need to cancel all future reservations
            # for that lease
            self.logger.info("Lease %i is scheduled. Cancelling reservations." % lease.id)
            rrs = lease.get_scheduled_reservations()
            for r in rrs:
                self.slottable.remove_reservation(r)
            
        elif lease_state in [Lease.STATE_QUEUED, Lease.STATE_SUSPENDED_QUEUED]:
            # If a lease is in the queue, waiting to be scheduled, cancelling
            # just requires removing it from the queue
            
            self.logger.info("Lease %i is in the queue. Removing..." % lease.id)
            self.queue.remove_lease(lease)
        else:
            # Cancelling in any of the other states is currently unsupported
            raise InconsistentLeaseStateError(lease, doing = "cancelling the VM")
            
        # Change state, and remove from lease table
        lease.set_state(Lease.STATE_CANCELLED)
        self.completed_leases.add(lease)
        self.leases.remove(lease)

    
    def fail_lease(self, lease, exc=None):
        """Transitions a lease to a failed state, and does any necessary cleaning up
        
        Arguments:
        lease -- Lease to fail
        exc -- The exception that made the lease fail
        """
        treatment = get_config().get("lease-failure-handling")
        
        if treatment == constants.ONFAILURE_CANCEL:
            # In this case, a lease failure is handled by cancelling the lease,
            # but allowing Haizea to continue to run normally.
            rrs = lease.get_scheduled_reservations()
            for r in rrs:
                self.slottable.remove_reservation(r)
            lease.set_state(Lease.STATE_FAIL)
            self.completed_leases.add(lease)
            self.leases.remove(lease)
        elif treatment == constants.ONFAILURE_EXIT or treatment == constants.ONFAILURE_EXIT_RAISE:
            # In this case, a lease failure makes Haizea exit. This is useful when debugging,
            # so we can immediately know about any errors.
            raise UnrecoverableError(exc)
            
    
    def notify_event(self, lease, event):
        """Notifies an event that affects a lease.
        
        This is the entry point of asynchronous events into the scheduler. Currently,
        the only supported event is the premature end of a VM (i.e., before its
        scheduled end). Other events will emerge when we integrate Haizea with OpenNebula 1.4,
        since that version will support sending asynchronous events to Haizea.
        
        Arguments:
        lease -- Lease the event refers to
        event -- Event type
        """
        time = get_clock().get_time()
        if event == constants.EVENT_END_VM:
            vmrr = lease.get_last_vmrr()
            self._handle_end_rr(vmrr)
            # TODO: Exception handling
            self.vm_scheduler._handle_unscheduled_end_vm(lease, vmrr)
            self._handle_end_lease(lease)
            nexttime = get_clock().get_next_schedulable_time()
            # We need to reevaluate the schedule to see if there are any 
            # leases scheduled in the future that could be rescheduled
            # to start earlier
            self.reevaluate_schedule(nexttime)


    def reevaluate_schedule(self, nexttime):
        """Reevaluates the schedule.
        
        This method can be called whenever resources are freed up
        unexpectedly (e.g., a lease than ends earlier than expected))
        to check if any leases scheduled in the future could be
        rescheduled to start earlier on the freed up resources.
        
        Currently, this method only checks if best-effort leases
        scheduled in the future (using a backfilling algorithm)
        can be rescheduled
        
        Arguments:
        nexttime -- The next time at which the scheduler can allocate resources.
        """        
        future = self.vm_scheduler.get_future_reschedulable_leases()
        for l in future:
            # We can only reschedule leases in the following four states
            if l.get_state() in (Lease.STATE_PREPARING, Lease.STATE_READY, Lease.STATE_SCHEDULED, Lease.STATE_SUSPENDED_SCHEDULED):
                # For each reschedulable lease already scheduled in the
                # future, we cancel the lease's preparantion and
                # the last scheduled VM.
                vmrr = l.get_last_vmrr()
                self.preparation_scheduler.cancel_preparation(l)
                self.vm_scheduler.cancel_vm(vmrr)
                l.remove_vmrr(vmrr)
                if l.get_state() in (Lease.STATE_READY, Lease.STATE_SCHEDULED, Lease.STATE_PREPARING):
                    l.set_state(Lease.STATE_PENDING)
                elif l.get_state() == Lease.STATE_SUSPENDED_SCHEDULED:
                    l.set_state(Lease.STATE_SUSPENDED_PENDING)

                # At this point, the lease just looks like a regular
                # pending lease that can be handed off directly to the
                # __schedule_lease method.
                # TODO: We should do exception handling here. However,
                # since we can only reschedule best-effort leases that were
                # originally schedule in the future, the scheduling function 
                # should always be able to schedule the lease (worst-case 
                # scenario is that it simply replicates the previous schedule)
                self.__schedule_lease(l, nexttime)


    def is_queue_empty(self):
        """Return True is the queue is empty, False otherwise"""
        return self.queue.is_empty()

    
    def exists_scheduled_leases(self):
        """Return True if there are any leases scheduled in the future"""
        return not self.slottable.is_empty()    

            
    def __process_queue(self, nexttime):
        """ Traverses the queue in search of leases that can be scheduled.
        
        This method processes the queue in order, but takes into account that
        it may be possible to schedule leases in the future (using a 
        backfilling algorithm)
        
        Arguments:
        nexttime -- The next time at which the scheduler can allocate resources.
        """        
        
        done = False
        newqueue = Queue()
        while not done and not self.is_queue_empty():
            if not self.vm_scheduler.can_schedule_in_future() and self.slottable.is_full(nexttime, restype = constants.RES_CPU):
                self.logger.debug("Used up all future reservations and slot table is full. Skipping rest of queue.")
                done = True
            else:
                lease = self.queue.dequeue()
                try:
                    self.logger.info("Next request in the queue is lease %i. Attempting to schedule..." % lease.id)
                    lease.print_contents()
                    self.__schedule_lease(lease, nexttime)
                    get_accounting().decr_counter(constants.COUNTER_QUEUESIZE, lease.id)
                except NotSchedulableException, msg:
                    # Put back on queue
                    newqueue.enqueue(lease)
                    self.logger.info("Lease %i could not be scheduled at this time." % lease.id)
                    if get_config().get("backfilling") == constants.BACKFILLING_OFF:
                        done = True
        
        for lease in self.queue:
            newqueue.enqueue(lease)
        
        self.queue = newqueue 
    

    def __schedule_lease(self, lease, nexttime):            
        """ Schedules a lease.
        
        This method orchestrates the preparation and VM scheduler to
        schedule a lease.
        
        Arguments:
        lease -- Lease to schedule.
        nexttime -- The next time at which the scheduler can allocate resources.
        """       
                
        lease_state = lease.get_state()
        migration = get_config().get("migration")
        
        # Determine earliest start time in each node
        if lease_state == Lease.STATE_PENDING or lease_state == Lease.STATE_QUEUED:
            # This lease might require preparation. Ask the preparation
            # scheduler for the earliest starting time.
            earliest = self.preparation_scheduler.find_earliest_starting_times(lease, nexttime)
        elif lease_state == Lease.STATE_SUSPENDED_PENDING or lease_state == Lease.STATE_SUSPENDED_QUEUED:
            # This lease may have to be migrated.
            # We have to ask both the preparation scheduler and the VM
            # scheduler what would be the earliest possible starting time
            # on each node, assuming we have to transfer files between
            # nodes.

            node_ids = self.slottable.nodes.keys()
            earliest = {}
            if migration == constants.MIGRATE_NO:
                # If migration is disabled, the earliest starting time
                # is simply nexttime.
                for node in node_ids:
                    earliest[node] = EarliestStartingTime(nexttime, EarliestStartingTime.EARLIEST_NOPREPARATION)
            else:
                # Otherwise, we ask the preparation scheduler and the VM
                # scheduler how long it would take them to migrate the
                # lease state.
                prep_migr_time = self.preparation_scheduler.estimate_migration_time(lease)            
                vm_migr_time = self.vm_scheduler.estimate_migration_time(lease)
                for node in node_ids:
                    earliest[node] = EarliestStartingTime(nexttime + prep_migr_time + vm_migr_time, EarliestStartingTime.EARLIEST_MIGRATION)
        else:
            raise InconsistentLeaseStateError(lease, doing = "scheduling a best-effort lease")

        # Now, we give the lease to the VM scheduler, along with the
        # earliest possible starting times. If the VM scheduler can
        # schedule VMs for this lease, it will return a resource reservation
        # that we can add to the slot table, along with a list of
        # leases that have to be preempted.
        # If the VM scheduler can't schedule the VMs, it will throw an
        # exception (we don't catch it here, and it is just thrown up
        # to the calling method.
        (vmrr, preemptions) = self.vm_scheduler.schedule(lease, nexttime, earliest)
                                
        # If scheduling the lease involves preempting other leases,
        # go ahead and preempt them.
        if len(preemptions) > 0:
            self.logger.info("Must preempt leases %s to make room for lease #%i" % ([l.id for l in preemptions], lease.id))
            for l in preemptions:
                self.__preempt_lease(l, preemption_time=vmrr.start)
                
        # Schedule lease preparation
        is_ready = False
        preparation_rrs = []
        if lease_state in (Lease.STATE_SUSPENDED_PENDING, Lease.STATE_SUSPENDED_QUEUED) and migration != constants.MIGRATE_NO:
            # The lease might require migration
            migr_rrs = self.preparation_scheduler.schedule_migration(lease, vmrr, nexttime)
            if len(migr_rrs) > 0:
                end_migr = migr_rrs[-1].end
            else:
                end_migr = nexttime
            migr_rrs += self.vm_scheduler.schedule_migration(lease, vmrr, end_migr)
            migr_rrs.reverse()
            for migr_rr in migr_rrs:
                vmrr.pre_rrs.insert(0, migr_rr)
            if len(migr_rrs) == 0:
                is_ready = True
        elif lease_state in (Lease.STATE_SUSPENDED_PENDING, Lease.STATE_SUSPENDED_QUEUED) and migration == constants.MIGRATE_NO:
            # No migration means the lease is ready
            is_ready = True
        elif lease_state in (Lease.STATE_PENDING, Lease.STATE_QUEUED):
            # The lease might require initial preparation
            preparation_rrs, is_ready = self.preparation_scheduler.schedule(lease, vmrr, earliest)

        # At this point, the lease is feasible.
        # Commit changes by adding RRs to lease and to slot table
        
        # Add preparation RRs (if any) to lease
        for rr in preparation_rrs:
            lease.append_preparationrr(rr)
        
        # Add VMRR to lease
        lease.append_vmrr(vmrr)
        

        # Add resource reservations to slottable
        
        # Preparation RRs (if any)
        for rr in preparation_rrs:
            self.slottable.add_reservation(rr)
        
        # Pre-VM RRs (if any)
        for rr in vmrr.pre_rrs:
            self.slottable.add_reservation(rr)
            
        # VM
        self.slottable.add_reservation(vmrr)
        
        # Post-VM RRs (if any)
        for rr in vmrr.post_rrs:
            self.slottable.add_reservation(rr)
          
        # Change lease state
        if lease_state == Lease.STATE_PENDING or lease_state == Lease.STATE_QUEUED:
            lease.set_state(Lease.STATE_SCHEDULED)
            if is_ready:
                lease.set_state(Lease.STATE_READY)
        elif lease_state == Lease.STATE_SUSPENDED_PENDING or lease_state == Lease.STATE_SUSPENDED_QUEUED:
            lease.set_state(Lease.STATE_SUSPENDED_SCHEDULED)

        lease.print_contents()

        
    def __preempt_lease(self, lease, preemption_time):
        """ Preempts a lease.
        
        This method preempts a lease such that any resources allocated
        to that lease after a given time are freed up. This may require
        scheduling the lease to suspend before that time, or cancelling
        the lease altogether.
        
        Arguments:
        lease -- Lease to schedule.
        preemption_time -- Time at which lease must be preempted
        """       
        
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
            self.preparation_scheduler.cancel_preparation(lease)
            self.vm_scheduler.cancel_vm(vmrr)
            lease.remove_vmrr(vmrr)
            # TODO: Take into account other states
            if lease.get_state() == Lease.STATE_SUSPENDED_SCHEDULED:
                lease.set_state(Lease.STATE_SUSPENDED_QUEUED)
            else:
                lease.set_state(Lease.STATE_QUEUED)
            self.__enqueue_in_order(lease)
        else:
            self.logger.info("... lease #%i will be suspended at %s." % (lease.id, preemption_time))
            self.vm_scheduler.preempt_vm(vmrr, preemption_time)            
            
        self.logger.vdebug("Lease after preemption:")
        lease.print_contents()
                
  
    def __enqueue(self, lease):
        """Queues a best-effort lease request
        
        Arguments:
        lease -- Lease to be queued
        """
        get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        self.queue.enqueue(lease)


    def __enqueue_in_order(self, lease):
        """Queues a lease in order (currently, time of submission)
        
        Arguments:
        lease -- Lease to be queued
        """
        get_accounting().incr_counter(constants.COUNTER_QUEUESIZE, lease.id)
        self.queue.enqueue_in_order(lease)


    def _handle_end_rr(self, rr):
        """Performs actions that have to be done each time a reservation ends.
        
        Arguments:
        rr -- Reservation that ended
        """
        self.slottable.remove_reservation(rr)
        

    def _handle_end_lease(self, l):
        """Performs actions that have to be done each time a lease ends.
        
        Arguments:
        lease -- Lease that has ended
        """
        l.set_state(Lease.STATE_DONE)
        l.duration.actual = l.duration.accumulated
        l.end = round_datetime(get_clock().get_time())
        self.preparation_scheduler.cleanup(l)
        self.completed_leases.add(l)
        self.leases.remove(l)
        if l.get_type() == Lease.BEST_EFFORT:
            get_accounting().incr_counter(constants.COUNTER_BESTEFFORTCOMPLETED, l.id)
        

class Queue(object):
    """A simple queue for leases
    
    This class is a simple queue container for leases, with some
    extra syntactic sugar added for convenience.    
    """    

    def __init__(self):
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
    """A simple container for leases
    
    This class is a simple dictionary-like container for leases, with some
    extra syntactic sugar added for convenience.    
    """    
    
    def __init__(self):
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
            return [e for e in self.entries.values() if e.get_type() == type]

    def get_leases_by_state(self, state):
        return [e for e in self.entries.values() if e.get_state() == state]
 
        