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

"""This module provides the main classes for Haizea's VM Scheduler. All the
scheduling code that decides when and where a lease is scheduled is contained
in the VMScheduler class (except for the code that specifically decides
what physical machines each virtual machine is mapped to, which is factored out
into the "mapper" module). This module also provides the classes for the
reservations that will be placed in the slot table and correspond to VMs. 
"""

import haizea.common.constants as constants
from haizea.common.utils import round_datetime_delta, round_datetime, estimate_transfer_time, pretty_nodemap, get_config, get_clock, get_persistence, compute_suspend_resume_time
from haizea.core.leases import Lease, Capacity
from haizea.core.scheduler.slottable import ResourceReservation, ResourceTuple
from haizea.core.scheduler import ReservationEventHandler, RescheduleLeaseException, NormalEndLeaseException, EnactmentError, NotSchedulableException, InconsistentScheduleError, InconsistentLeaseStateError, MigrationResourceReservation
from operator import attrgetter, itemgetter
from mx.DateTime import TimeDelta

import logging
import operator


class VMScheduler(object):
    """The Haizea VM Scheduler
    
    This class is responsible for taking a lease and scheduling VMs to satisfy
    the requirements of that lease.
    """
    
    def __init__(self, slottable, resourcepool, mapper, max_in_future):
        """Constructor
        
        The constructor does little more than create the VM scheduler's
        attributes. However, it does expect (in the arguments) a fully-constructed 
        SlotTable, ResourcePool, and Mapper (these are constructed in the 
        Manager's constructor). 
        
        Arguments:
        slottable -- Slot table
        resourcepool -- Resource pool where enactment commands will be sent to
        mapper -- Mapper
        """        
        self.slottable = slottable
        self.resourcepool = resourcepool
        self.mapper = mapper
        self.logger = logging.getLogger("VMSCHED")
        
        # Register the handlers for the types of reservations used by
        # the VM scheduler
        self.handlers = {}
        self.handlers[VMResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_vm,
                                on_end   = VMScheduler._handle_end_vm)

        self.handlers[ShutdownResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_shutdown,
                                on_end   = VMScheduler._handle_end_shutdown)

        self.handlers[SuspensionResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_suspend,
                                on_end   = VMScheduler._handle_end_suspend)

        self.handlers[ResumptionResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_resume,
                                on_end   = VMScheduler._handle_end_resume)

        self.handlers[MemImageMigrationResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_migrate,
                                on_end   = VMScheduler._handle_end_migrate)
        
        self.max_in_future = max_in_future
        
        self.future_leases = set()


    def schedule(self, lease, duration, nexttime, earliest, override_state = None):
        """ The scheduling function
        
        This particular function doesn't do much except call __schedule_asap
        and __schedule_exact (which do all the work).
        
        Arguments:
        lease -- Lease to schedule
        nexttime -- The next time at which the scheduler can allocate resources.
        earliest -- The earliest possible starting times on each physical node
        """        
        if lease.get_type() == Lease.BEST_EFFORT:
            return self.__schedule_asap(lease, duration, nexttime, earliest, allow_in_future = self.can_schedule_in_future())
        elif lease.get_type() == Lease.ADVANCE_RESERVATION:
            return self.__schedule_exact(lease, duration, nexttime, earliest)
        elif lease.get_type() == Lease.IMMEDIATE:
            return self.__schedule_asap(lease, duration, nexttime, earliest, allow_in_future = False)
        elif lease.get_type() == Lease.DEADLINE:
            return self.__schedule_deadline(lease, duration, nexttime, earliest, override_state)


    def estimate_migration_time(self, lease):
        """ Estimates the time required to migrate a lease's VMs

        This function conservatively estimates that all the VMs are going to
        be migrated to other nodes. Since all the transfers are intra-node,
        the bottleneck is the transfer from whatever node has the most
        memory to transfer.
        
        Note that this method only estimates the time to migrate the memory
        state files for the VMs. Migrating the software environment (which may
        or may not be a disk image) is the responsibility of the preparation
        scheduler, which has it's own set of migration scheduling methods.

        Arguments:
        lease -- Lease that might be migrated
        """                
        migration = get_config().get("migration")
        if migration == constants.MIGRATE_YES:
            bandwidth = self.resourcepool.info.get_migration_bandwidth()            
            vmrr = lease.get_last_vmrr()
            #mem_in_pnode = dict([(pnode,0) for pnode in set(vmrr.nodes.values())])
            transfer_time = 0
            for pnode in vmrr.nodes.values():
                mem = vmrr.resources_in_pnode[pnode].get_by_type(constants.RES_MEM)
                transfer_time += estimate_transfer_time(mem, bandwidth)
            return transfer_time
        elif migration == constants.MIGRATE_YES_NOTRANSFER:
            return TimeDelta(seconds=0)        

    def schedule_migration(self, lease, vmrr, nexttime):
        """ Schedules migrations for a lease

        Arguments:
        lease -- Lease being migrated
        vmrr -- The VM reservation before which the migration will take place
        nexttime -- The next time at which the scheduler can allocate resources.
        """
        
        # Determine what migrations have to be done. We do this by looking at
        # the mapping in the previous VM RR and in the new VM RR
        last_vmrr = lease.get_last_vmrr()
        vnode_migrations = dict([(vnode, (last_vmrr.nodes[vnode], vmrr.nodes[vnode])) for vnode in vmrr.nodes])
        
        # Determine if we actually have to migrate
        mustmigrate = False
        for vnode in vnode_migrations:
            if vnode_migrations[vnode][0] != vnode_migrations[vnode][1]:
                mustmigrate = True
                break
            
        if not mustmigrate:
            return []

        # If Haizea is configured to migrate without doing any transfers,
        # then we just return a nil-duration migration RR
        if get_config().get("migration") == constants.MIGRATE_YES_NOTRANSFER:
            start = nexttime
            end = nexttime
            res = {}
            migr_rr = MemImageMigrationResourceReservation(lease, start, end, res, vmrr, vnode_migrations)
            migr_rr.state = ResourceReservation.STATE_SCHEDULED
            return [migr_rr]

        # Figure out what migrations can be done simultaneously
        migrations = []
        while len(vnode_migrations) > 0:
            pnodes = set()
            migration = {}
            for vnode in vnode_migrations:
                origin = vnode_migrations[vnode][0]
                dest = vnode_migrations[vnode][1]
                if not origin in pnodes and not dest in pnodes:
                    migration[vnode] = vnode_migrations[vnode]
                    pnodes.add(origin)
                    pnodes.add(dest)
            for vnode in migration:
                del vnode_migrations[vnode]
            migrations.append(migration)
        
        # Create migration RRs
        start = max(last_vmrr.post_rrs[-1].end, nexttime)
        bandwidth = self.resourcepool.info.get_migration_bandwidth()
        migr_rrs = []
        for m in migrations:
            vnodes_to_migrate = m.keys()
            max_mem_to_migrate = max([lease.requested_resources[vnode].get_quantity(constants.RES_MEM) for vnode in vnodes_to_migrate])
            migr_time = estimate_transfer_time(max_mem_to_migrate, bandwidth)
            end = start + migr_time
            res = {}
            for (origin,dest) in m.values():
                resorigin = Capacity([constants.RES_NETOUT])
                resorigin.set_quantity(constants.RES_NETOUT, bandwidth)
                resdest = Capacity([constants.RES_NETIN])
                resdest.set_quantity(constants.RES_NETIN, bandwidth)
                res[origin] = self.slottable.create_resource_tuple_from_capacity(resorigin)
                res[dest] = self.slottable.create_resource_tuple_from_capacity(resdest)                
            migr_rr = MemImageMigrationResourceReservation(lease, start, start + migr_time, res, vmrr, m)
            migr_rr.state = ResourceReservation.STATE_SCHEDULED
            migr_rrs.append(migr_rr)
            start = end
            
        return migr_rrs

    def cancel_vm(self, vmrr):
        """ Cancels a VM resource reservation

        Arguments:
        vmrr -- VM RR to be cancelled
        """         
        
        # If this VM RR is part of a lease that was scheduled in the future,
        # remove that lease from the set of future leases.
        if vmrr.lease in self.future_leases:
            self.future_leases.remove(vmrr.lease)
            get_persistence().persist_future_leases(self.future_leases)

        # If there are any pre-RRs that are scheduled, remove them
        for rr in vmrr.pre_rrs:
            if rr.state == ResourceReservation.STATE_SCHEDULED:
                self.slottable.remove_reservation(rr)

        # If there are any post RRs, remove them
        for rr in vmrr.post_rrs:
            self.slottable.remove_reservation(rr)
        
        # Remove the reservation itself
        self.slottable.remove_reservation(vmrr)


    def can_suspend_at(self, lease, t, nexttime=None):
        """ Determines if it is possible to suspend a lease before a given time

        Arguments:
        vmrr -- VM RR to be preempted
        t -- Time by which the VM must be preempted
        """
        vmrr = lease.get_vmrr_at(t)
        if t < vmrr.start:
            return False # t could be during a resume
        by_t = min(vmrr.end, t) # t could be during a suspend
        if nexttime == None:
            time_until_suspend = t - vmrr.start
        else:
            time_until_suspend = min( (t - vmrr.start, t - nexttime))
        min_duration = self.__compute_scheduling_threshold(lease)
        can_suspend = time_until_suspend >= min_duration       
        return can_suspend
 
    
    def preempt_vm(self, vmrr, t):
        """ Preempts a VM reservation at a given time

        This method assumes that the lease is, in fact, preemptable,
        that the VMs are running at the given time, and that there is 
        enough time to suspend the VMs before the given time (all these
        checks are done in the lease scheduler).
        
        Arguments:
        vmrr -- VM RR to be preempted
        t -- Time by which the VM must be preempted
        """             
        
        # Save original start and end time of the vmrr
        old_start = vmrr.start
        old_end = vmrr.end
        
        # Schedule the VM suspension
        self.__schedule_suspension(vmrr, t)
        
        # Update the VMRR in the slot table
        self.slottable.update_reservation(vmrr, old_start, old_end)
        
        # Add the suspension RRs to the VM's post-RRs
        for susprr in vmrr.post_rrs:
            self.slottable.add_reservation(susprr)
            
            
    def get_future_reschedulable_leases(self):
        """ Returns a list of future leases that are reschedulable.

        Currently, this list is just the best-effort leases scheduled
        in the future as determined by the backfilling algorithm.
        Advance reservation leases, by their nature, cannot be 
        rescheduled to find a "better" starting time.
        """             
        return list(self.future_leases)
    

    def can_schedule_in_future(self):
        """ Returns True if the backfilling algorithm would allow a lease
        to be scheduled in the future.

        """             
        if self.max_in_future == -1: # Unlimited
            return True
        else:
            return len(self.future_leases) < self.max_in_future

        
    def get_utilization(self, time):
        """ Computes resource utilization (currently just CPU-based)
        
        This utilization information shows what 
        portion of the physical resources is used by each type of reservation 
        (e.g., 70% are running a VM, 5% are doing suspensions, etc.)

        Arguments:
        time -- Time at which to determine utilization
        """         
        total = self.slottable.get_total_capacity(restype = constants.RES_CPU)
        util = {}
        reservations = self.slottable.get_reservations_at(time)
        for r in reservations:
            for node in r.resources_in_pnode:
                if isinstance(r, VMResourceReservation):
                    use = r.resources_in_pnode[node].get_by_type(constants.RES_CPU)
                    util[type(r)] = use + util.setdefault(type(r),0.0)
                elif isinstance(r, SuspensionResourceReservation) or isinstance(r, ResumptionResourceReservation) or isinstance(r, ShutdownResourceReservation):
                    use = r.vmrr.resources_in_pnode[node].get_by_type(constants.RES_CPU)
                    util[type(r)] = 0.0
                    #util[type(r)] = use + util.setdefault(type(r),0.0)
        util[None] = total - sum(util.values())

        if total != 0:
            for k in util:
                util[k] /= total

        return util
        

    def __schedule_exact(self, lease, duration, nexttime, earliest):
        """ Schedules VMs that must start at an exact time
        
        This type of lease is "easy" to schedule because we know the exact
        start time, which means that's the only starting time we have to
        check. So, this method does little more than call the mapper.
        
        Arguments:
        lease -- Lease to schedule
        nexttime -- The next time at which the scheduler can allocate resources.
        earliest -- The earliest possible starting times on each physical node
        """             

        # Determine the start and end time
        shutdown_time = lease.estimate_shutdown_time()
        start = lease.start.requested
        end = start + lease.duration.requested + shutdown_time
        
        # Convert Capacity objects in lease object into ResourceTuples that
        # we can hand over to the mapper.
        requested_resources = dict([(k,self.slottable.create_resource_tuple_from_capacity(v)) for k,v in lease.requested_resources.items()])

        # Let the mapper do its magiv
        mapping, actualend, preemptions = self.mapper.map(lease, 
                                                          requested_resources,
                                                          start, 
                                                          end, 
                                                          strictend = True,
                                                          allow_preemption = True)

        # If no mapping was found, tell the lease scheduler about it
        if mapping == None:
            raise NotSchedulableException, "Not enough resources in specified interval"
        
        # Create VM resource reservations
        res = {}
        
        for (vnode,pnode) in mapping.items():
            vnode_res = requested_resources[vnode]
            if res.has_key(pnode):
                res[pnode].incr(vnode_res)
            else:
                res[pnode] = ResourceTuple.copy(vnode_res)
        
        vmrr = VMResourceReservation(lease, start, end, mapping, res)
        vmrr.state = ResourceReservation.STATE_SCHEDULED

        # Schedule shutdown for the VM
        self.__schedule_shutdown(vmrr)

        return vmrr, preemptions


    def __schedule_asap(self, lease, duration, nexttime, earliest, allow_in_future = None, override_state = None):
        """ Schedules VMs as soon as possible
        
        This method is a bit more complex that __schedule_exact because
        we need to figure out what "as soon as possible" actually is.
        This involves attempting several mappings, at different points
        in time, before we can schedule the lease.
        
        This method will always check, at least, if the lease can be scheduled
        at the earliest possible moment at which the lease could be prepared
        (e.g., if the lease can't start until 1 hour in the future because that's
        the earliest possible time at which the disk images it requires can
        be transferred, then that's when the scheduler will check). Note, however,
        that this "earliest possible moment" is determined by the preparation
        scheduler.
        
        Additionally, if the lease can't be scheduled at the earliest
        possible moment, it can also check if the lease can be scheduled
        in the future. This partially implements a backfilling algorithm
        (the maximum number of future leases is stored in the max_in_future
        attribute of VMScheduler), the other part being implemented in the
        __process_queue method of LeaseScheduler.
        
        Note that, if the method is allowed to scheduled in the future,
        and assuming that the lease doesn't request more resources than
        the site itself, this method will always schedule the VMs succesfully
        (since there's always an empty spot somewhere in the future).
        
        
        Arguments:
        lease -- Lease to schedule
        nexttime -- The next time at which the scheduler can allocate resources.
        earliest -- The earliest possible starting times on each physical node
        allow_in_future -- Boolean indicating whether the scheduler is
        allowed to schedule the VMs in the future.
        """                
        
        #
        # STEP 1: PROLEGOMENA
        #
        
        lease_id = lease.id
        remaining_duration = duration
        shutdown_time = lease.estimate_shutdown_time()
        
        if override_state != None:
            state = override_state
        else:
            state = lease.get_state()
        
        # We might be scheduling a suspended lease. If so, we will
        # also have to schedule its resumption. Right now, just 
        # figure out if this is such a lease.
        mustresume = (state in (Lease.STATE_SUSPENDED_PENDING, Lease.STATE_SUSPENDED_QUEUED, Lease.STATE_SUSPENDED_SCHEDULED))

        # This is the minimum duration that we must be able to schedule.
        # See __compute_scheduling_threshold for more details.
        min_duration = self.__compute_scheduling_threshold(lease)
        

        #
        # STEP 2: FIND THE CHANGEPOINTS
        #

        # Find the changepoints, and the available nodes at each changepoint
        # We need to do this because the preparation scheduler may have
        # determined that some nodes might require more time to prepare
        # than others (e.g., if using disk image caching, some nodes
        # might have the required disk image predeployed, while others
        # may require transferring the image to that node).
        # 
        # The end result of this step is a list (cps) where each entry
        # is a (t,nodes) pair, where "t" is the time of the changepoint
        # and "nodes" is the set of nodes that are available at that time.
        
        if not mustresume:
            # If this is not a suspended lease, then the changepoints
            # are determined based on the "earliest" parameter.
            cps = [(node, e.time) for node, e in earliest.items()]
            cps.sort(key=itemgetter(1))
            curcp = None
            changepoints = []
            nodes = []
            for node, time in cps:
                nodes.append(node)
                if time != curcp:
                    changepoints.append([time, set(nodes)])
                    curcp = time
                else:
                    changepoints[-1][1] = set(nodes)
        else:
            # If the lease is suspended, we take into account that, if
            # migration is disabled, we can only schedule the lease
            # on the nodes it is currently scheduled on.
            if get_config().get("migration") == constants.MIGRATE_NO:
                vmrr = lease.get_last_vmrr()
                onlynodes = set(vmrr.nodes.values())
            else:
                onlynodes = None               
            changepoints = list(set([x.time for x in earliest.values()]))
            changepoints.sort()
            changepoints = [(x, onlynodes) for x in changepoints]

        # If we can schedule VMs in the future,
        # we also consider future changepoints
        if allow_in_future:
            res = self.slottable.get_reservations_ending_after(changepoints[-1][0])
            # We really only care about changepoints where VMs end (which is
            # when resources become available)
            futurecp = [r.get_final_end() for r in res if isinstance(r, VMResourceReservation)]
            # Corner case: Sometimes we're right in the middle of a ShutdownReservation, so it won't be
            # included in futurecp.
            futurecp += [r.end for r in res if isinstance(r, ShutdownResourceReservation) and not r.vmrr in res]
            if not mustresume:
                futurecp = [(p,None) for p in futurecp]
            else:
                futurecp = [(p,onlynodes) for p in futurecp]                
        else:
            futurecp = []
            
        futurecp.sort()

        if lease.deadline != None:
            changepoints = [cp for cp in changepoints if cp[0] <= lease.deadline - duration]
            futurecp = [cp for cp in futurecp if cp[0] <= lease.deadline - duration]

        #
        # STEP 3: FIND A MAPPING
        #
        
        # In this step we find a starting time and a mapping for the VMs,
        # which involves going through the changepoints in order and seeing
        # if we can find a mapping.
        # Most of the work is done in the __find_fit_at_points
        
        # If resuming, we also have to allocate enough time for the resumption
        if mustresume:
            duration = remaining_duration + lease.estimate_resume_time()
        else:
            duration = remaining_duration

        duration += shutdown_time

        in_future = False

        # Convert Capacity objects in lease object into ResourceTuples that
        # we can hand over to the mapper.
        requested_resources = dict([(k,self.slottable.create_resource_tuple_from_capacity(v)) for k,v in lease.requested_resources.items()])

        # First, try to find a mapping assuming we can't schedule in the future
        start, end, mapping, preemptions = self.__find_fit_at_points(lease,
                                                                     requested_resources,
                                                                     changepoints, 
                                                                     duration, 
                                                                     min_duration)
        
        if start == None and not allow_in_future:
            # We did not find a suitable starting time. This can happen
            # if we're unable to schedule in the future
            raise NotSchedulableException, "Could not find enough resources for this request"

        # If we haven't been able to fit the lease, check if we can
        # reserve it in the future
        if start == None and allow_in_future:
            start, end, mapping, preemptions = self.__find_fit_at_points(lease,
                                                                         requested_resources,
                                                                         futurecp, 
                                                                         duration, 
                                                                         min_duration
                                                                         )
            # TODO: The following will also raise an exception if a lease
            # makes a request that could *never* be satisfied with the
            # current resources.
            if start == None:
                if lease.deadline != None:
                    raise NotSchedulableException, "Could not find enough resources for this request before deadline"
                else:
                    raise InconsistentScheduleError, "Could not find a mapping in the future (this should not happen)"

            in_future = True

        #
        # STEP 4: CREATE RESERVATIONS
        #
        
        # At this point, the lease is feasible. We just need to create
        # the reservations for the VMs and, possibly, for the VM resumption,
        # suspension, and shutdown.    
        
        # VM resource reservation
        res = {}
        
        for (vnode,pnode) in mapping.items():
            vnode_res = requested_resources[vnode]
            if res.has_key(pnode):
                res[pnode].incr(vnode_res)
            else:
                res[pnode] = ResourceTuple.copy(vnode_res)

        vmrr = VMResourceReservation(lease, start, end, mapping, res)
        vmrr.state = ResourceReservation.STATE_SCHEDULED

        # VM resumption resource reservation
        if mustresume:
            self.__schedule_resumption(vmrr, start)

        # If the mapper couldn't find a mapping for the full duration
        # of the lease, then we need to schedule a suspension.
        mustsuspend = (vmrr.end - vmrr.start) - shutdown_time < remaining_duration
        if mustsuspend:
            self.__schedule_suspension(vmrr, end)
        else:
            # Compensate for any overestimation
            if (vmrr.end - vmrr.start) > remaining_duration + shutdown_time:
                vmrr.end = vmrr.start + remaining_duration + shutdown_time
            self.__schedule_shutdown(vmrr)
        
        if in_future:
            self.future_leases.add(lease)
            get_persistence().persist_future_leases(self.future_leases)

        susp_str = res_str = ""
        if mustresume:
            res_str = " (resuming)"
        if mustsuspend:
            susp_str = " (suspending)"
        self.logger.info("Lease #%i can be scheduled on nodes %s from %s%s to %s%s" % (lease.id, mapping.values(), start, res_str, end, susp_str))

        return vmrr, preemptions


    def __schedule_deadline(self, lease, duration, nexttime, earliest, override_state):   
        
        earliest_time = nexttime
        for n in earliest:
            earliest[n].time = max(lease.start.requested, earliest[n].time)
            earliest_time = max(earliest_time, earliest[n].time)

        slack = (lease.deadline - lease.start.requested) / lease.duration.requested
        if slack <= 2.0:
            try:
                self.logger.debug("Trying to schedule lease #%i as an advance reservation..." % lease.id)
                vmrr, preemptions = self.__schedule_exact(lease, duration, nexttime, earliest)
                # Don't return preemptions. They have already been preempted by the deadline mapper
                return vmrr, []
            except NotSchedulableException:
                self.logger.debug("Lease #%i cannot be scheduled as an advance reservation, trying as best-effort..." % lease.id)
                try:
                    vmrr, preemptions = self.__schedule_asap(lease, duration, nexttime, earliest, allow_in_future = True, override_state=override_state)
                except NotSchedulableException:
                    vmrr = None
                    preemptions = []
                if vmrr == None or vmrr.end - vmrr.start != duration or vmrr.end > lease.deadline or len(preemptions)>0:
                    self.logger.debug("Lease #%i cannot be scheduled before deadline using best-effort." % lease.id)
                    #raise NotSchedulableException, "Could not schedule before deadline without making other leases miss deadline"
                else:
                    return vmrr, preemptions
        else:
            self.logger.debug("Trying to schedule lease #%i as best-effort..." % lease.id)
            try:
                vmrr, preemptions = self.__schedule_asap(lease, duration, nexttime, earliest, allow_in_future = True, override_state=override_state)
            except NotSchedulableException:
                vmrr = None
                preemptions = []

        if vmrr == None or vmrr.end - vmrr.start != duration or vmrr.end > lease.deadline or len(preemptions)>0:
            self.logger.debug("Trying to schedule lease #%i by rescheduling other leases..." % lease.id)
            dirtynodes = set()
            dirtytime = earliest_time

            future_vmrrs = self.slottable.get_reservations_on_or_after(earliest_time)
            future_vmrrs.sort(key=operator.attrgetter("start"))
            future_vmrrs = [rr for rr in future_vmrrs 
                            if isinstance(rr, VMResourceReservation) 
                            and rr.state == ResourceReservation.STATE_SCHEDULED
                            and reduce(operator.and_, [(prerr.state == ResourceReservation.STATE_SCHEDULED) for prerr in rr.pre_rrs], True)]

            leases = list(set([future_vmrr.lease for future_vmrr in future_vmrrs]))

            self.slottable.push_state(leases)
            
            for future_vmrr in future_vmrrs:
                #print "REMOVE", future_vmrr.lease.id, future_vmrr.start, future_vmrr.end
                future_vmrr.lease.remove_vmrr(future_vmrr)
                self.cancel_vm(future_vmrr)
            
            orig_vmrrs = dict([(l,[rr for rr in future_vmrrs if rr.lease == l]) for l in leases])
            
            leases.append(lease)
            leases.sort(key= lambda l: (l.deadline - earliest_time) / l.get_remaining_duration_at(nexttime))

            new_vmrrs = {}

            self.logger.debug("Attempting to reschedule leases %s" % [l.id for l in leases])

            # First pass
            scheduled = set()
            for lease2 in leases:
                
                last_vmrr = lease2.get_last_vmrr()
                if last_vmrr != None and last_vmrr.is_suspending():
                    override_state = Lease.STATE_SUSPENDED_PENDING
                    l_earliest_time = max(last_vmrr.post_rrs[-1].end, earliest_time)
                else:
                    override_state = None
                    l_earliest_time = earliest_time
                    
                for n in earliest:
                    earliest[n].time = max(lease2.start.requested, l_earliest_time)
                    
                self.logger.debug("Rescheduling lease %s" % lease2.id)
                dur = lease2.get_remaining_duration_at(l_earliest_time)                

                try:
                    vmrr, preemptions = self.__schedule_asap(lease2, dur, nexttime, earliest, allow_in_future = True, override_state=override_state)
                except NotSchedulableException:
                    vmrr = None
                    preemptions = []
                if vmrr == None or vmrr.end - vmrr.start != dur or vmrr.end > lease2.deadline or len(preemptions) != 0:
                    self.logger.debug("Lease %s could not be rescheduled, undoing changes." % lease2.id)
                    self.slottable.pop_state()

                    raise NotSchedulableException, "Could not schedule before deadline without making other leases miss deadline"
                    
                dirtytime = max(vmrr.end, dirtytime)
                dirtynodes.update(vmrr.resources_in_pnode.keys())
                    
                for rr in vmrr.pre_rrs:
                    self.slottable.add_reservation(rr)                
                self.slottable.add_reservation(vmrr)
                for rr in vmrr.post_rrs:
                    self.slottable.add_reservation(rr)                    
                scheduled.add(lease2)
                if lease2 == lease:
                    return_vmrr = vmrr
                    break
                else:
                    new_vmrrs[lease2] = vmrr
            
            # We've scheduled the lease. Now we try to schedule the rest of the leases but,
            # since now we know the nodes the new lease is in, we can do a few optimizations
            # Restore the leases in nodes we haven't used, and that would not be
            # affected by the new lease. We need to find what this set of nodes is.
            
            to_schedule = [l for l in leases if l not in scheduled]
            dirtynodes, cleanleases = self.find_dirty_nodes(to_schedule, dirtynodes, orig_vmrrs)
            self.logger.debug("Rescheduling only leases on nodes %s" % dirtynodes)
            self.logger.debug("Leases %s can be skipped" % [l.id for l in cleanleases])
            
            # Restore the leases
            restored_leases = set()
            for l in leases:
                if l in cleanleases:
                    # Restore
                    for l_vmrr in orig_vmrrs[l]:
                        for rr in l_vmrr.pre_rrs:
                            self.slottable.add_reservation(rr)                
                        self.slottable.add_reservation(l_vmrr)
                        for rr in l_vmrr.post_rrs:
                            self.slottable.add_reservation(rr)   
                        l.append_vmrr(l_vmrr)
                        scheduled.add(l)
                        restored_leases.add(l)
                            
            to_schedule = [l for l in leases if l not in scheduled]
            try:
                (more_scheduled, add_vmrrs, dirtytime) = self.reschedule_deadline_leases(to_schedule, orig_vmrrs, earliest_time, earliest, nexttime, dirtytime)
                scheduled.update(more_scheduled)
                new_vmrrs.update(add_vmrrs)
            except NotSchedulableException:
                self.logger.debug("Lease %s could not be rescheduled, undoing changes." % l.id)
                self.slottable.pop_state()
                raise
                                
            self.slottable.pop_state(discard=True)

            for l in leases:
                if l not in scheduled:
                    for l_vmrr in orig_vmrrs[l]:
                        for rr in l_vmrr.pre_rrs:
                            self.slottable.add_reservation(rr)                
                        self.slottable.add_reservation(l_vmrr)
                        for rr in l_vmrr.post_rrs:
                            self.slottable.add_reservation(rr)
                        l.append_vmrr(l_vmrr)
                        restored_leases.add(l)
    
            for lease2, vmrr in new_vmrrs.items():
                lease2.append_vmrr(vmrr)
                    
            # Remove from slottable, because lease_scheduler is the one that actually
            # adds the RRs
            for rr in return_vmrr.pre_rrs:
                self.slottable.remove_reservation(rr)                
            self.slottable.remove_reservation(return_vmrr)
            for rr in return_vmrr.post_rrs:
                self.slottable.remove_reservation(rr)             
            
            for l in leases:
                if l in scheduled:
                    self.logger.vdebug("Lease %i after rescheduling:" % l.id)
                    l.print_contents()                   

            return return_vmrr, []
        else:
            return vmrr, preemptions


    def find_dirty_nodes(self, to_schedule, dirtynodes, orig_vmrrs):
        dirtynodes = set(dirtynodes)
        done = False
        while not done:
            stable = True
            cleanleases = set()            
            for l in to_schedule:
                pnodes = set()
                for l_vmrr in orig_vmrrs[l]:
                    pnodes.update(l_vmrr.resources_in_pnode.keys())
                in_dirty = dirtynodes & pnodes
                in_clean = pnodes - dirtynodes
                if len(in_dirty) > 0 and len(in_clean) > 0:
                    stable = False
                    dirtynodes.update(in_clean)
                if len(in_clean) > 0 and len(in_dirty) == 0:
                    cleanleases.add(l)
            if stable == True:
                done = True
                
        return dirtynodes, cleanleases


    def reschedule_deadline_leases(self, leases, orig_vmrrs, earliest_time, earliest, nexttime, dirtytime):
        scheduled = set()
        new_vmrrs = {}
        for l in leases:
            if len(scheduled) < len(leases) and dirtytime != None:
                min_future_start = min([min([rr.start for rr in lrr]) for l2, lrr in orig_vmrrs.items() if l2 in leases and l2 not in scheduled])
                if min_future_start > dirtytime:
                    break
                
            last_vmrr = l.get_last_vmrr()
            if last_vmrr != None and last_vmrr.is_suspending():
                override_state = Lease.STATE_SUSPENDED_PENDING
                l_earliest_time = max(last_vmrr.post_rrs[-1].end, earliest_time)
            else:
                override_state = None
                l_earliest_time = earliest_time
                
            for n in earliest:
                earliest[n].time = max(l.start.requested, l_earliest_time)
                
            self.logger.debug("Rescheduling lease %s" % l.id)
            dur = l.get_remaining_duration_at(l_earliest_time)
            
            try:
                vmrr, preemptions = self.__schedule_asap(l, dur, nexttime, earliest, allow_in_future = True, override_state=override_state)
            except NotSchedulableException:
                vmrr = None
                preemptions = []
            
            if vmrr == None or vmrr.end - vmrr.start != dur or vmrr.end > l.deadline or len(preemptions) != 0:
                raise NotSchedulableException, "Could not schedule before deadline without making other leases miss deadline"
            
            if dirtytime != None:
                dirtytime = max(vmrr.end, dirtytime)
            
            for rr in vmrr.pre_rrs:
                self.slottable.add_reservation(rr)                
            self.slottable.add_reservation(vmrr)
            for rr in vmrr.post_rrs:
                self.slottable.add_reservation(rr)                    
            new_vmrrs[l] = vmrr
            scheduled.add(l)                    
                
        return scheduled, new_vmrrs, dirtytime
        

    def reschedule_deadline(self, lease, duration, nexttime, earliest, override_state = None):
        for n in earliest:
            earliest[n].time = max(lease.start.requested, earliest[n].time)
        
        try:
            vmrr, preemptions = self.__schedule_asap(lease, duration, nexttime, earliest, allow_in_future = True, override_state=override_state)
        except NotSchedulableException:
            vmrr = None
            preemptions = []
            
        if vmrr == None or vmrr.end - vmrr.start != duration or vmrr.end > lease.deadline or len(preemptions)>0:
            self.logger.debug("Lease #%i cannot be scheduled before deadline using best-effort." % lease.id)
            raise NotSchedulableException, "Could not schedule before deadline without making other leases miss deadline"
        else:
            return vmrr, preemptions
        
        

    def __find_fit_at_points(self, lease, requested_resources, changepoints, duration, min_duration):
        """ Tries to map a lease in a given list of points in time
        
        This method goes through a given list of points in time and tries
        to find the earliest time at which that lease can be allocated
        resources.
        
        Arguments:
        lease -- Lease to schedule
        requested_resources -- A dictionary of lease node -> ResourceTuple.
        changepoints -- The list of changepoints
        duration -- The amount of time requested
        min_duration -- The minimum amount of time that should be allocated
        
        Returns:
        start -- The time at which resources have been found for the lease
        actualend -- The time at which the resources won't be available. Note
        that this is not necessarily (start + duration) since the mapper
        might be unable to find enough resources for the full requested duration.
        mapping -- A mapping of lease nodes to physical nodes
        preemptions -- A list of 
        (if no mapping is found, all these values are set to None)
        """                 
        found = False
        
        for time, onlynodes in changepoints:
            start = time
            end = start + duration
            self.logger.debug("Attempting to map from %s to %s" % (start, end))
            
            # If suspension is disabled, we will only accept mappings that go
            # from "start" strictly until "end".
            susptype = get_config().get("suspension")
            # TODO: Remove the "if is deadline lease" condition and replace it
            # with something cleaner
            if susptype == constants.SUSPENSION_NONE or (lease.numnodes > 1 and susptype == constants.SUSPENSION_SERIAL) or lease.get_type() == Lease.DEADLINE: 
                strictend = True
            else:
                strictend = False

            # Let the mapper work its magic
            mapping, actualend, preemptions = self.mapper.map(lease, 
                                                              requested_resources,
                                                              start, 
                                                              end, 
                                                              strictend = strictend,
                                                              onlynodes = onlynodes,
                                                              allow_preemption = False)

            # We have a mapping; we still have to check if it satisfies
            # the minimum duration.
            if mapping != None:
                if actualend < end:
                    actualduration = actualend - start
                    if actualduration >= min_duration:
                        self.logger.debug("This lease can be scheduled from %s to %s (will require suspension)" % (start, actualend))
                        found = True
                        break
                    else:
                        self.logger.debug("This starting time does not allow for the requested minimum duration (%s < %s)" % (actualduration, min_duration))
                else:
                    self.logger.debug("This lease can be scheduled from %s to %s (full duration)" % (start, end))
                    found = True
                    break
        
        if found:
            return start, actualend, mapping, preemptions
        else:
            return None, None, None, None
    
    
    def __compute_susprem_times(self, vmrr, time, direction, exclusion, rate, override = None):
        """ Computes the times at which suspend/resume operations would have to start
        
        When suspending or resuming a VM, the VM's memory is dumped to a
        file on disk. To correctly estimate the time required to suspend
        a lease with multiple VMs, Haizea makes sure that no two 
        suspensions/resumptions happen at the same time (e.g., if eight
        memory files were being saved at the same time to disk, the disk's
        performance would be reduced in a way that is not as easy to estimate
        as if only one file were being saved at a time). Based on a number
        of parameters, this method estimates the times at which the 
        suspend/resume commands would have to be sent to guarantee this
        exclusion.
                    
        Arguments:
        vmrr -- The VM reservation that will be suspended/resumed
        time -- The time at which the suspend should end or the resume should start.
        direction -- DIRECTION_BACKWARD: start at "time" and compute the times going
        backward (for suspensions) DIRECTION_FORWARD: start at time "time" and compute
        the times going forward.
        exclusion -- SUSPRES_EXCLUSION_GLOBAL (memory is saved to global filesystem)
        or SUSPRES_EXCLUSION_LOCAL (saved to local filesystem)
        rate -- The rate at which an individual VM is suspended/resumed
        override -- If specified, then instead of computing the time to 
        suspend/resume VM based on its memory and the "rate" parameter,
        use this override value.
        
        """         
        times = [] # (start, end, {pnode -> vnodes})
        enactment_overhead = get_config().get("enactment-overhead") 

        if override != None:
            override = TimeDelta(seconds=override)

        if exclusion == constants.SUSPRES_EXCLUSION_GLOBAL:
            # Global exclusion (which represents, e.g., reading/writing the memory image files
            # from a global file system) meaning no two suspensions/resumptions can happen at 
            # the same time in the entire resource pool.
            
            t = time
            t_prev = None
                
            for (vnode,pnode) in vmrr.nodes.items():
                if override == None:
                    mem = vmrr.lease.requested_resources[vnode].get_quantity(constants.RES_MEM)
                    op_time = compute_suspend_resume_time(mem, rate)
                else:
                    op_time = override

                op_time += enactment_overhead
                    
                t_prev = t
                
                if direction == constants.DIRECTION_FORWARD:
                    t += op_time
                    times.append((t_prev, t, {pnode:[vnode]}))
                elif direction == constants.DIRECTION_BACKWARD:
                    t -= op_time
                    times.append((t, t_prev, {pnode:[vnode]}))

        elif exclusion == constants.SUSPRES_EXCLUSION_LOCAL:
            # Local exclusion (which represents, e.g., reading the memory image files
            # from a local file system) means no two resumptions can happen at the same
            # time in the same physical node.
            pervnode_times = [] # (start, end, vnode)
            vnodes_in_pnode = {}
            for (vnode,pnode) in vmrr.nodes.items():
                vnodes_in_pnode.setdefault(pnode, []).append(vnode)
            for pnode in vnodes_in_pnode:
                t = time
                t_prev = None
                for vnode in vnodes_in_pnode[pnode]:
                    if override == None:
                        mem = vmrr.lease.requested_resources[vnode].get_quantity(constants.RES_MEM)
                        op_time = compute_suspend_resume_time(mem, rate)
                    else:
                        op_time = override                    
                    
                    t_prev = t
                    
                    if direction == constants.DIRECTION_FORWARD:
                        t += op_time
                        pervnode_times.append((t_prev, t, vnode))
                    elif direction == constants.DIRECTION_BACKWARD:
                        t -= op_time
                        pervnode_times.append((t, t_prev, vnode))
            
            # Consolidate suspend/resume operations happening at the same time
            uniq_times = set([(start, end) for (start, end, vnode) in pervnode_times])
            for (start, end) in uniq_times:
                vnodes = [x[2] for x in pervnode_times if x[0] == start and x[1] == end]
                node_mappings = {}
                for vnode in vnodes:
                    pnode = vmrr.nodes[vnode]
                    node_mappings.setdefault(pnode, []).append(vnode)
                times.append([start,end,node_mappings])
        
            # Add the enactment overhead
            for t in times:
                num_vnodes = sum([len(vnodes) for vnodes in t[2].values()])
                overhead = TimeDelta(seconds = num_vnodes * enactment_overhead)
                if direction == constants.DIRECTION_FORWARD:
                    t[1] += overhead
                elif direction == constants.DIRECTION_BACKWARD:
                    t[0] -= overhead
                    
            # Fix overlaps
            if direction == constants.DIRECTION_FORWARD:
                times.sort(key=itemgetter(0))
            elif direction == constants.DIRECTION_BACKWARD:
                times.sort(key=itemgetter(1))
                times.reverse()
                
            prev_start = None
            prev_end = None
            for t in times:
                if prev_start != None:
                    start = t[0]
                    end = t[1]
                    if direction == constants.DIRECTION_FORWARD:
                        if start < prev_end:
                            diff = prev_end - start
                            t[0] += diff
                            t[1] += diff
                    elif direction == constants.DIRECTION_BACKWARD:
                        if end > prev_start:
                            diff = end - prev_start
                            t[0] -= diff
                            t[1] -= diff
                prev_start = t[0]
                prev_end = t[1]
        
        return times
    
    
    def __schedule_shutdown(self, vmrr):
        """ Schedules the shutdown of a VM reservation
                            
        Arguments:
        vmrr -- The VM reservation that will be shutdown
        
        """                 
        shutdown_time = vmrr.lease.estimate_shutdown_time()

        start = vmrr.end - shutdown_time
        end = vmrr.end
        
        shutdown_rr = ShutdownResourceReservation(vmrr.lease, start, end, vmrr.resources_in_pnode, vmrr.nodes, vmrr)
        shutdown_rr.state = ResourceReservation.STATE_SCHEDULED
                
        vmrr.update_end(start)
        
        # If there are any post RRs, remove them
        for rr in vmrr.post_rrs:
            self.slottable.remove_reservation(rr)
        vmrr.post_rrs = []

        vmrr.post_rrs.append(shutdown_rr)


    def __schedule_suspension(self, vmrr, suspend_by):
        """ Schedules the suspension of a VM reservation
                         
        Most of the work is done in __compute_susprem_times. See that
        method's documentation for more details.
                            
        Arguments:
        vmrr -- The VM reservation that will be suspended
        suspend_by -- The time by which the VMs should be suspended.
        
        """            
        config = get_config()
        susp_exclusion = config.get("suspendresume-exclusion")
        override = get_config().get("override-suspend-time")
        rate = config.get("suspend-rate") 

        if suspend_by < vmrr.start or suspend_by > vmrr.end:
            raise InconsistentScheduleError, "Tried to schedule a suspension by %s, which is outside the VMRR's duration (%s-%s)" % (suspend_by, vmrr.start, vmrr.end)

        # Find the suspension times
        times = self.__compute_susprem_times(vmrr, suspend_by, constants.DIRECTION_BACKWARD, susp_exclusion, rate, override)
        
        # Create the suspension resource reservations
        suspend_rrs = []
        for (start, end, node_mappings) in times:
            suspres = {}
            all_vnodes = []
            for (pnode,vnodes) in node_mappings.items():
                num_vnodes = len(vnodes)
                r = Capacity([constants.RES_MEM,constants.RES_DISK])
                mem = 0
                for vnode in vnodes:
                    mem += vmrr.lease.requested_resources[vnode].get_quantity(constants.RES_MEM)
                #r.set_quantity(constants.RES_CPU, 100) # TODO: Need to get the same quantity as pnode
                r.set_quantity(constants.RES_MEM, mem * num_vnodes)
                r.set_quantity(constants.RES_DISK, mem * num_vnodes)
                suspres[pnode] = self.slottable.create_resource_tuple_from_capacity(r)          
                all_vnodes += vnodes     
                             
            susprr = SuspensionResourceReservation(vmrr.lease, start, end, suspres, all_vnodes, vmrr)
            susprr.state = ResourceReservation.STATE_SCHEDULED
            suspend_rrs.append(susprr)
                
        suspend_rrs.sort(key=attrgetter("start"))
            
        susp_start = suspend_rrs[0].start
        if susp_start < vmrr.start:
            raise InconsistentScheduleError, "Determined suspension should start at %s, before the VMRR's start (%s) -- Suspend time not being properly estimated?" % (susp_start, vmrr.start)
        
        vmrr.update_end(susp_start)
        
        # If there are any post RRs, remove them
        for rr in vmrr.post_rrs:
            self.slottable.remove_reservation(rr)
        vmrr.post_rrs = []

        # Add the suspension RRs to the VM RR
        for susprr in suspend_rrs:
            vmrr.post_rrs.append(susprr)       
            
            
    def __schedule_resumption(self, vmrr, resume_at):
        """ Schedules the resumption of a VM reservation
                         
        Most of the work is done in __compute_susprem_times. See that
        method's documentation for more details.
                            
        Arguments:
        vmrr -- The VM reservation that will be resumed
        resume_at -- The time at which the resumption should start
        
        """                 
        config = get_config()
        resm_exclusion = config.get("suspendresume-exclusion")        
        override = get_config().get("override-resume-time")
        rate = config.get("resume-rate") 

        if resume_at < vmrr.start or resume_at > vmrr.end:
            raise InconsistentScheduleError, "Tried to schedule a resumption at %s, which is outside the VMRR's duration (%s-%s)" % (resume_at, vmrr.start, vmrr.end)

        # Find the resumption times
        times = self.__compute_susprem_times(vmrr, resume_at, constants.DIRECTION_FORWARD, resm_exclusion, rate, override)
        
        # Create the resumption resource reservations
        resume_rrs = []
        for (start, end, node_mappings) in times:
            resmres = {}
            all_vnodes = []
            for (pnode,vnodes) in node_mappings.items():
                num_vnodes = len(vnodes)
                r = Capacity([constants.RES_MEM,constants.RES_DISK])
                mem = 0
                for vnode in vnodes:
                    mem += vmrr.lease.requested_resources[vnode].get_quantity(constants.RES_MEM)
                #r.set_quantity(constants.RES_CPU, 100) # TODO: Need to get the same quantity as pnode                    
                r.set_quantity(constants.RES_MEM, mem * num_vnodes)
                r.set_quantity(constants.RES_DISK, mem * num_vnodes)
                resmres[pnode] = self.slottable.create_resource_tuple_from_capacity(r)
                all_vnodes += vnodes
            resmrr = ResumptionResourceReservation(vmrr.lease, start, end, resmres, all_vnodes, vmrr)
            resmrr.state = ResourceReservation.STATE_SCHEDULED
            resume_rrs.append(resmrr)
                
        resume_rrs.sort(key=attrgetter("start"))
            
        resm_end = resume_rrs[-1].end
        if resm_end > vmrr.end:
            raise InconsistentScheduleError, "Determined resumption would end at %s, after the VMRR's end (%s) -- Resume time not being properly estimated?" % (resm_end, vmrr.end)
        
        vmrr.update_start(resm_end)
        
        # Add the resumption RRs to the VM RR
        for resmrr in resume_rrs:
            vmrr.pre_rrs.append(resmrr)        


    def __compute_scheduling_threshold(self, lease):
        """ Compute the scheduling threshold (the 'minimum duration') of a lease
        
        To avoid thrashing, Haizea will not schedule a lease unless all overheads
        can be correctly scheduled (which includes image transfers, suspensions, etc.).
        However, this can still result in situations where a lease is prepared,
        and then immediately suspended because of a blocking lease in the future.
        The scheduling threshold is used to specify that a lease must
        not be scheduled unless it is guaranteed to run for a minimum amount of
        time (the rationale behind this is that you ideally don't want leases
        to be scheduled if they're not going to be active for at least as much time
        as was spent in overheads).
        
        An important part of computing this value is the "scheduling threshold factor".
        The default value is 1, meaning that the lease will be active for at least
        as much time T as was spent on overheads (e.g., if preparing the lease requires
        60 seconds, and we know that it will have to be suspended, requiring 30 seconds,
        Haizea won't schedule the lease unless it can run for at least 90 minutes).
        In other words, a scheduling factor of F required a minimum duration of 
        F*T. A value of 0 could lead to thrashing, since Haizea could end up with
        situations where a lease starts and immediately gets suspended.         
        
        Arguments:
        lease -- Lease for which we want to find the scheduling threshold
        """
        # TODO: Take into account other things like boot overhead, migration overhead, etc.
        config = get_config()
        threshold = config.get("force-scheduling-threshold")
        if threshold != None:
            # If there is a hard-coded threshold, use that
            return threshold
        else:
            factor = config.get("scheduling-threshold-factor")
            
            # First, figure out the "safe duration" (the minimum duration
            # so that we at least allocate enough time for all the
            # overheads).
            susp_overhead = lease.estimate_suspend_time()
            safe_duration = susp_overhead
            
            if lease.get_state() == Lease.STATE_SUSPENDED_QUEUED:
                resm_overhead = lease.estimate_resume_time()
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


    #-------------------------------------------------------------------#
    #                                                                   #
    #                  SLOT TABLE EVENT HANDLERS                        #
    #                                                                   #
    #-------------------------------------------------------------------#

    def _handle_start_vm(self, l, rr):
        """ Handles the start of a VMResourceReservation       
        
        Arguments:
        l -- Lease the VMResourceReservation belongs to
        rr -- THe VMResourceReservation
        """        
        self.logger.debug("LEASE-%i Start of handleStartVM" % l.id)
        l.print_contents()

        lease_state = l.get_state()

        if get_config().get("lease-preparation") == "imagetransfer":
            if not self.resourcepool.verify_deploy(l, rr):
                self.logger.error("Deployment was not complete.")
                raise # TODO raise something better

        # Kludge: Should be done by the preparations scheduler
        if l.get_state() == Lease.STATE_SCHEDULED:
            # Piggybacking
            l.set_state(Lease.STATE_READY)
            lease_state = l.get_state()
            
        if lease_state == Lease.STATE_READY:
            l.set_state(Lease.STATE_ACTIVE)
            rr.state = ResourceReservation.STATE_ACTIVE
            now_time = get_clock().get_time()
            l.start.actual = now_time
            
            try:
                self.resourcepool.start_vms(l, rr)
            except EnactmentError, exc:
                self.logger.error("Enactment error when starting VMs.")
                # Right now, this is a non-recoverable error, so we just
                # propagate it upwards to the lease scheduler
                # In the future, it may be possible to react to these
                # kind of errors.
                raise
                
        elif lease_state == Lease.STATE_RESUMED_READY:
            l.set_state(Lease.STATE_ACTIVE)
            rr.state = ResourceReservation.STATE_ACTIVE
            # No enactment to do here, since all the suspend/resume actions are
            # handled during the suspend/resume RRs
        else:
            raise InconsistentLeaseStateError(l, doing = "starting a VM")
        
        # If this was a future reservation (as determined by backfilling),
        # remove that status, since the future is now.
        if rr.lease in self.future_leases:
            self.future_leases.remove(l)
            get_persistence().persist_future_leases(self.future_leases)
        
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartVM" % l.id)
        self.logger.info("Started VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))


    def _handle_end_vm(self, l, rr):
        """ Handles the end of a VMResourceReservation       
        
        Arguments:
        l -- Lease the VMResourceReservation belongs to
        rr -- THe VMResourceReservation
        """        
        self.logger.debug("LEASE-%i Start of handleEndVM" % l.id)
        self.logger.vdebug("LEASE-%i Before:" % l.id)
        l.print_contents()
        now_time = round_datetime(get_clock().get_time())
        diff = now_time - rr.start
        l.duration.accumulate_duration(diff)
        rr.state = ResourceReservation.STATE_DONE
                
        self.logger.vdebug("LEASE-%i After:" % l.id)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndVM" % l.id)
        self.logger.info("Stopped VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))


    def _handle_unscheduled_end_vm(self, l, vmrr):
        """ Handles the unexpected end of a VMResourceReservation
        
        Arguments:
        l -- Lease the VMResourceReservation belongs to
        rr -- THe VMResourceReservation
        """  
        
        self.logger.info("LEASE-%i The VM has ended prematurely." % l.id)
        for rr in vmrr.post_rrs:
            self.slottable.remove_reservation(rr)
        vmrr.post_rrs = []
        vmrr.end = get_clock().get_time()
        self._handle_end_vm(l, vmrr)


    def _handle_start_suspend(self, l, rr):
        """ Handles the start of a SuspensionResourceReservation       
        
        Arguments:
        l -- Lease the SuspensionResourceReservation belongs to
        rr -- The SuspensionResourceReservation
        
        """
        self.logger.debug("LEASE-%i Start of handleStartSuspend" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        
        try:
            self.resourcepool.suspend_vms(l, rr)
        except EnactmentError, exc:
            self.logger.error("Enactment error when suspending VMs.")
            # Right now, this is a non-recoverable error, so we just
            # propagate it upwards to the lease scheduler
            # In the future, it may be possible to react to these
            # kind of errors.
            raise            

        if rr.is_first():
            l.set_state(Lease.STATE_SUSPENDING)
            l.print_contents()
            self.logger.info("Suspending lease %i..." % (l.id))
        self.logger.debug("LEASE-%i End of handleStartSuspend" % l.id)


    def _handle_end_suspend(self, l, rr):
        """ Handles the end of a SuspensionResourceReservation       
        
        Arguments:
        l -- Lease the SuspensionResourceReservation belongs to
        rr -- The SuspensionResourceReservation
        """               
        self.logger.debug("LEASE-%i Start of handleEndSuspend" % l.id)
        l.print_contents()
        # TODO: React to incomplete suspend
        self.resourcepool.verify_suspend(l, rr)
        rr.state = ResourceReservation.STATE_DONE
        if rr.is_last():
            if l.get_type() == Lease.DEADLINE:
                l.set_state(Lease.STATE_SUSPENDED_PENDING)
                l.set_state(Lease.STATE_SUSPENDED_SCHEDULED)
            else:
                l.set_state(Lease.STATE_SUSPENDED_PENDING)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndSuspend" % l.id)
        self.logger.info("Lease %i suspended." % (l.id))
        
        if l.get_state() == Lease.STATE_SUSPENDED_PENDING:
            raise RescheduleLeaseException


    def _handle_start_resume(self, l, rr):
        """ Handles the start of a ResumptionResourceReservation       
        
        Arguments:
        l -- Lease the ResumptionResourceReservation belongs to
        rr -- The ResumptionResourceReservation
        
        """             
        self.logger.debug("LEASE-%i Start of handleStartResume" % l.id)
        l.print_contents()
        
        try:
            self.resourcepool.resume_vms(l, rr)
        except EnactmentError, exc:
            self.logger.error("Enactment error when resuming VMs.")
            # Right now, this is a non-recoverable error, so we just
            # propagate it upwards to the lease scheduler
            # In the future, it may be possible to react to these
            # kind of errors.
            raise
                    
        rr.state = ResourceReservation.STATE_ACTIVE
        if rr.is_first():
            l.set_state(Lease.STATE_RESUMING)
            l.print_contents()
            self.logger.info("Resuming lease %i..." % (l.id))
        self.logger.debug("LEASE-%i End of handleStartResume" % l.id)


    def _handle_end_resume(self, l, rr):
        """ Handles the end of a ResumptionResourceReservation       
        
        Arguments:
        l -- Lease the ResumptionResourceReservation belongs to
        rr -- The ResumptionResourceReservation
        
        """        
        self.logger.debug("LEASE-%i Start of handleEndResume" % l.id)
        l.print_contents()
        # TODO: React to incomplete resume
        self.resourcepool.verify_resume(l, rr)
        rr.state = ResourceReservation.STATE_DONE
        if rr.is_last():
            l.set_state(Lease.STATE_RESUMED_READY)
            self.logger.info("Resumed lease %i" % (l.id))
        for vnode, pnode in rr.vmrr.nodes.items():
            self.resourcepool.remove_ramfile(pnode, l.id, vnode)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndResume" % l.id)


    def _handle_start_shutdown(self, l, rr):
        """ Handles the start of a ShutdownResourceReservation       
        
        Arguments:
        l -- Lease the SuspensionResourceReservation belongs to
        rr -- The SuspensionResourceReservation
        """        
        
        self.logger.debug("LEASE-%i Start of handleStartShutdown" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        try:
            self.resourcepool.stop_vms(l, rr)
        except EnactmentError, exc:
            self.logger.error("Enactment error when shutting down VMs.")
            # Right now, this is a non-recoverable error, so we just
            # propagate it upwards to the lease scheduler
            # In the future, it may be possible to react to these
            # kind of errors.
            raise
        
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartShutdown" % l.id)


    def _handle_end_shutdown(self, l, rr):
        """ Handles the end of a SuspensionResourceReservation       
        
        Arguments:
        l -- Lease the SuspensionResourceReservation belongs to
        rr -- The SuspensionResourceReservation
        
        """
        self.logger.debug("LEASE-%i Start of handleEndShutdown" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_DONE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndShutdown" % l.id)
        self.logger.info("Lease %i's VMs have shutdown." % (l.id))
        raise NormalEndLeaseException
    

    def _handle_start_migrate(self, l, rr):
        """ Handles the start of a MemImageMigrationResourceReservation       
        
        Arguments:
        l -- Lease the MemImageMigrationResourceReservation belongs to
        rr -- The MemImageMigrationResourceReservation
        
        """             
        self.logger.debug("LEASE-%i Start of handleStartMigrate" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartMigrate" % l.id)
        self.logger.info("Migrating lease %i..." % (l.id))


    def _handle_end_migrate(self, l, rr):
        """ Handles the end of a MemImageMigrationResourceReservation       
        
        Arguments:
        l -- Lease the MemImageMigrationResourceReservation belongs to
        rr -- The MemImageMigrationResourceReservation
        
        """                
        self.logger.debug("LEASE-%i Start of handleEndMigrate" % l.id)
        l.print_contents()

        for vnode in rr.transfers:
            origin = rr.transfers[vnode][0]
            dest = rr.transfers[vnode][1]
            
            # Update RAM files
            self.resourcepool.remove_ramfile(origin, l.id, vnode)
            self.resourcepool.add_ramfile(dest, l.id, vnode, l.requested_resources[vnode].get_quantity(constants.RES_MEM))
        
        rr.state = ResourceReservation.STATE_DONE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndMigrate" % l.id)
        self.logger.info("Migrated lease %i..." % (l.id))



class VMResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, nodes, res):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.nodes = nodes # { vnode -> pnode }
        self.pre_rrs = []
        self.post_rrs = []
        self.prematureend = None

    def update_start(self, time):
        self.start = time
        # ONLY for simulation
        self.lease._update_prematureend()

    def update_end(self, time):
        self.end = time
        # ONLY for simulation
        self.lease._update_prematureend()
        
    def get_first_start(self):
        if len(self.pre_rrs) == 0:
            return self.start
        else:
            return [rr for rr in self.pre_rrs if isinstance(rr, ResumptionResourceReservation)][0].start

    def get_final_end(self):
        if len(self.post_rrs) == 0:
            return self.end
        else:
            return self.post_rrs[-1].end

    def is_resuming(self):
        return len(self.pre_rrs) > 0 and reduce(operator.or_, [isinstance(rr, ResumptionResourceReservation) for rr in self.pre_rrs])

    def is_suspending(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], SuspensionResourceReservation)

    def is_shutting_down(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], ShutdownResourceReservation)
    
    def clear_rrs(self):
        for rr in self.pre_rrs:
            rr.clear_rrs()
        for rr in self.post_rrs:
            rr.clear_rrs()
        self.pre_rrs = None
        self.post_rrs = None

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        for resmrr in self.pre_rrs:
            resmrr.print_contents(loglevel)
            logger.log(loglevel, "--")
        logger.log(loglevel, "Type           : VM")
        logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes))
        if self.prematureend != None:
            logger.log(loglevel, "Premature end  : %s" % self.prematureend)
        ResourceReservation.print_contents(self, loglevel)
        for susprr in self.post_rrs:
            logger.log(loglevel, "--")
            susprr.print_contents(loglevel)

        
class SuspensionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : SUSPEND")
        logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)
        
    def is_first(self):
        return (self == self.vmrr.post_rrs[0])

    def is_last(self):
        return (self == self.vmrr.post_rrs[-1])   
        
    def clear_rrs(self):
        self.vmrr = None
        
class ResumptionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : RESUME")
        logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)

    def is_first(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[0])

    def is_last(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[-1])
    
    def clear_rrs(self):
        self.vmrr = None
    
class ShutdownResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : SHUTDOWN")
        ResourceReservation.print_contents(self, loglevel)

    def clear_rrs(self):
        self.vmrr = None

class MemImageMigrationResourceReservation(MigrationResourceReservation):
    def __init__(self, lease, start, end, res, vmrr, transfers):
        MigrationResourceReservation.__init__(self, lease, start, end, res, vmrr, transfers)
  
    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : MEM IMAGE MIGRATION")
        logger.log(loglevel, "Transfers      : %s" % self.transfers)
        ResourceReservation.print_contents(self, loglevel)
