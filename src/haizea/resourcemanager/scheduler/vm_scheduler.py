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

import haizea.common.constants as constants
from haizea.common.utils import round_datetime_delta, round_datetime, estimate_transfer_time, pretty_nodemap, get_config, get_accounting, get_clock
from haizea.resourcemanager.scheduler.slottable import SlotTable, SlotFittingException
from haizea.resourcemanager.leases import Lease, ARLease, BestEffortLease, ImmediateLease
from haizea.resourcemanager.scheduler.slottable import ResourceReservation, ResourceTuple
from haizea.resourcemanager.scheduler.resourcepool import ResourcePool, ResourcePoolWithReusableImages
from haizea.resourcemanager.scheduler import ReservationEventHandler, RescheduleLeaseException, NormalEndLeaseException
from operator import attrgetter, itemgetter
from mx.DateTime import TimeDelta

import logging


class VMScheduler(object):
    """The Haizea VM Scheduler
    
    """
    
    def __init__(self, slottable, resourcepool):
        self.slottable = slottable
        self.resourcepool = resourcepool
        self.logger = logging.getLogger("VMSCHED")
        
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

        self.handlers[MigrationResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = VMScheduler._handle_start_migrate,
                                on_end   = VMScheduler._handle_end_migrate)
        
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

    def fit_exact(self, leasereq, preemptible=False, canpreempt=True, avoidpreempt=True):
        lease_id = leasereq.id
        start = leasereq.start.requested
        end = leasereq.start.requested + leasereq.duration.requested + self.__estimate_shutdown_time(leasereq)
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
                        res[physnode] = ResourceTuple.copy(resreq)
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
                        res[physnode] = ResourceTuple.copy(resreq)
                    canfit[physnode][1] -= 1
                    vnode += 1
                    # Check if this will actually result in a preemption
                    if canfit[physnode][0] == 0:
                        if preemptions.has_key(physnode):
                            preemptions[physnode].incr(resreq)
                        else:
                            preemptions[physnode] = ResourceTuple.copy(resreq)
                    else:
                        canfit[physnode][0] -= 1
                    if vnode > numnodes:
                        break
                if vnode > numnodes:
                    break

        if vnode <= numnodes:
            raise SchedException, "Availability window indicated that request is feasible, but could not fit it"

        # Create VM resource reservations
        vmrr = VMResourceReservation(leasereq, start, end, nodeassignment, res, False)
        vmrr.state = ResourceReservation.STATE_SCHEDULED

        self.__schedule_shutdown(vmrr)

        return vmrr, preemptions

    def fit_asap(self, lease, nexttime, earliest, allow_reservation_in_future = False):
        lease_id = lease.id
        remaining_duration = lease.duration.get_remaining_duration()
        numnodes = lease.numnodes
        requested_resources = lease.requested_resources
        preemptible = lease.preemptible
        mustresume = (lease.state == Lease.STATE_SUSPENDED)
        shutdown_time = self.__estimate_shutdown_time(lease)
        susptype = get_config().get("suspension")
        if susptype == constants.SUSPENSION_NONE or (susptype == constants.SUSPENSION_SERIAL and lease.numnodes == 1):
            suspendable = False
        else:
            suspendable = True

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
            res = self.slottable.get_reservations_ending_after(changepoints[-1][0])
            futurecp = [r.get_final_end() for r in res if isinstance(r, VMResourceReservation)]
            # Corner case: Sometimes we're right in the middle of a ShutdownReservation, so it won't be
            # included in futurecp.
            futurecp += [r.end for r in res if isinstance(r, ShutdownResourceReservation) and not r.vmrr in res]
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

        duration += shutdown_time

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
                        res[n] = ResourceTuple.copy(requested_resources)
                    vmnode += 1
                    break


        vmrr = VMResourceReservation(lease, start, end, mappings, res, reservation)
        vmrr.state = ResourceReservation.STATE_SCHEDULED

        if mustresume:
            self.__schedule_resumption(vmrr, start)

        mustsuspend = (vmrr.end - vmrr.start) < remaining_duration
        if mustsuspend:
            self.__schedule_suspension(vmrr, end)
        else:
            # Compensate for any overestimation
            if (vmrr.end - vmrr.start) > remaining_duration + shutdown_time:
                vmrr.end = vmrr.start + remaining_duration + shutdown_time
            self.__schedule_shutdown(vmrr)
        

        
        susp_str = res_str = ""
        if mustresume:
            res_str = " (resuming)"
        if mustsuspend:
            susp_str = " (suspending)"
        self.logger.info("Lease #%i has been scheduled on nodes %s from %s%s to %s%s" % (lease.id, mappings.values(), start, res_str, end, susp_str))

        return vmrr, reservation

    # TODO: This has to be tied in with the preparation scheduler
    def schedule_migration(self, lease, vmrr, nexttime):
        last_vmrr = lease.get_last_vmrr()
        vnode_migrations = dict([(vnode, (last_vmrr.nodes[vnode], vmrr.nodes[vnode])) for vnode in vmrr.nodes])
        
        mustmigrate = False
        for vnode in vnode_migrations:
            if vnode_migrations[vnode][0] != vnode_migrations[vnode][1]:
                mustmigrate = True
                break
            
        if not mustmigrate:
            return

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
        start = last_vmrr.post_rrs[-1].end
        migr_time = self.__estimate_migration_time(lease)
        bandwidth = self.resourcepool.info.get_migration_bandwidth()
        migr_rrs = []
        for m in migrations:
            end = start + migr_time
            res = {}
            for (origin,dest) in m.values():
                resorigin = ResourceTuple.create_empty()
                resorigin.set_by_type(constants.RES_NETOUT, bandwidth)
                resdest = ResourceTuple.create_empty()
                resdest.set_by_type(constants.RES_NETIN, bandwidth)
                res[origin] = resorigin
                res[dest] = resdest
            migr_rr = MigrationResourceReservation(lease, start, start + migr_time, res, vmrr, m)
            migr_rr.state = ResourceReservation.STATE_SCHEDULED
            migr_rrs.append(migr_rr)
            start = end

        migr_rrs.reverse()
        for migr_rr in migr_rrs:
            vmrr.pre_rrs.insert(0, migr_rr)

    def preempt(self, vmrr, t):
        # Save original start and end time of the vmrr
        old_start = vmrr.start
        old_end = vmrr.end
        self.__schedule_suspension(vmrr, t)
        self.slottable.update_reservation_with_key_change(vmrr, old_start, old_end)
        for susprr in vmrr.post_rrs:
            self.slottable.addReservation(susprr)
        
        
        
    def can_suspend_at(self, lease, t):
        # TODO: Make more general, should determine vmrr based on current time
        vmrr = lease.get_last_vmrr()
        time_until_suspend = t - vmrr.start
        min_duration = self.__compute_scheduling_threshold(lease)
        can_suspend = time_until_suspend >= min_duration        
        return can_suspend


    def can_reserve_besteffort_in_future(self):
        return self.numbesteffortres < self.maxres
                
    def is_backfilling(self):
        return self.maxres > 0


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
    
    def __compute_susprem_times(self, vmrr, time, direction, exclusion, rate, override = None):
        times = [] # (start, end, {pnode -> vnodes})
        enactment_overhead = get_config().get("enactment-overhead") 

        if exclusion == constants.SUSPRES_EXCLUSION_GLOBAL:
            # Global exclusion (which represents, e.g., reading/writing the memory image files
            # from a global file system) meaning no two suspensions/resumptions can happen at 
            # the same time in the entire resource pool.
            
            t = time
            t_prev = None
                
            for (vnode,pnode) in vmrr.nodes.items():
                if override == None:
                    mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                    op_time = self.__compute_suspend_resume_time(mem, rate)
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
                        mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                        op_time = self.__compute_suspend_resume_time(mem, rate)
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
        config = get_config()
        shutdown_time = self.__estimate_shutdown_time(vmrr.lease)

        start = vmrr.end - shutdown_time
        end = vmrr.end
        
        shutdown_rr = ShutdownResourceReservation(vmrr.lease, start, end, vmrr.resources_in_pnode, vmrr.nodes, vmrr)
        shutdown_rr.state = ResourceReservation.STATE_SCHEDULED
                
        vmrr.update_end(start)
        
        # If there are any post RRs, remove them
        for rr in vmrr.post_rrs:
            self.slottable.removeReservation(rr)
        vmrr.post_rrs = []

        vmrr.post_rrs.append(shutdown_rr)

    def __schedule_suspension(self, vmrr, suspend_by):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        susp_exclusion = config.get("suspendresume-exclusion")
        override = get_config().get("override-suspend-time")
        rate = config.get("suspend-rate") 

        if suspend_by < vmrr.start or suspend_by > vmrr.end:
            raise SchedException, "Tried to schedule a suspension by %s, which is outside the VMRR's duration (%s-%s)" % (suspend_by, vmrr.start, vmrr.end)

        times = self.__compute_susprem_times(vmrr, suspend_by, constants.DIRECTION_BACKWARD, susp_exclusion, rate, override)
        suspend_rrs = []
        for (start, end, node_mappings) in times:
            suspres = {}
            all_vnodes = []
            for (pnode,vnodes) in node_mappings.items():
                num_vnodes = len(vnodes)
                r = ResourceTuple.create_empty()
                mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                r.set_by_type(constants.RES_MEM, mem * num_vnodes)
                r.set_by_type(constants.RES_DISK, mem * num_vnodes)
                suspres[pnode] = r          
                all_vnodes += vnodes                      
            susprr = SuspensionResourceReservation(vmrr.lease, start, end, suspres, all_vnodes, vmrr)
            susprr.state = ResourceReservation.STATE_SCHEDULED
            suspend_rrs.append(susprr)
                
        suspend_rrs.sort(key=attrgetter("start"))
            
        susp_start = suspend_rrs[0].start
        if susp_start < vmrr.start:
            raise SchedException, "Determined suspension should start at %s, before the VMRR's start (%s) -- Suspend time not being properly estimated?" % (susp_start, vmrr.start)
        
        vmrr.update_end(susp_start)
        
        # If there are any post RRs, remove them
        for rr in vmrr.post_rrs:
            self.slottable.removeReservation(rr)
        vmrr.post_rrs = []

        for susprr in suspend_rrs:
            vmrr.post_rrs.append(susprr)       
            
    def __schedule_resumption(self, vmrr, resume_at):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        resm_exclusion = config.get("suspendresume-exclusion")        
        override = get_config().get("override-resume-time")
        rate = config.get("resume-rate") 

        if resume_at < vmrr.start or resume_at > vmrr.end:
            raise SchedException, "Tried to schedule a resumption at %s, which is outside the VMRR's duration (%s-%s)" % (resume_at, vmrr.start, vmrr.end)

        times = self.__compute_susprem_times(vmrr, resume_at, constants.DIRECTION_FORWARD, resm_exclusion, rate, override)
        resume_rrs = []
        for (start, end, node_mappings) in times:
            resmres = {}
            all_vnodes = []
            for (pnode,vnodes) in node_mappings.items():
                num_vnodes = len(vnodes)
                r = ResourceTuple.create_empty()
                mem = vmrr.lease.requested_resources.get_by_type(constants.RES_MEM)
                r.set_by_type(constants.RES_MEM, mem * num_vnodes)
                r.set_by_type(constants.RES_DISK, mem * num_vnodes)
                resmres[pnode] = r
                all_vnodes += vnodes
            resmrr = ResumptionResourceReservation(vmrr.lease, start, end, resmres, all_vnodes, vmrr)
            resmrr.state = ResourceReservation.STATE_SCHEDULED
            resume_rrs.append(resmrr)
                
        resume_rrs.sort(key=attrgetter("start"))
            
        resm_end = resume_rrs[-1].end
        if resm_end > vmrr.end:
            raise SchedException, "Determined resumption would end at %s, after the VMRR's end (%s) -- Resume time not being properly estimated?" % (resm_end, vmrr.end)
        
        vmrr.update_start(resm_end)
        for resmrr in resume_rrs:
            vmrr.pre_rrs.append(resmrr)        
           
    def __compute_suspend_resume_time(self, mem, rate):
        time = float(mem) / rate
        time = round_datetime_delta(TimeDelta(seconds = time))
        return time
    
    def __estimate_suspend_resume_time(self, lease, rate):
        susp_exclusion = get_config().get("suspendresume-exclusion")        
        enactment_overhead = get_config().get("enactment-overhead") 
        mem = lease.requested_resources.get_by_type(constants.RES_MEM)
        if susp_exclusion == constants.SUSPRES_EXCLUSION_GLOBAL:
            return lease.numnodes * (self.__compute_suspend_resume_time(mem, rate) + enactment_overhead)
        elif susp_exclusion == constants.SUSPRES_EXCLUSION_LOCAL:
            # Overestimating
            return lease.numnodes * (self.__compute_suspend_resume_time(mem, rate) + enactment_overhead)

    def __estimate_shutdown_time(self, lease):
        enactment_overhead = get_config().get("enactment-overhead").seconds
        return get_config().get("shutdown-time") + (enactment_overhead * lease.numnodes)

    def __estimate_suspend_time(self, lease):
        rate = get_config().get("suspend-rate")
        override = get_config().get("override-suspend-time")
        if override != None:
            return override
        else:
            return self.__estimate_suspend_resume_time(lease, rate)

    def __estimate_resume_time(self, lease):
        rate = get_config().get("resume-rate") 
        override = get_config().get("override-resume-time")
        if override != None:
            return override
        else:
            return self.__estimate_suspend_resume_time(lease, rate)


    def __estimate_migration_time(self, lease):
        whattomigrate = get_config().get("what-to-migrate")
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

    def find_preemptable_leases(self, mustpreempt, startTime, endTime):
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
        reservationsAtStart = [r for r in reservationsAtStart if isinstance(r, VMResourceReservation) and r.is_preemptible()
                        and len(set(r.resources_in_pnode.keys()) & nodes)>0]
        
        reservationsAtMiddle = self.slottable.get_reservations_starting_between(startTime, endTime)
        reservationsAtMiddle = [r for r in reservationsAtMiddle if isinstance(r, VMResourceReservation) and r.is_preemptible()
                        and len(set(r.resources_in_pnode.keys()) & nodes)>0]
        
        reservationsAtStart.sort(comparepreemptability)
        reservationsAtMiddle.sort(comparepreemptability)
        
        amountToPreempt = {}
        for n in mustpreempt:
            amountToPreempt[n] = ResourceTuple.copy(mustpreempt[n])

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
                    amountToPreempt[n] = ResourceTuple.copy(mustpreempt[n])
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
                
    # TODO: Should be moved to LeaseScheduler
    def reevaluate_schedule(self, endinglease, nodes, nexttime, checkedleases):
        self.logger.debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes, nexttime)) 
        leases = []
        vmrrs = self.slottable.get_next_reservations_in_nodes(nexttime, nodes, rr_type=VMResourceReservation, immediately_next=True)
        leases = set([rr.lease for rr in vmrrs])
        leases = [l for l in leases if isinstance(l, BestEffortLease) and l.state in (Lease.STATE_SUSPENDED,Lease.STATE_READY) and not l in checkedleases]
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
            originalstart = vmrr.pre_rrs[0].start
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
                resmrrs = [r for r in vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
                for resmrr in resmrrs:
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
                for susprr in vmrr.post_rrs:
                    self.slottable.removeReservation(susprr)
                vmrr.post_rrs = []
            else:
                vmrr.update_end(vmrr.end - diff)
                
            if not vmrr.is_suspending():
                # If the VM was set to shutdown, we need to slideback the shutdown RRs
                for rr in vmrr.post_rrs:
                    rr_old_start = rr.start
                    rr_old_end = rr.end
                    rr.start -= diff
                    rr.end -= diff
                    self.slottable.update_reservation_with_key_change(rr, rr_old_start, rr_old_end)

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


    def _handle_end_vm(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndVM" % l.id)
        self.logger.vdebug("LEASE-%i Before:" % l.id)
        l.print_contents()
        now_time = round_datetime(get_clock().get_time())
        diff = now_time - rr.start
        l.duration.accumulate_duration(diff)
        rr.state = ResourceReservation.STATE_DONE
       
        if isinstance(l, BestEffortLease):
            if rr.backfill_reservation == True:
                self.numbesteffortres -= 1
                
        self.logger.vdebug("LEASE-%i After:" % l.id)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndVM" % l.id)
        self.logger.info("Stopped VMs for lease %i on nodes %s" % (l.id, rr.nodes.values()))

    def _handle_unscheduled_end_vm(self, l, vmrr, enact=False):
        self.logger.info("LEASE-%i The VM has ended prematurely." % l.id)
        for rr in vmrr.post_rrs:
            self.slottable.removeReservation(rr)
        vmrr.post_rrs = []
        # TODO: slideback shutdown RRs
        vmrr.end = get_clock().get_time()
        self._handle_end_vm(l, vmrr)

    def _handle_start_shutdown(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartShutdown" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        self.resourcepool.stop_vms(l, rr)
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartShutdown" % l.id)

    def _handle_end_shutdown(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndShutdown" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_DONE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndShutdown" % l.id)
        self.logger.info("Lease %i shutdown." % (l.id))
        raise NormalEndLeaseException



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
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndSuspend" % l.id)
        self.logger.info("Lease %i suspended." % (l.id))
        
        if l.state == Lease.STATE_SUSPENDED:
            raise RescheduleLeaseException

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
        rr.state = ResourceReservation.STATE_ACTIVE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartMigrate" % l.id)
        self.logger.info("Migrating lease %i..." % (l.id))

    def _handle_end_migrate(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndMigrate" % l.id)
        l.print_contents()

        for vnode in rr.transfers:
            origin = rr.transfers[vnode][0]
            dest = rr.transfers[vnode][1]
            
            # Update VM image mappings
            self.resourcepool.remove_diskimage(origin, l.id, vnode)
            self.resourcepool.add_diskimage(dest, l.diskimage_id, l.diskimage_size, l.id, vnode)
            l.diskimagemap[vnode] = dest

            # Update RAM file mappings
            self.resourcepool.remove_ramfile(origin, l.id, vnode)
            self.resourcepool.add_ramfile(dest, l.id, vnode, l.requested_resources.get_by_type(constants.RES_MEM))
            l.memimagemap[vnode] = dest
        
        rr.state = ResourceReservation.STATE_DONE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndMigrate" % l.id)
        self.logger.info("Migrated lease %i..." % (l.id))



            

class VMResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, nodes, res, backfill_reservation):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.nodes = nodes # { vnode -> pnode }
        self.backfill_reservation = backfill_reservation
        self.pre_rrs = []
        self.post_rrs = []

        # ONLY for simulation
        self.__update_prematureend()

    def update_start(self, time):
        self.start = time
        # ONLY for simulation
        self.__update_prematureend()

    def update_end(self, time):
        self.end = time
        # ONLY for simulation
        self.__update_prematureend()
        
    # ONLY for simulation
    def __update_prematureend(self):
        if self.lease.duration.known != None:
            remdur = self.lease.duration.get_remaining_known_duration()
            rrdur = self.end - self.start
            if remdur < rrdur:
                self.prematureend = self.start + remdur
            else:
                self.prematureend = None
        else:
            self.prematureend = None 

    def get_final_end(self):
        if len(self.post_rrs) == 0:
            return self.end
        else:
            return self.post_rrs[-1].end

    def is_suspending(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], SuspensionResourceReservation)

    def is_shutting_down(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], ShutdownResourceReservation)

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        for resmrr in self.pre_rrs:
            resmrr.print_contents(loglevel)
            self.logger.log(loglevel, "--")
        self.logger.log(loglevel, "Type           : VM")
        self.logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes))
        if self.prematureend != None:
            self.logger.log(loglevel, "Premature end  : %s" % self.prematureend)
        ResourceReservation.print_contents(self, loglevel)
        for susprr in self.post_rrs:
            self.logger.log(loglevel, "--")
            susprr.print_contents(loglevel)
        
    def is_preemptible(self):
        return self.lease.preemptible

    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "VM"
        rr["nodes"] = self.nodes.items()
        return rr

        
class SuspensionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : SUSPEND")
        self.logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)
        
    def is_first(self):
        return (self == self.vmrr.post_rrs[0])

    def is_last(self):
        return (self == self.vmrr.post_rrs[-1])
        
    # TODO: Suspension RRs should be preemptible, but preempting a suspension RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a suspension RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "SUSP"
        return rr
        
class ResumptionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : RESUME")
        self.logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)

    def is_first(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[0])

    def is_last(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[-1])

    # TODO: Resumption RRs should be preemptible, but preempting a resumption RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a resumption RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "RESM"
        return rr
    
class ShutdownResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : SHUTDOWN")
        ResourceReservation.print_contents(self, loglevel)
        
    def is_preemptible(self):
        return True        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "SHTD"
        return rr

class MigrationResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vmrr, transfers):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.transfers = transfers
        
    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : MIGRATE")
        self.logger.log(loglevel, "Transfers      : %s" % self.transfers)
        ResourceReservation.print_contents(self, loglevel)        

    def is_preemptible(self):
        return False        

