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

from mx.DateTime import ISO, TimeDelta
from operator import attrgetter, itemgetter
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from haizea.common.utils import roundDateTimeDelta
import bisect
import copy
import logging

class SlotFittingException(Exception):
    pass

class CriticalSlotFittingException(Exception):
    pass


class Node(object):
    def __init__(self, capacity, capacitywithpreemption, resourcepoolnode):
        self.capacity = ds.ResourceTuple.copy(capacity)
        self.capacitywithpreemption = ds.ResourceTuple.copy(capacitywithpreemption)
        self.resourcepoolnode = resourcepoolnode
        
    @classmethod
    def from_resourcepool_node(cls, node):
        capacity = node.get_capacity()
        return cls(capacity, capacity, node)

class NodeList(object):
    def __init__(self):
        self.nodelist = []

    def add(self, node):
        self.nodelist.append(node)
        
    def __getitem__(self, n):
        return self.nodelist[n-1]

    def copy(self):
        nodelist = NodeList()
        for n in self.nodelist:
            nodelist.add(Node(n.capacity, n.capacitywithpreemption, n.resourcepoolnode))
        return nodelist

    def toPairList(self, onlynodes=None):
        nodelist = []
        for i, n in enumerate(self.nodelist):
            if onlynodes == None or (onlynodes != None and i+1 in onlynodes):
                nodelist.append((i+1,Node(n.capacity, n.capacitywithpreemption, n.resourcepoolnode)))
        return nodelist
    
    def toDict(self):
        nodelist = self.copy()
        return dict([(i+1, v) for i, v in enumerate(nodelist)])
        
class KeyValueWrapper(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        
    def __cmp__(self, other):
        return cmp(self.key, other.key)

class SlotTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.rm = scheduler.rm
        self.resourcepool = scheduler.resourcepool
        self.logger = logging.getLogger("SLOTTABLE")
        self.nodes = NodeList()
        self.reservations = []
        self.reservationsByStart = []
        self.reservationsByEnd = []
        self.availabilitycache = {}
        self.changepointcache = None
        
        self.availabilitywindow = AvailabilityWindow(self)

    def add_node(self, resourcepoolnode):
        self.nodes.add(Node.from_resourcepool_node(resourcepoolnode))

    def dirty(self):
        # You're a dirty, dirty slot table and you should be
        # ashamed of having outdated caches!
        self.availabilitycache = {}
        self.changepointcache = None
        
    def getAvailabilityCacheMiss(self, time):
        nodes = self.nodes.copy()
        reservations = self.getReservationsAt(time)
        # Find how much resources are available on each node
        for r in reservations:
            for node in r.resources_in_pnode:
                nodes[node].capacity.decr(r.resources_in_pnode[node])
                if not r.is_preemptible():
                    nodes[node].capacitywithpreemption.decr(r.resources_in_pnode[node])                        
            
        self.availabilitycache[time] = nodes

    def getAvailability(self, time, resreq=None, onlynodes=None):
        if not self.availabilitycache.has_key(time):
            self.getAvailabilityCacheMiss(time)
            # Cache miss
            
        if onlynodes != None:
            onlynodes = set(onlynodes)
            
        nodes = self.availabilitycache[time].toPairList(onlynodes)
        #nodes = {}
        #for n in self.availabilitycache[time]:
        #    nodes[n] = Node(self.availabilitycache[time][n].capacity.res, self.availabilitycache[time][n].capacitywithpreemption.res)

        # Keep only those nodes with enough resources
        if resreq != None:
            newnodes = []
            for i, node in nodes:
                if not resreq.fits_in(node.capacity) and not resreq.fits_in(node.capacitywithpreemption):
                    pass
                else:
                    newnodes.append((i, node))
            nodes = newnodes
        
        return dict(nodes)
    
    def getUtilization(self, time, restype=constants.RES_CPU):
        nodes = self.getAvailability(time)
        total = sum([n.capacity.get_by_type(restype) for n in self.nodes.nodelist])
        avail = sum([n.capacity.get_by_type(restype) for n in nodes.values()])
        return 1.0 - (float(avail)/total)

    def getReservationsAt(self, time):
        item = KeyValueWrapper(time, None)
        startpos = bisect.bisect_right(self.reservationsByStart, item)
        bystart = set([x.value for x in self.reservationsByStart[:startpos]])
        endpos = bisect.bisect_right(self.reservationsByEnd, item)
        byend = set([x.value for x in self.reservationsByEnd[endpos:]])
        res = bystart & byend
        return list(res)
    
    def getReservationsStartingBetween(self, start, end):
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservationsByStart, startitem)
        endpos = bisect.bisect_right(self.reservationsByEnd, enditem)
        res = [x.value for x in self.reservationsByStart[startpos:endpos]]
        return res
    
    # ONLY for simulation
    def getNextPrematureEnd(self, after):
        # Inefficient, but ok since this query seldom happens
        res = [i.value for i in self.reservationsByEnd if isinstance(i.value, ds.VMResourceReservation) and i.value.prematureend > after]
        if len(res) > 0:
            prematureends = [r.prematureend for r in res]
            prematureends.sort()
            return prematureends[0]
        else:
            return None
    
    # ONLY for simulation
    def getPrematurelyEndingRes(self, t):
        return [i.value for i in self.reservationsByEnd if isinstance(i.value, ds.VMResourceReservation) and i.value.prematureend == t]

    
    def getReservationsWithChangePointsAfter(self, after):
        item = KeyValueWrapper(after, None)
        startpos = bisect.bisect_right(self.reservationsByStart, item)
        bystart = set([x.value for x in self.reservationsByStart[startpos:]])
        endpos = bisect.bisect_right(self.reservationsByEnd, item)
        byend = set([x.value for x in self.reservationsByEnd[endpos:]])
        res = bystart | byend
        return list(res)    
    
    def addReservation(self, rr):
        startitem = KeyValueWrapper(rr.start, rr)
        enditem = KeyValueWrapper(rr.end, rr)
        bisect.insort(self.reservationsByStart, startitem)
        bisect.insort(self.reservationsByEnd, enditem)
        self.dirty()

    # If the slot table keys are not modified (start / end time)
    # Just remove and reinsert.
    def updateReservation(self, rr):
        # TODO: Might be more efficient to resort lists
        self.removeReservation(rr)
        self.addReservation(rr)
        self.dirty()

    # If the slot table keys are modified (start and/or end time)
    # provide the old reservation (so we can remove it using
    # the original keys) and also the new reservation
    def updateReservationWithKeyChange(self, rrold, rrnew):
        # TODO: Might be more efficient to resort lists
        self.removeReservation(rrold)
        self.addReservation(rrnew)
        rrold.lease.replace_rr(rrold, rrnew)
        self.dirty()


    def getIndexOfReservation(self, rlist, rr, key):
        item = KeyValueWrapper(key, None)
        pos = bisect.bisect_left(rlist, item)
        found = False
        while not found:
            if rlist[pos].value == rr:
                found = True
            else:
                pos += 1
        return pos

    def removeReservation(self, rr, start=None, end=None):
        if start == None:
            start = rr.start
        if end == None:
            end = rr.start
        posstart = self.getIndexOfReservation(self.reservationsByStart, rr, start)
        posend = self.getIndexOfReservation(self.reservationsByEnd, rr, end)
        self.reservationsByStart.pop(posstart)
        self.reservationsByEnd.pop(posend)
        self.dirty()

    
    def findChangePointsAfter(self, after, until=None, nodes=None):
        changepoints = set()
        res = self.getReservationsWithChangePointsAfter(after)
        for rr in res:
            if nodes == None or (nodes != None and len(set(rr.resources_in_pnode.keys()) & set(nodes)) > 0):
                if rr.start > after:
                    changepoints.add(rr.start)
                if rr.end > after:
                    changepoints.add(rr.end)
        changepoints = list(changepoints)
        if until != None:
            changepoints =  [c for c in changepoints if c < until]
        changepoints.sort()
        return changepoints
    
    def peekNextChangePoint(self, time):
        if self.changepointcache == None:
            # Cache is empty
            changepoints = self.findChangePointsAfter(time)
            changepoints.reverse()
            self.changepointcache = changepoints
        if len(self.changepointcache) == 0:
            return None
        else:
            return self.changepointcache[-1]
    
    def getNextChangePoint(self, time):
        p = self.peekNextChangePoint(time)
        if p != None:
            self.changepointcache.pop()
        return p
    
    def fitExact(self, leasereq, preemptible=False, canpreempt=True, avoidpreempt=True):
        lease_id = leasereq.id
        start = leasereq.start.requested
        end = leasereq.start.requested + leasereq.duration.requested
        diskImageID = leasereq.diskimage_id
        numnodes = leasereq.numnodes
        resreq = leasereq.requested_resources

        self.availabilitywindow.initWindow(start, resreq, canpreempt=canpreempt)
        self.availabilitywindow.printContents(withpreemption = False)
        self.availabilitywindow.printContents(withpreemption = True)

        mustpreempt = False
        unfeasiblewithoutpreemption = False
        
        fitatstart = self.availabilitywindow.fitAtStart(canpreempt = False)
        if fitatstart < numnodes:
            if not canpreempt:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                unfeasiblewithoutpreemption = True
        feasibleend, canfitnopreempt = self.availabilitywindow.findPhysNodesForVMs(numnodes, end, strictend=True, canpreempt = False)
        fitatend = sum([n for n in canfitnopreempt.values()])
        if fitatend < numnodes:
            if not canpreempt:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                unfeasiblewithoutpreemption = True

        canfitpreempt = None
        if canpreempt:
            fitatstart = self.availabilitywindow.fitAtStart(canpreempt = True)
            if fitatstart < numnodes:
                raise SlotFittingException, "Not enough resources in specified interval"
            feasibleendpreempt, canfitpreempt = self.availabilitywindow.findPhysNodesForVMs(numnodes, end, strictend=True, canpreempt = True)
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

        orderednodes = self.prioritizenodes(canfit, diskImageID, start, canpreempt, avoidpreempt)
            
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
            raise CriticalSlotFittingException, "Availability window indicated that request but feasible, but could not fit it"

        return nodeassignment, res, preemptions


    def findLeasesToPreempt(self, mustpreempt, startTime, endTime):
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
        
        reservationsAtStart = self.getReservationsAt(startTime)
        reservationsAtStart = [r for r in reservationsAtStart if r.is_preemptible()
                        and len(set(r.resources_in_pnode.keys()) & nodes)>0]
        
        reservationsAtMiddle = self.getReservationsStartingBetween(startTime, endTime)
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


    def fitBestEffort(self, lease, earliest, canreserve, suspendable, canmigrate, mustresume):
        lease_id = lease.id
        remdur = lease.duration.get_remaining_duration()
        numnodes = lease.numnodes
        resreq = lease.requested_resources
        preemptible = lease.preemptible
        suspendresumerate = self.resourcepool.info.get_suspendresume_rate()

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
            # TODO: Get bandwidth another way. Right now, the
            # image node bandwidth is the same as the bandwidt
            # in the other nodes, but this won't always be true.
            migratetime = lease.estimate_migration_time(self.rm.scheduler.resourcepool.info.get_bandwidth())
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
            futurecp = self.findChangePointsAfter(changepoints[-1][0])
            futurecp = [(p,None) for p in futurecp]
        else:
            futurecp = []



        #
        # STEP 3: SLOT FITTING
        #

        # First, assuming we can't make reservations in the future
        start, end, canfit, mustsuspend = self.fitBestEffortInChangepoints(changepoints, numnodes, resreq, remdur, suspendable, suspendthreshold)

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
            start, end, canfit, mustsuspend = self.fitBestEffortInChangepoints(futurecp, numnodes, resreq, remdur, suspendable, suspendthreshold)

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

    def fitBestEffortInChangepoints(self, changepoints, numnodes, resreq, remdur, suspendable, suspendthreshold):
        start = None
        end = None
        canfit = None
        mustsuspend = None

        for p in changepoints:
            self.availabilitywindow.initWindow(p[0], resreq, p[1], canpreempt = False)
            self.availabilitywindow.printContents()
            
            if self.availabilitywindow.fitAtStart() >= numnodes:
                start=p[0]
                maxend = start + remdur
                end, canfit = self.availabilitywindow.findPhysNodesForVMs(numnodes, maxend)
        
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

    def suspend(self, lease, time):
        suspendresumerate = self.resourcepool.info.get_suspendresume_rate()
        
        (vmrr, susprr) = lease.get_last_vmrr()
        vmrrnew = copy.copy(vmrr)
        
        suspendtime = lease.estimate_suspend_resume_time(suspendresumerate)
        vmrrnew.end = time - suspendtime
            
        vmrrnew.oncomplete = constants.ONCOMPLETE_SUSPEND

        self.updateReservationWithKeyChange(vmrr, vmrrnew)
       
        if susprr != None:
            lease.remove_rr(susprr)
            self.removeReservation(susprr)
        
        mappings = vmrr.nodes
        suspres = {}
        for n in mappings.values():
            r = ds.ResourceTuple.create_empty()
            r.set_by_type(constants.RES_MEM, vmrr.resources_in_pnode[n].get_by_type(constants.RES_MEM))
            r.set_by_type(constants.RES_DISK, vmrr.resources_in_pnode[n].get_by_type(constants.RES_DISK))
            suspres[n] = r
        
        newsusprr = ds.SuspensionResourceReservation(lease, time - suspendtime, time, suspres, mappings)
        newsusprr.state = constants.RES_STATE_SCHEDULED
        lease.append_rr(newsusprr)
        self.addReservation(newsusprr)
        

    def slideback(self, lease, earliest):
        (vmrr, susprr) = lease.get_last_vmrr()
        vmrrnew = copy.copy(vmrr)
        nodes = vmrrnew.nodes.values()
        if lease.state == constants.LEASE_STATE_SUSPENDED:
            resmrr = lease.prev_rr(vmrr)
            originalstart = resmrr.start
        else:
            resmrr = None
            originalstart = vmrrnew.start
        cp = self.findChangePointsAfter(after=earliest, until=originalstart, nodes=nodes)
        cp = [earliest] + cp
        newstart = None
        for p in cp:
            self.availabilitywindow.initWindow(p, lease.requested_resources, canpreempt=False)
            self.availabilitywindow.printContents()
            if self.availabilitywindow.fitAtStart(nodes=nodes) >= lease.numnodes:
                (end, canfit) = self.availabilitywindow.findPhysNodesForVMs(lease.numnodes, originalstart)
                if end == originalstart and set(nodes) <= set(canfit.keys()):
                    self.logger.debug("Can slide back to %s" % p)
                    newstart = p
                    break
        if newstart == None:
            # Can't slide back. Leave as is.
            pass
        else:
            diff = originalstart - newstart
            if resmrr != None:
                resmrrnew = copy.copy(resmrr)
                resmrrnew.start -= diff
                resmrrnew.end -= diff
                self.updateReservationWithKeyChange(resmrr, resmrrnew)
            vmrrnew.start -= diff
            
            # If the lease was going to be suspended, check to see if
            # we don't need to suspend any more.
            remdur = lease.duration.get_remaining_duration()
            if susprr != None and vmrrnew.end - newstart >= remdur: 
                vmrrnew.end = vmrrnew.start + remdur
                vmrrnew.oncomplete = constants.ONCOMPLETE_ENDLEASE
                lease.remove_rr(susprr)
                self.removeReservation(susprr)
            else:
                vmrrnew.end -= diff
            # ONLY for simulation
            if vmrrnew.prematureend != None:
                vmrrnew.prematureend -= diff
            self.updateReservationWithKeyChange(vmrr, vmrrnew)
            self.dirty()
            self.logger.vdebug("New lease descriptor (after slideback):")
            lease.print_contents()


    def prioritizenodes(self, canfit, diskImageID, start, canpreempt, avoidpreempt):
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
        
    def isFull(self, time):
        nodes = self.getAvailability(time)
        avail = sum([node.capacity.get_by_type(constants.RES_CPU) for node in nodes.values()])
        return (avail == 0)
    


class AvailEntry(object):
    def __init__(self, time, avail, availpreempt, resreq):
        self.time = time
        self.avail = avail
        self.availpreempt = availpreempt
        
        if avail == None and availpreempt == None:
            self.canfit = 0
            self.canfitpreempt = 0
        else:
            self.canfit = resreq.get_num_fits_in(avail)
            if availpreempt == None:
                self.canfitpreempt = 0
            else:
                self.canfitpreempt = resreq.get_num_fits_in(availpreempt)
        
    def getCanfit(self, canpreempt):
        if canpreempt:
            return self.canfitpreempt
        else:
            return self.canfit


class AvailabilityWindow(object):
    def __init__(self, slottable):
        self.slottable = slottable
        self.logger = logging.getLogger("SLOTTABLE.WIN")
        self.time = None
        self.resreq = None
        self.onlynodes = None
        self.avail = None
        
    # Create avail structure
    def initWindow(self, time, resreq, onlynodes = None, canpreempt=False):
        self.time = time
        self.resreq = resreq
        self.onlynodes = onlynodes

        self.avail = {}

        # Availability at initial time
        availatstart = self.slottable.getAvailability(self.time, self.resreq, self.onlynodes)

        for node in availatstart:
            capacity = availatstart[node].capacity
            if canpreempt:
                capacitywithpreemption = availatstart[node].capacitywithpreemption
            else:
                capacitywithpreemption = None
            self.avail[node] = [AvailEntry(self.time, capacity, capacitywithpreemption, self.resreq)]
        
        # Determine the availability at the subsequent change points
        nodes = set(availatstart.keys())
        changepoints = self.slottable.findChangePointsAfter(self.time, nodes=self.avail.keys())
        for p in changepoints:
            availatpoint = self.slottable.getAvailability(p, self.resreq, nodes)
            newnodes = set(availatpoint.keys())
            
            # Add entries for nodes that have no resources available
            # (for, at least, one VM)
            fullnodes = nodes - newnodes
            for node in fullnodes:
                self.avail[node].append(AvailEntry(p, None, None, None))
                nodes.remove(node)
                
            # For the rest, only interested if the available resources
            # Decrease in the window
            for node in newnodes:
                capacity = availatpoint[node].capacity
                fits = self.resreq.get_num_fits_in(capacity)
                if canpreempt:
                    capacitywithpreemption = availatpoint[node].capacitywithpreemption
                    fitswithpreemption = self.resreq.get_num_fits_in(capacitywithpreemption)
                prevavail = self.avail[node][-1]
                if not canpreempt and prevavail.getCanfit(canpreempt=False) > fits:
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                elif canpreempt and (prevavail.getCanfit(canpreempt=False) > fits or prevavail.getCanfit(canpreempt=True) > fitswithpreemption):
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                  
    
    def fitAtStart(self, nodes = None, canpreempt = False):
        if nodes != None:
            avail = [v for (k, v) in self.avail.items() if k in nodes]
        else:
            avail = self.avail.values()
        if canpreempt:
            return sum([e[0].canfitpreempt for e in avail])
        else:
            return sum([e[0].canfit for e in avail])
        
    # TODO: Also return the amount of resources that would have to be
    # preempted in each physnode
    def findPhysNodesForVMs(self, numnodes, maxend, strictend=False, canpreempt=False):
        # Returns the physical nodes that can run all VMs, and the
        # time at which the VMs must end
        canfit = dict([(n, v[0].getCanfit(canpreempt)) for (n, v) in self.avail.items()])
        entries = []
        for n in self.avail.keys():
            entries += [(n, e) for e in self.avail[n][1:]]
        getTime = lambda x: x[1].time
        entries.sort(key=getTime)
        if strictend:
            end = None
        else:
            end = maxend
        for e in entries:
            physnode = e[0]
            entry = e[1]
       
            if entry.time >= maxend:
                # Can run to its maximum duration
                break
            else:
                diff = canfit[physnode] - entry.getCanfit(canpreempt)
                totalcanfit = sum([n for n in canfit.values()]) - diff
                if totalcanfit < numnodes and not strictend:
                    # Not enough resources. Must end here
                    end = entry.time
                    break
                else:
                    # Update canfit
                    canfit[physnode] = entry.getCanfit(canpreempt)

        # Filter out nodes where we can't fit any vms
        canfit = dict([(n, v) for (n, v) in canfit.items() if v > 0])
        
        return end, canfit
            
                    
    def printContents(self, nodes = None, withpreemption = False):
        if self.logger.getEffectiveLevel() == constants.LOGLEVEL_VDEBUG:
            if nodes == None:
                physnodes = self.avail.keys()
            else:
                physnodes = [k for k in self.avail.keys() if k in nodes]
            physnodes.sort()
            if withpreemption:
                p = "(with preemption)"
            else:
                p = "(without preemption)"
            self.logger.vdebug("AVAILABILITY WINDOW (time=%s, nodes=%s) %s"%(self.time, nodes, p))
            for n in physnodes:
                contents = "Node %i --- " % n
                for x in self.avail[n]:
                    contents += "[ %s " % x.time
                    contents += "{ "
                    if x.avail == None and x.availpreempt == None:
                        contents += "END "
                    else:
                        if withpreemption:
                            res = x.availpreempt
                            canfit = x.canfitpreempt
                        else:
                            res = x.avail
                            canfit = x.canfit
                        contents += "%s" % res
                    contents += "} (Fits: %i) ]  " % canfit
                self.logger.vdebug(contents)
                

                
                          
                          
            


        
        