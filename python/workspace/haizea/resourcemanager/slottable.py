from sets import Set
from mx.DateTime import ISO, TimeDelta
from operator import attrgetter, itemgetter
import workspace.haizea.common.constants as constants
import workspace.haizea.resourcemanager.datastruct as ds
from workspace.haizea.common.log import info, debug, warning, edebug
import workspace.haizea.common.log as log
from workspace.haizea.common.utils import roundDateTimeDelta
import bisect

class SlotFittingException(Exception):
    pass

class CriticalSlotFittingException(Exception):
    pass


class Node(object):
    def __init__(self, capacity, capacitywithpreemption):
        self.capacity = ds.ResourceTuple.copy(capacity)
        self.capacitywithpreemption = ds.ResourceTuple.copy(capacitywithpreemption)

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
            nodelist.add(Node(n.capacity, n.capacitywithpreemption))
        return nodelist

    def toPairList(self, onlynodes=None):
        nodelist = []
        for i,n in enumerate(self.nodelist):
            if onlynodes == None or (onlynodes != None and i+1 in onlynodes):
                nodelist.append((i+1,Node(n.capacity, n.capacitywithpreemption)))
        return nodelist
    
    def toDict(self):
        nodelist = self.copy()
        return dict([(i+1,v) for i,v in enumerate(nodelist)])
        
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
        self.nodes = NodeList()
        self.reservations = []
        self.reservationsByStart = []
        self.reservationsByEnd = []
        self.availabilitycache = {}
        self.changepointcache = None
            
        numnodes = self.rm.config.getNumPhysicalNodes()
        resources = self.rm.config.getResourcesPerPhysNode()
        bandwidth = self.rm.config.getBandwidth()
        
        capacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity[constants.str_res(resourcename)] = int(resourcecapacity)
        capacity = ds.ResourceTuple.fromList(capacity)
        # Create nodes
        for n in range(numnodes):
            self.nodes.add(Node(capacity, capacity))
                
        # Create image nodes
        imgcapacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        imgcapacity[constants.RES_CPU]=0
        imgcapacity[constants.RES_MEM]=0
        imgcapacity[constants.RES_NETIN]=0
        imgcapacity[constants.RES_NETOUT]=bandwidth
        imgcapacity[constants.RES_DISK]=0
        imgcapacity = ds.ResourceTuple.fromList(imgcapacity)
        self.nodes.add(Node(imgcapacity, imgcapacity))
        self.FIFOnode = numnodes + 1
        self.nodes.add(Node(imgcapacity, imgcapacity))
        self.EDFnode = numnodes + 2
        
        self.availabilitywindow = AvailabilityWindow(self)

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
            for node in r.res:
                nodes[node].capacity.decr(r.res[node])
                if not r.isPreemptible():
                    nodes[node].capacitywithpreemption.decr(r.res[node])                        
            
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
            for i,node in nodes:
                if not resreq.fitsIn(node.capacity) and not resreq.fitsIn(node.capacitywithpreemption):
                    pass
                else:
                    newnodes.append((i,node))
            nodes = newnodes
        
        return dict(nodes)
    
    def getUtilization(self, time, restype=constants.RES_CPU):
        nodes = self.getAvailability(time)
        total = sum([n.capacity.get(restype) for n in self.nodes.nodelist])
        avail = sum([n.capacity.get(restype) for n in nodes.values()])
        return 1.0 - (float(avail)/total)
            
#    def getReservationsAt(self, time):
#        res = [rr for rr in self.reservations if rr.start <= time and rr.end > time]
#        return res
#    
#    def getReservationsStartingBetween(self, start, end):
#        res = [rr for rr in self.reservations if rr.start >= start and rr.start < end]
#        return res    
#    
#    def addReservation(self, rr):
#        self.reservations.append(rr)
#        self.dirty()
#
#    def removeReservation(self, rr):
#        self.reservations.remove(rr)
#        self.dirty()
#
#    def getReservationsWithChangePointsAfter(self, after, includereal):
#        if includereal:
#            res = [r for r in self.reservations if r.start > after or r.end > after or r.realend > after]
#        else:
#            res = [r for r in self.reservations  if r.start > after or r.end > after]
#        return res

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
    
    def getReservationsWithChangePointsAfter(self, after, includereal):
        if includereal:
            # Inefficient, but ok since this query seldom happens
            res = [i.value for i in self.reservationsByStart if i.value.start > after or i.value.end > after or i.value.realend > after]
            return res
        else:
            item = KeyValueWrapper(after, None)
            startpos = bisect.bisect_right(self.reservationsByStart, item)
            bystart = set([x.value for x in self.reservationsByStart[:startpos]])
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

    def updateReservation(self, rr):
        # Remove and reinsert in order
        # TODO: Might be more efficient to resort lists
        self.removeReservation(rr)
        self.addReservation(rr)
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

    def removeReservation(self, rr):
        posstart = self.getIndexOfReservation(self.reservationsByStart, rr, rr.start)
        posend = self.getIndexOfReservation(self.reservationsByEnd, rr, rr.end)
        self.reservationsByStart.pop(posstart)
        self.reservationsByEnd.pop(posend)
        self.dirty()

    
    def findChangePointsAfter(self, after, until=None, includereal = False, nodes=None):
        changepoints = set()
        res = self.getReservationsWithChangePointsAfter(after, includereal)
        for rr in res:
            if nodes == None or (nodes != None and len(set(rr.res.keys()) & set(nodes)) > 0):
                if rr.start > after:
                    changepoints.add(rr.start)
                if rr.end > after:
                    changepoints.add(rr.end)
                if includereal and rr.realend > after:
                    changepoints.add(rr.realend)
        changepoints = list(changepoints)
        if until != None:
            changepoints =  [c for c in changepoints if c < until]
        changepoints.sort()
        return changepoints
    
    def peekNextChangePoint(self, time):
        if self.changepointcache == None:
            # Cache is empty
            changepoints = self.findChangePointsAfter(time, includereal = True)
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
    
    def fitExact(self, leasereq, preemptible=False, canpreempt=True):
        leaseID = leasereq.leaseID
        start = leasereq.start
        end = leasereq.end
        vmimage = leasereq.vmimage
        numnodes = leasereq.numnodes
        resreq = leasereq.resreq
        prematureend = leasereq.prematureend

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
            info("The VM reservations for this lease are feasible without preemption.", constants.ST, self.rm.time)
        else:
            info("The VM reservations for this lease are feasible but will require preemption.", constants.ST, self.rm.time)

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

        orderednodes = self.prioritizenodes(canfit, vmimage, start, canpreempt)
            
        info("Node ordering: %s" % orderednodes, constants.ST, self.rm.time)
        
        # vnode -> pnode
        nodeassignment = {}
        
        # pnode -> resourcetuple
        res = {}
        
        # physnode -> how many vnodes
        preemptions = {}
        
        # First pass, without preemption
        vnode = 1
        for physnode in orderednodes:
            canfitinnode = canfit[physnode][0]
            for i in range(1, canfitinnode+1):
                nodeassignment[vnode] = physnode
                res[physnode] = resreq
                canfit[physnode][0] -= 1
                canfit[physnode][1] -= 1
                vnode += 1
                if vnode > numnodes:
                    break
            if vnode > numnodes:
                break
            
        # Second pass, with preemption
        if mustpreempt:
            for physnode in orderednodes:
                canfitinnode = canfit[physnode][1]
                for i in range(1, canfitinnode+1):
                    nodeassignment[vnode] = physnode
                    res[physnode] = resreq
                    canfit[physnode][1] -= 1
                    vnode += 1
                    if preemptions.has_key(physnode):
                        preemptions[physnode].incr(resreq)
                    else:
                        preemptions[physnode] = ds.ResourceTuple.copy(resreq)
                    if vnode > numnodes:
                        break
                if vnode > numnodes:
                    break

        if vnode <= numnodes:
            raise CriticalSlotFittingException, "Availability window indicated that request but feasible, but could not fit it"

        return nodeassignment, res, preemptions


    def findLeasesToPreempt(self, mustpreempt, startTime, endTime):
        def comparepreemptability(rrX,rrY):
            if rrX.lease.tSubmit > rrY.lease.tSubmit:
                return constants.BETTER
            elif rrX.lease.tSubmit < rrY.lease.tSubmit:
                return constants.WORSE
            else:
                return constants.EQUAL        
            
        def preemptedEnough(amountToPreempt):
            for node in amountToPreempt:
                if not amountToPreempt[node].isZeroOrLess():
                    return False
            return True
        
        # Get allocations at the specified time
        atstart = set()
        atmiddle = set()
        nodes = set(mustpreempt.keys())
        
        reservationsAtStart = self.getReservationsAt(startTime)
        reservationsAtStart = [r for r in reservationsAtStart if r.isPreemptible()
                        and len(set(r.res.keys()) & nodes)>0]
        
        reservationsAtMiddle = self.getReservationsStartingBetween(startTime, endTime)
        reservationsAtMiddle = [r for r in reservationsAtMiddle if r.isPreemptible()
                        and len(set(r.res.keys()) & nodes)>0]
        
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
            for n in r.res.keys():
                # Don't need to preempt if we've already preempted all
                # the needed resources in node n
                if amountToPreempt.has_key(n) and not amountToPreempt[n].isZeroOrLess():
                    amountToPreempt[n].decr(r.res[n])
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
                    for n in r.res.keys():
                        if amountToPreempt.has_key(n) and not amountToPreempt[n].isZeroOrLess():
                            amountToPreempt[n].decr(r.res[n])
                            mustpreemptres = True
                    if mustpreemptres:
                        atmiddle.add(r)
                    if preemptedEnough(amountToPreempt):
                        break
            
        info("Preempting leases (at start of reservation): %s" % [r.lease.leaseID for r in atstart], constants.ST, None)
        info("Preempting leases (in middle of reservation): %s" % [r.lease.leaseID for r in atmiddle], constants.ST, None)
        
        leases = [r.lease for r in atstart|atmiddle]
        
        return leases


    def fitBestEffort(self, lease, earliest, canreserve, suspendable, preemptible, canmigrate, mustresume):
        leaseID = lease.leaseID
        remdur = lease.remdur
        numnodes = lease.numnodes
        resreq = lease.resreq
        realdur = lease.realremdur


        #
        # STEP 1: TAKE INTO ACCOUNT VM RESUMPTION (IF ANY)
        #
        
        curnodes=None
        # If we can't migrate, we have to stay in the
        # nodes where the lease is currently deployed
        if mustresume and not canmigrate:
            vmrr, susprr = lease.getLastVMRR()
            curnodes = set(vmrr.nodes.values())
        
        if mustresume and canmigrate:
            # If we have to resume this lease, make sure that
            # we have enough time to transfer the images.
            whattomigrate = self.rm.config.getMustMigrate()
            if whattomigrate != constants.MIGRATE_NONE:
                if whattomigrate == constants.MIGRATE_MEM:
                    mbtotransfer = resreq.res[constants.RES_MEM]
                elif whattomigrate == constants.MIGRATE_MEMVM:
                    mbtotransfer = lease.vmimagesize + resreq.res[constants.RES_MEM]
            
                migratetime = float(mbtotransfer) / self.rm.config.getBandwidth()
                migratetime = roundDateTimeDelta(TimeDelta(seconds=migratetime))
                earliesttransfer = self.rm.time + migratetime

                for n in earliest:
                    earliest[n][0] = max(earliest[n][0],earliesttransfer)
                    
        if mustresume:
            resumerate = self.rm.config.getSuspendResumeRate()
            resumetime = float(resreq.res[constants.RES_MEM]) / resumerate
            resumetime = roundDateTimeDelta(TimeDelta(seconds = resumetime))
            # Must allocate time for resumption too
            remdur += resumetime
            realdur += resumetime


        #
        # STEP 2: FIND THE CHANGEPOINTS
        #

        # Find the changepoints, and the nodes we can use at each changepoint
        # Nodes may not be available at a changepoint because images
        # cannot be transferred at that time.
        if not mustresume:
            cps = [(node,e[0]) for node,e in earliest.items()]
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
        start, end, realend, canfit, mustsuspend = self.fitBestEffortInChangepoints(changepoints, numnodes, resreq, remdur, realdur, suspendable)

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
            start, end, realend, canfit, mustsuspend = self.fitBestEffortInChangepoints(futurecp, numnodes, resreq, remdur, realdur, suspendable)

        if mustsuspend and not suspendable:
            raise SlotFittingException, "Scheduling this lease would require preempting it, which is not allowed"

        if start in [p[0] for p in futurecp]:
            reservation = True
        else:
            reservation = False

        if realend > end:
            realend = end



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
            vmrr, susprr = lease.getLastVMRR()
            nodes = set(vmrr.nodes.values())
            availnodes = set(physnodes)
            deplnodes = availnodes.intersection(nodes)
            notdeplnodes = availnodes.difference(nodes)
            physnodes = list(deplnodes) + list(notdeplnodes)
        else:
            physnodes.sort() # Arbitrary, prioritize nodes, as in exact

        # Adjust times in case the lease has to be suspended/resumed
        if mustsuspend:
            suspendtime = self.getSuspendTime(resreq.res[constants.RES_MEM])
            if end != realend:
                end -= suspendtime
            else:
                end -= suspendtime
                realend -= suspendtime
                
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
                    res[n] = resreq
                    vmnode += 1
                    break



        #
        # STEP 5: CREATE RESOURCE RESERVATIONS
        #
        
        if mustresume:
            resmres = {}
            for n in mappings.values():
                r = [None, None, None, None, None]  # TODO: Hardcoding == bad
                r[constants.RES_CPU] = 0
                r[constants.RES_MEM] = resreq.res[constants.RES_MEM]
                r[constants.RES_NETIN] = 0
                r[constants.RES_NETOUT] = 0
                r[constants.RES_DISK] = resreq.res[constants.RES_DISK]
                resmres[n] = ds.ResourceTuple.fromList(r)
            resmrr = ds.ResumptionResourceReservation(lease, start-resumetime, start, resmres, mappings)
            resmrr.state = constants.RES_STATE_SCHEDULED
        else:
            resmrr = None
        if mustsuspend:
            suspres = {}
            for n in mappings.values():
                r = [None, None, None, None, None]  # TODO: Hardcoding == bad
                r[constants.RES_CPU] = 0
                r[constants.RES_MEM] = resreq.res[constants.RES_MEM]
                r[constants.RES_NETIN] = 0
                r[constants.RES_NETOUT] = 0
                r[constants.RES_DISK] = resreq.res[constants.RES_DISK]
                suspres[n] = ds.ResourceTuple.fromList(r)
            susprr = ds.SuspensionResourceReservation(lease, end, end + suspendtime, suspres, mappings)
            susprr.state = constants.RES_STATE_SCHEDULED
            oncomplete = constants.ONCOMPLETE_SUSPEND
        else:
            susprr = None
            oncomplete = constants.ONCOMPLETE_ENDLEASE

        vmrr = ds.VMResourceReservation(lease, start, end, realend, mappings, res, oncomplete, reservation)
        vmrr.state = constants.RES_STATE_SCHEDULED

        return resmrr, vmrr, susprr, reservation

    def fitBestEffortInChangepoints(self, changepoints, numnodes, resreq, remdur, realdur, suspendable):
        start = None
        end = None
        realend = None
        canfit = None
        mustsuspend = None
        suspendthreshold = self.rm.config.getSuspendThreshold()

        for p in changepoints:
            self.availabilitywindow.initWindow(p[0], resreq, p[1], canpreempt = False)
            self.availabilitywindow.printContents()
            
            if self.availabilitywindow.fitAtStart() >= numnodes:
                start=p[0]
                maxend = start + remdur
                realend = start + realdur
                end, canfit = self.availabilitywindow.findPhysNodesForVMs(numnodes, maxend)
        
                info("This lease can be scheduled from %s to %s" % (start, end), constants.ST, self.rm.time)
                
                if end < maxend:
                    mustsuspend=True
                    info("This lease will require suspension (maxend = %s)" % (maxend), constants.ST, self.rm.time)
                    
                    if suspendable:
                        # It the lease is suspendable...
                        if suspendthreshold != None:
                            if end-start > suspendthreshold:
                                break
                            else:
                                info("This starting time does not meet the suspend threshold (%s < %s)" % (end-start, suspendthreshold), constants.ST, self.rm.time)
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
                
        return start, end, realend, canfit, mustsuspend

    def suspend(self, lease, time):
        (vmrr, susprr) = lease.getLastVMRR()
        
        suspendtime = self.getSuspendTime(lease.resreq.res[constants.RES_MEM])
        if vmrr.end != vmrr.realend:
            vmrr.end = time - suspendtime
        else:
            vmrr.end = time - suspendtime
            vmrr.realend = vmrr.end
            
        vmrr.oncomplete = constants.ONCOMPLETE_SUSPEND
        
        self.updateReservation(vmrr)
       
        if susprr != None:
            lease.removeRR(susprr)
            self.removeReservation(susprr)
        
        mappings = vmrr.nodes
        suspres = {}
        for n in mappings.values():
            r = [None, None, None, None, None] # TODO: Hardcoding == bad
            r[constants.RES_CPU] = 0
            r[constants.RES_MEM] = vmrr.res[n].res[constants.RES_MEM]
            r[constants.RES_NETIN] = 0
            r[constants.RES_NETOUT] = 0
            r[constants.RES_DISK] = vmrr.res[n].res[constants.RES_DISK]
            suspres[n] = ds.ResourceTuple.fromList(r)
        
        newsusprr = ds.SuspensionResourceReservation(lease, time - suspendtime, time, suspres, mappings)
        newsusprr.state = constants.RES_STATE_SCHEDULED
        lease.appendRR(newsusprr)
        self.addReservation(newsusprr)
            
    def getSuspendTime(self, memsize):
        suspendrate = self.rm.config.getSuspendResumeRate()
        suspendtime = float(memsize) / suspendrate
        suspendtime = roundDateTimeDelta(TimeDelta(seconds = suspendtime))
        return suspendtime



    def slideback(self, lease, earliest):
        (vmrr, susprr) = lease.getLastVMRR()
        nodes = vmrr.nodes.values()
        if lease.state == constants.LEASE_STATE_SUSPENDED:
            resmrr = lease.prevRR(vmrr)
            originalstart = resmrr.start
        else:
            resmrr = None
            originalstart = vmrr.start
        cp = self.findChangePointsAfter(after=earliest, until=originalstart, nodes=nodes)
        cp = [earliest] + cp
        newstart = None
        for p in cp:
            self.availabilitywindow.initWindow(p, lease.resreq, canpreempt=False)
            self.availabilitywindow.printContents()
            if self.availabilitywindow.fitAtStart(nodes=nodes) >= lease.numnodes:
                (end, canfit) = self.availabilitywindow.findPhysNodesForVMs(lease.numnodes, originalstart)
                if end == originalstart and set(nodes) <= set(canfit.keys()):
                    info("Can slide back to %s" % p, constants.ST, self.rm.time)
                    newstart = p
                    break
        if newstart == None:
            # Can't slide back. Leave as is.
            pass
        else:
            diff = originalstart - newstart
            if resmrr != None:
                resmrr.start -= diff
                resmrr.end -= diff
            vmrr.start -= diff
            if susprr != None:
                # This lease was going to be suspended. Determine if
                # we still want to use some of the extra time.
                if vmrr.end - newstart < lease.remdur:
                    # We still need to run until the end, and suspend there
                    # Don't change the end time or the suspend RR
                    if newstart + lease.realremdur < vmrr.end:
                        vmrr.realend = newstart + lease.realremdur
                else:
                    # No need to suspend any more.
                    vmrr.end -= diff
                    vmrr.realend -= diff
                    vmrr.oncomplete = constants.ONCOMPLETE_ENDLEASE
                    lease.removeRR(susprr)
                    self.removeReservation(susprr)
            else:
                vmrr.end -= diff
                vmrr.realend -= diff
            self.dirty()
            edebug("New lease descriptor (after slideback):", constants.ST, self.rm.time)
            lease.printContents()


    def prioritizenodes(self,canfit, vmimage,start,canpreempt):
        # TODO2: Choose appropriate prioritizing function based on a
        # config file, instead of hardcoding it)
        #
        # TODO3: Basing decisions only on CPU allocations. This is ok for now,
        # since the memory allocation is proportional to the CPU allocation.
        # Later on we need to come up with some sort of weighed average.
        
        nodes = canfit.keys()
        
        reusealg = self.rm.config.getReuseAlg()
        nodeswithimg=[]
        if reusealg==constants.REUSE_COWPOOL:
            nodeswithimg = self.rm.enactment.getNodesWithImgInPool(vmimage, start)

        # Compares node x and node y. 
        # Returns "x is ??? than y" (???=BETTER/WORSE/EQUAL)
        def comparenodes(x,y):
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
        
        # Order nodes
        nodes.sort(comparenodes)
        return nodes
        
    def isFull(self, time):
        nodes = self.getAvailability(time)
        avail = sum([node.capacity.res[constants.RES_CPU] for node in nodes.values()])
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
            self.canfit = resreq.getNumFitsIn(avail)
            if availpreempt == None:
                self.canfitpreempt = 0
            else:
                self.canfitpreempt = resreq.getNumFitsIn(availpreempt)
        
    def getCanfit(self, canpreempt):
        if canpreempt:
            return self.canfitpreempt
        else:
            return self.canfit


class AvailabilityWindow(object):
    def __init__(self, slottable):
        self.slottable = slottable
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
            self.avail[node] = [AvailEntry(self.time,capacity,capacitywithpreemption, self.resreq)]
        
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
                fits = self.resreq.getNumFitsIn(capacity)
                if canpreempt:
                    capacitywithpreemption = availatpoint[node].capacitywithpreemption
                    fitswithpreemption = self.resreq.getNumFitsIn(capacitywithpreemption)
                prevavail = self.avail[node][-1]
                if not canpreempt and prevavail.getCanfit(canpreempt=False) > fits:
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                elif canpreempt and (prevavail.getCanfit(canpreempt=False) > fits or prevavail.getCanfit(canpreempt=True) > fitswithpreemption):
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                  
    
    def fitAtStart(self, nodes = None, canpreempt = False):
        if nodes != None:
            avail = [v for (k,v) in self.avail.items() if k in nodes]
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
        canfit = dict([(n, v[0].getCanfit(canpreempt)) for (n,v) in self.avail.items()])
        entries = []
        for n in self.avail.keys():
            entries += [(n,e) for e in self.avail[n][1:]]
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
        canfit = dict([(n,v) for (n,v) in canfit.items() if v > 0])
        
        return end, canfit
            
                    
    def printContents(self, nodes = None, withpreemption = False):
        if log.extremedebug:
            if nodes == None:
                physnodes = self.avail.keys()
            else:
                physnodes = [k for k in self.avail.keys() if k in nodes]
            physnodes.sort()
            if withpreemption:
                p = "(with preemption)"
            else:
                p = "(without preemption)"
            edebug("AVAILABILITY WINDOW (time=%s, nodes=%s) %s"%(self.time, nodes, p), constants.ST, None)
            for n in physnodes:
                contents = "Node %i --- " % n
                for x in self.avail[n]:
                    contents += "[ %s " % x.time
                    contents += "{ "
                    if x.avail == None and x.availpreempt == None:
                        contents += "END "
                    else:
                        if withpreemption:
                            res = x.availpreempt.res
                            canfit = x.canfitpreempt
                        else:
                            res = x.avail.res
                            canfit = x.canfit
                        for i,x in enumerate(res):
                            contents += "%s:%.2f " % (constants.res_str(i),x)
                    contents += "} (Fits: %i) ]  " % canfit
                edebug(contents, constants.ST, None)
                

                
                          
                          
            


        
        