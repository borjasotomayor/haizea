from sets import Set
from mx.DateTime import ISO, TimeDelta
from operator import attrgetter, itemgetter
import workspace.haizea.common.constants as constants
import workspace.haizea.resourcemanager.datastruct as ds
from workspace.haizea.common.log import info, debug, warning, edebug
from pysqlite2.dbapi2 import IntegrityError
from workspace.haizea.common.utils import roundDateTimeDelta
import copy

class SlotFittingException(Exception):
    pass

class CriticalSlotFittingException(Exception):
    pass


class Node(object):
    def __init__(self, capacity):
        self.capacity = ds.ResourceTuple(capacity)
        self.capacitywithpreemption = ds.ResourceTuple(capacity)
        



class SlotTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.rm = scheduler.rm
        self.nodes = {}
        self.reservations = []
        self.availabilitycache = {}
        self.changepointcache = None
            
        numnodes = self.rm.config.getNumPhysicalNodes()
        resources = self.rm.config.getResourcesPerPhysNode()
        bandwidth = self.rm.config.getBandwidth()
        
        capacity = {}
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity[constants.str_res(resourcename)] = float(resourcecapacity)
            
        # Create nodes
        for n in range(numnodes):
            self.nodes[n+1] = Node(capacity)
                
        # Create image nodes
        imgcapacity = { constants.RES_CPU:0, constants.RES_MEM:0, constants.RES_NETIN:0, constants.RES_NETOUT : bandwidth, constants.RES_DISK:0 }
        self.nodes[4200] = Node(imgcapacity)
        self.nodes[4201] = Node(imgcapacity)  
        
        self.availabilitywindow = AvailabilityWindow(self)

    def dirty(self):
        # You're a dirty, dirty slot table and you should be
        # ashamed of having outdated caches!
        self.availabilitycache = {}
        self.changepointcache = None

    def getAvailability(self, time, resreq=None, onlynodes=None):
        if not self.availabilitycache.has_key(time):
            # Cache miss
            nodes = copy.deepcopy(self.nodes)
            reservations = self.getReservationsAt(time)

            # Find how much resources are available on each node
            for r in reservations:
                for node in r.res:
                    nodes[node].capacity.decr(r.res[node])
                    if not r.isPreemptible():
                        nodes[node].capacitywithpreemption.decr(r.res[node])                        
                
            self.availabilitycache[time] = nodes

        nodes = self.availabilitycache[time]
        
        # Filter nodes
        if onlynodes != None:
            allnodes = set(nodes.keys())
            onlynodes = set(onlynodes)
            exclude = allnodes - onlynodes
            for n in exclude:
                del nodes[n]

        # Keep only those nodes with enough resources
        if resreq != None:
            exclude = []
            for n in nodes:
                if not resreq.fitsIn(nodes[n].capacity):
                    exclude.append(n)
            for n in exclude:
                del nodes[n]
                
        return nodes
        
    def getReservationsAt(self, time):
        res = [rr for rr in self.reservations if rr.start <= time and rr.end > time]
        return res
    
    def addReservation(self, rr):
        self.reservations.append(rr)
        self.dirty()

    def removeReservation(self, rr):
        self.reservations.remove(rr)
        self.dirty()
    
    def findChangePointsAfter(self, time, includereal = False, nodes=None):
        changepoints = set()
        if includereal:
            res = [rr for rr in self.reservations if rr.start > time or rr.end > time or rr.realend > time]
        else:
            res = [rr for rr in self.reservations if rr.start > time or rr.end > time]
        for rr in res:
            if rr.start > time:
                changepoints.add(rr.start)
            if rr.end > time:
                changepoints.add(rr.end)
            if includereal and rr.realend > time:
                changepoints.add(rr.realend)
        changepoints = list(changepoints)
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

        self.availabilitywindow.initWindow(start, resreq)
        self.availabilitywindow.printContents()

        mustpreempt = False
        feasibleend, canfitnopreempt = self.availabilitywindow.findPhysNodesForVMs(numnodes, end, canpreempt = False)
        if feasibleend != end:
            if not canpreempt:
                raise SlotFittingException, "Not enough resources in specified interval"
            else:
                unfeasiblewithoutpreemption = True
        else:
            unfeasiblewithoutpreemption = False
            
        canfitpreempt = None
        if canpreempt:
            feasibleendpreempt, canfitpreempt = self.availabilitywindow.findPhysNodesForVMs(numnodes, end, canpreempt = True)
            if feasibleendpreempt != end:
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
                        preemptions[physnode] += 1
                    else:
                        preemptions[physnode] = 1
                    if vnode > numnodes:
                        break
                if vnode > numnodes:
                    break

        if vnode <= numnodes:
            raise CriticalSlotFittingException, "Availability window indicated that request but feasible, but could not fit it"

        return nodeassignment, res, preemptions


    def findLeasesToPreempt(self, mustpreempt, startTime, endTime):
        def comparepreemptability(x,y):
            leaseIDX = x["RES_ID"]
            leaseIDY = y["RES_ID"]
            
            leaseX = self.scheduler.scheduledleases.getLease(leaseIDX)
            leaseY = self.scheduler.scheduledleases.getLease(leaseIDY)
            if leaseX.tSubmit > leaseY.tSubmit:
                return constants.BETTER
            elif leaseX.tSubmit < leaseY.tSubmit:
                return constants.WORSE
            else:
                return constants.EQUAL        
        
        # Get allocations at the specified time
        atstart = set()
        atmiddle = set()

        for node in mustpreempt.keys():
            preemptibleAtStart = {}
            preemptibleAtMiddle = {}
            cur = self.db.getCurrentAllocationsInNode(startTime, node, rsp_preemptible=True)
            cur = cur.fetchall()
            for alloc in cur:
                restype=alloc["slt_id"]
                # Make sure this allocation falls within the preemptible period
                start = ISO.ParseDateTime(alloc["all_schedstart"])
                end = ISO.ParseDateTime(alloc["all_schedend"])
                if start < startTime and end > startTime:
                    if not preemptibleAtStart.has_key(restype):
                        preemptibleAtStart[restype] = []
                    preemptibleAtStart[restype].append(alloc)
                elif start < endTime and end > startTime:
                    if not preemptibleAtMiddle.has_key(restype):
                        preemptibleAtMiddle[restype] = []
                    preemptibleAtMiddle[restype].append(alloc)

            # Reservation parts we will be preempting
            resparts = set()
            leasesStart = set()
            leasesMiddle = set()
            respartsinfo = {}

            # First step: CHOOSE RESOURCES TO PREEMPT AT START OF RESERVATION
            # These can potentially be already running, so we want to choose
            # the ones which will be least impacted by being preempted
            
            if len(preemptibleAtStart) > 0:
                # Order preemptible resources
                for restype in preemptibleAtStart.keys():
                    preemptibleAtStart[restype].sort(comparepreemptability)

                # Start marking resources for preemption, until we've preempted
                # all the resources db need.
                for restype in mustpreempt[node].keys():
                    amountToPreempt = mustpreempt[node][restype]
                    for alloc in preemptibleAtStart[restype]:
                        amount = alloc["all_amount"]
                        amountToPreempt -= amount
                        rsp_key = alloc["rsp_id"]
                        resparts.add(rsp_key)
                        leasesStart.add(alloc["res_id"])
                        respartsinfo[rsp_key] = alloc
                        if amountToPreempt <= 0:
                            break # Ugh
            
            # Second step: CHOOSE RESOURCES TO PREEMPT DURING RESERVATION
            # These cannot be running, so we greedily choose the largest resources
            # first, to minimize the number of preempted resources. This algorithm
            # could potentially also take into account the scheduled starting time
            # of the preempted resource (later is better)
            
            if len(preemptibleAtMiddle) > 0:
                # Find changepoints
                changepoints = Set()
                for restype in mustpreempt[node].keys():
                    for alloc in preemptibleAtMiddle[restype]:
                        start = ISO.ParseDateTime(alloc["all_schedstart"])
                        end = ISO.ParseDateTime(alloc["all_schedend"])
                        if start < endTime:
                            changepoints.add(start)
                        if end < endTime:
                            changepoints.add(end)
                        
                changepoints = list(changepoints)
                changepoints.sort()
                
                #print resparts
                
                # Go through changepoints and, at each point, make sure we have enough
                # resources
                for changepoint in changepoints:
                    #print changepoint
                    for restype in mustpreempt[node].keys():
                        # Find allocations in that changepoint
                        allocs = []
                        amountToPreempt = mustpreempt[node][restype]
                        preemptallocs = preemptibleAtMiddle[restype]
                        if len(preemptibleAtStart) > 0:
                            preemptallocs += preemptibleAtStart[restype]
                        for alloc in preemptallocs:
                            start = ISO.ParseDateTime(alloc["all_schedstart"])
                            end = ISO.ParseDateTime(alloc["all_schedend"])
                            rsp_id = alloc["rsp_id"]
                            # Only choose it if we have not already decided to preempt it
                            if start <= changepoint and changepoint < end:
                                #print rsp_id
                                if not rsp_id in resparts:
                                    allocs.append(alloc)
                                else:
                                    amountToPreempt -= alloc["all_amount"]
                        allocs.sort(comparepreemptability)
                        #print [v["rsp_id"] for v in allocs]
                        for alloc in allocs:
                            if amountToPreempt <= 0:
                                break # Ugh
                            amount = alloc["all_amount"]
                            amountToPreempt -= amount
                            rsp_key = alloc["rsp_id"]
                            #print rsp_key, amountToPreempt
                            resparts.add(rsp_key)
                            leasesMiddle.add(alloc["res_id"])
                            respartsinfo[rsp_key] = alloc

            atstart.update(leasesStart)
            atmiddle.update(leasesMiddle)
            
        info("Preempting leases (at start of reservation): %s" % atstart, constants.ST, None)
        info("Preempting leases (in middle of reservation): %s" % atmiddle, constants.ST, None)
        
        return list(atstart | atmiddle)


    def fitBestEffort(self, lease, earliest, canreserve, suspendable, preemptible, canmigrate, mustresume):
        leaseID = lease.leaseID
        remdur = lease.remdur
        numnodes = lease.numnodes
        resreq = lease.resreq
        realdur = lease.realremdur

        curnodes=None
        # If we can't migrate, we have to stay in the
        # nodes where the lease is currently deployed
        if mustresume and not canmigrate:
            vmrr, susprr = lease.getLastVMRR()
            curnodes = set(vmrr.nodes.values())

        start = None
        end = None
        
        if mustresume and canmigrate:
            # If we have to resume this lease, make sure that
            # we have enough time to transfer the images.
            whattomigrate = self.rm.config.getMustMigrate()
            if whattomigrate != constants.MIGRATE_NONE:
                if whattomigrate == constants.MIGRATE_MEM:
                    mbtotransfer = resreq[constants.RES_MEM]
                elif whattomigrate == constants.MIGRATE_MEMVM:
                    mbtotransfer = lease.vmimagesize + resreq[constants.RES_MEM]
            
                migratetime = float(mbtotransfer) / self.rm.config.getBandwidth()
                migratetime = roundDateTimeDelta(TimeDelta(seconds=migratetime))
                earliesttransfer = self.rm.time + migratetime

                for n in earliest:
                    earliest[n][0] = max(earliest[n][0],earliesttransfer)

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
        else:
            futurecp = []

        resumetime = None
        if mustresume:
            resumerate = self.rm.config.getSuspendResumeRate()
            resumetime = float(resreq[constants.RES_MEM]) / resumerate
            resumetime = roundDateTimeDelta(TimeDelta(seconds = resumetime))
            # Must allocate time for resumption too
            remdur += resumetime
            realdur += resumetime

        suspendthreshold = self.rm.config.getSuspendThreshold()

        first = changepoints[0]
        
        for p in changepoints:
            self.availabilitywindow.initWindow(p[0], resreq, p[1])
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

        if not canreserve:
            if start == None:
                # We did not find a suitable starting time. This can happen
                # if we're unable to make future reservations
                raise SlotFittingException, "Could not find enough resources for this request"
            elif mustsuspend and not suspendable:
                raise SlotFittingException, "Scheduling this lease would require preempting it, which is not allowed"

        if start != None and mustsuspend and not suspendable:
            start = None # No satisfactory start time
            
        # TODO Factor out common code in the above loop and the following one
        # TODO Better logging
        
        if start == None and canreserve:
            # Check future points
            for p in futurecp:
                self.availabilitywindow.initWindow(p, resreq)
                self.availabilitywindow.printContents()
                
                if self.availabilitywindow.fitAtStart() >= numnodes:
                    start=p
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
                        mustsuspend=False
                        # We've found a satisfactory starting time
                        break   

        if mustsuspend and not suspendable:
            raise SlotFittingException, "Scheduling this lease would require preempting it, which is not allowed"

        if start in futurecp:
            reservation = True
        else:
            reservation = False

        if realend > end:
            realend = end

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

        suspendtime = None
        if mustsuspend:
            suspendtime = self.getSuspendTime(resreq[constants.RES_MEM])
            if end != realend:
                end -= suspendtime
            else:
                end -= suspendtime
                realend -= suspendtime
                
        if mustresume:
            start += resumetime
        
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

        # TODO: Return the RRs, instead of all the spare data

        return start, end, realend, mappings, res, resumetime, suspendtime, reservation




    def suspend(self, lease, time):
        (vmrr, susprr) = lease.getLastVMRR()
        
        suspendtime = self.getSuspendTime(lease.resreq[constants.RES_MEM])
        if vmrr.end != vmrr.realend:
            vmrr.end = time - suspendtime
        else:
            vmrr.end = time - suspendtime
            vmrr.realend = vmrr.end
            
        vmrr.oncomplete = constants.ONCOMPLETE_SUSPEND
        
        self.db.updateReservationPartTimes(vmrr.db_rsp_ids, vmrr.start, vmrr.end, vmrr.realend)            
        
        if susprr != None:
            lease.removeRR(susprr)

        # New suspension allocations
        mappings = vmrr.nodes
        rsp_name = "SUSPEND %i" % lease.leaseID
        rsp_id = self.db.addReservationPart(lease.leaseID, rsp_name, 3, preemptible=False)
        for vmnode in mappings:
            physnode = mappings[vmnode]
            sl_id = self.availabilitywindow.slot_ids[physnode][constants.RES_MEM]
            self.db.addAllocation(rsp_id, sl_id, time - suspendtime, time, lease.resreq[constants.RES_MEM], realEndTime=time)
        self.commit()

        newsusprr = ds.SuspensionResourceReservation(lease, time - suspendtime, time, mappings, [rsp_id])
        newsusprr.state = constants.RES_STATE_SCHEDULED
        lease.appendRR(newsusprr)
            
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
        cp = self.db.findChangePointsInNode(start=earliest, end=originalstart, nodes=nodes, closed=False)
        cp = [ISO.ParseDateTime(p["time"]) for p in cp.fetchall()]
        cp = [earliest] + cp
        newstart = None
        for p in cp:
            self.availabilitywindow.initWindow(p, lease.resreq)
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
                self.db.updateReservationPartTimes(resmrr.db_rsp_ids, resmrr.start, resmrr.end, resmrr.realend)            
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
            else:
                vmrr.end -= diff
                vmrr.realend -= diff
            self.db.updateReservationPartTimes(vmrr.db_rsp_ids, vmrr.start, vmrr.end, vmrr.realend)
            self.db.commit()
            self.availabilitywindow.flushCache()
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
            
            canfitnopreemptionY = canfit[x][0]
            canfitpreemptionY = canfit[x][1]
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


    def genUtilizationStats(self, start, slottype=constants.RES_CPU, end=None):
        changepoints = self.db.findChangePoints(start, end)
        accumUtil=0
        prevTime = None
        startVM = None
        stats = []
        for point in changepoints:
            cur = self.db.getUtilization(point["time"], type=slottype)
            totalcapacity = 0
            totalused =0
            for row in cur:
                totalcapacity += row["sl_capacity"]
                totalused += row["used"]
            utilization = float(totalused) / totalcapacity
            time = ISO.ParseDateTime(point["time"])
            seconds = (time-start).seconds
            if startVM == None and utilization > 0:
                startVM = seconds
            if prevTime != None:
                timediff = time - prevTime
                weightedUtilization = prevUtilization*timediff.seconds 
                accumUtil += weightedUtilization
                average = accumUtil/seconds
            else:
                average = utilization
            stats.append((seconds, None, utilization, average))
            prevTime = time
            prevUtilization = utilization
        
        return stats
    
    def addImageTransfer(self, lease, t):
        rsp_id = self.db.addReservationPart(lease.leaseID, "Image transfer", 2)
        self.db.addAllocation(rsp_id, self.imagenodeslot_exact, t.start, t.end, 100.0, moveable=True, deadline=t.deadline, duration=(t.end-t.start).seconds)
        return rsp_id

    def updateStartTimes(self, db_rsp_ids, newstart):
        for rsp_id in db_rsp_ids:
            self.db.updateStartTimes(rsp_id, newstart)
        self.availabilitywindow.flushCache()
    
    def updateEndTimes(self, db_rsp_ids, newend):
        for rsp_id in db_rsp_ids:
            self.db.updateEndTimes(rsp_id, newend)
        self.availabilitywindow.flushCache()
            
    def updateLeaseEnd(self, rsp_id, newend):
        self.db.endReservationPart(rsp_id, newend)
        self.availabilitywindow.flushCache()
        
    def removeReservationPart(self, rsp_id):
        self.db.removeReservationPart(rsp_id)
        self.availabilitywindow.flushCache()
        
    def isFull(self, time):
        nodes = self.getAvailability(time)
        avail = sum([node.capacity[constants.RES_CPU] for node in nodes.values()])
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
        
    # Generate raw availability at change points
    def genAvail(self):
        self.avail = {}

        # Availability at initial time
        availatstart = self.slottable.getAvailability(self.time, self.resreq, self.onlynodes)

        for node in availatstart:
            capacity = availatstart[node].capacity
            capacitywithpreemption = availatstart[node].capacitywithpreemption
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
                capacitywithpreemption = availatpoint[node].capacitywithpreemption
                fits = self.resreq.getNumFitsIn(capacity)
                fitswithpreemption = self.resreq.getNumFitsIn(capacitywithpreemption)
                prevavail = self.avail[node][-1]
                if prevavail.getCanfit(canpreempt=False) > fits or prevavail.getCanfit(canpreempt=True) > fitswithpreemption:
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                
    # Create avail structure
    def initWindow(self, time, resreq, onlynodes = None):
        self.time = time
        self.resreq = resreq
        self.onlynodes = onlynodes
        self.genAvail()
    
    def fitAtStart(self, nodes = None):
        if nodes != None:
            avail = [v for (k,v) in self.avail.items() if k in nodes]
        else:
            avail = self.avail.values()
        return sum([e[0].canfit for e in avail])
    
    def findPhysNodesForVMs(self, numnodes, maxend, canpreempt=False):
        # Returns the physical nodes that can run all VMs, and the
        # time at which the VMs must end
        canfit = dict([(n, v[0].getCanfit(canpreempt)) for (n,v) in self.avail.items()])
        entries = []
        for n in self.avail.keys():
            entries += [(n,e) for e in self.avail[n][1:]]
        getTime = lambda x: x[1].time
        entries.sort(key=getTime)
        end = maxend
        for e in entries:
            physnode = e[0]
            entry = e[1]
       
            if entry.time >= maxend:
                # Can run to its maximum duration
                break
            else:
                diff = canfit[physnode] - entry.canfit
                totalcanfit = sum([n for n in canfit.values()]) - diff
                if totalcanfit < numnodes:
                    # Not enough resources. Must end here
                    end = entry.time
                    break
                else:
                    # Update canfit
                    canfit[physnode] = entry.canfit

        # Filter out nodes where we can't fit any vms
        canfit = dict([(n,v) for (n,v) in canfit.items() if v > 0])
        
        return end, canfit
            
                    
    def printContents(self, nodes = None):
        if nodes == None:
            physnodes = self.avail.keys()
        else:
            physnodes = [k for k in self.avail.keys() if k in nodes]
        physnodes.sort()
        edebug("AVAILABILITY WINDOW (time=%s, nodes=%s)"%(self.time,nodes), constants.ST, None)
        for n in physnodes:
            contents = "Node %i --- " % n
            for x in self.avail[n]:
                contents += "[ %s " % x.time
                contents += "{ "
                if x.avail == None and x.availpreempt == None:
                    contents += "END "
                else:
                    for s in x.avail.res.keys():
                        contents += "%s:%.2f " % (constants.res_str(s),x.avail.res[s])
                contents += "} (Fits: %i) ]  " % x.canfit
            edebug(contents, constants.ST, None)
                

                
                          
                          
            


        
        