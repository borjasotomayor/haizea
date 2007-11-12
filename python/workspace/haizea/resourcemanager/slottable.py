from sets import Set
from mx.DateTime import ISO, TimeDelta
from operator import attrgetter
import workspace.haizea.common.constants as constants
import workspace.haizea.resourcemanager.db as db
import workspace.haizea.resourcemanager.datastruct as ds
from workspace.haizea.common.log import info, debug, warning, edebug
from pysqlite2.dbapi2 import IntegrityError
from workspace.haizea.common.utils import roundDateTimeDelta

class SlotFittingException(Exception):
    pass

class SlotTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.rm = scheduler.rm
        
        templatedb = self.rm.config.getDBTemplate()
        targetdb = self.rm.config.getTargetDB()
        if targetdb == "memory":
            self.db = db.SQLiteSlotTableDB.toMemFromFile(templatedb)
        else:
            self.db = db.SQLiteSlotTableDB.toFileFromFile(templatedb, targetdb)
            
        self.createDatabase()
        
        self.availabilitywindow = AvailabilityWindow(self.db)

        
    def createDatabase(self):
        numnodes = self.rm.config.getNumPhysicalNodes()
        resources = self.rm.config.getResourcesPerPhysNode()
        bandwidth = self.rm.config.getBandwidth()
        
        slottypes = []
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            slottypes.append((self.db.getSlotTypeID(resourcename), resourcecapacity))
            
        # Create nodes
        for node in range(numnodes):
            nod_id = self.db.addNode("fakenode-%i.mcs.anl.gov" % (node+1))
            # Create slots
            for slottype in slottypes:
                self.db.addSlot(nod_id,slt_id=slottype[0],sl_capacity=slottype[1])
                
        # Create image nodes
        nod_id = self.db.addNode("fakeimagenode-exact.mcs.anl.gov")
        sl_id = self.db.addSlot(nod_id,slt_id=constants.RES_NETOUT,sl_capacity=bandwidth)
        
        self.imagenodeslot_exact = sl_id

        nod_id = self.db.addNode("fakeimagenode-besteffort.mcs.anl.gov")
        sl_id = self.db.addSlot(nod_id,slt_id=constants.RES_NETOUT,sl_capacity=bandwidth)
        
        self.imagenodeslot_besteffort = sl_id
        self.db.commit()
        
    def commit(self):
        self.db.commit()
    
    def rollback(self):
        self.db.rollback()
    
    def getNextChangePoint(self, time):
        return self.db.popNextChangePoint(time)

    def peekNextChangePoint(self, time):
        return self.db.peekNextChangePoint(time)
    
    def fitExact(self, leaseID, start, end, vmimage, numnodes, resreq, prematureend=None, preemptible=False, canpreempt=True):
        slottypes = resreq.keys()
                
        candidatenodes = self.candidateNodesInRange(start,end,resreq,canpreempt)

        # Decide if we can actually fit the entire VW
        # This is the point were we can apply different slot fitting algorithms
        # Currently, a greedy algorithm is used
        # This is O(numvirtualnodes*numphysicalnodes), but could be implemented
        # in O(numvirtualnodes). We'll worry about that later.
        allfits = True
        assignment = {}
        nodeassignment = {}
        orderednodes = self.prioritizenodes(candidatenodes,vmimage,start, resreq, canpreempt)
        
        # We try to fit each virtual node into a physical node
        # First we iterate through the physical nodes trying to fit the virtual node
        # without using preemption. If preemption is allowed, we iterate through the
        # nodes again but try to use preemptible resources.
        
        # This variable keeps track of how many resources we have to preempt on a node
        mustpreempt={}
        info("Node ordering: %s" % orderednodes, constants.ST, self.rm.time)

        for vwnode in range(1,numnodes+1):
            assignment[vwnode] = {}
            # Without preemption
            for physnode in orderednodes:
                fits = True
                for slottype in slottypes:
                    res = resreq[slottype]
                    if res > candidatenodes[physnode][slottype][2]:
                        fits = False
                if fits:
                    for slottype in slottypes:
                        res = resreq[slottype]
                        assignment[vwnode][slottype] = candidatenodes[physnode][slottype][1]
                        nodeassignment[vwnode] = physnode

                        candidatenodes[physnode][slottype][2] -= res
                        candidatenodes[physnode][slottype][3] -= res
                    break # Ouch
            else:
                if not canpreempt:
                    raise SlotFittingException, "Could not fit node %i in any physical node (w/o preemption)" % vwnode
                # Try preemption
                for physnode in orderednodes:
                    fits = True
                    for slottype in slottypes:
                        res = resreq[slottype]
                        if res > candidatenodes[physnode][slottype][3]:
                            fits = False
                    if fits:
                        for slottype in slottypes:
                            res = resreq[slottype]
                            assignment[vwnode][slottype] = candidatenodes[physnode][slottype][1]
                            nodeassignment[vwnode] = physnode
                            # See how much we have to preempt
                            if candidatenodes[physnode][slottype][2] > 0:
                                if res > candidatenodes[physnode][slottype][2]:
                                    res -= candidatenodes[physnode][slottype][2]
                                    candidatenodes[physnode][slottype][2] = 0
                                else:
                                    res = 0
                                    candidatenodes[physnode][slottype][2] -= res
                            candidatenodes[physnode][slottype][3] -= res
                            if not mustpreempt.has_key(physnode):
                                mustpreempt[physnode] = {}
                            if not mustpreempt[physnode].has_key(slottype):
                                mustpreempt[physnode][slottype]=res                                
                            else:
                                mustpreempt[physnode][slottype]+=res                              
                        break # Ouch
                else:
                    raise SlotFittingException, "Could not fit node %i in any physical node (w/preemption)" % vwnode

        db_rsp_ids = []
        if allfits:
            self.availabilitywindow.flushCache()
            transfers = []
            info("The VM reservations for this lease are feasible", constants.ST, self.rm.time)
            self.db.addReservation(leaseID, "LEASE #%i" % leaseID)
            for vwnode in range(1,numnodes+1):
                rsp_name = "VM %i" % (vwnode)
                rsp_id = self.db.addReservationPart(leaseID, rsp_name, 1, preemptible)
                db_rsp_ids.append(rsp_id)
                transfers.append((nodeassignment[vwnode], rsp_id))
                for slottype in slottypes:
                    amount = resreq[slottype]
                    sl_id = assignment[vwnode][slottype]
                    self.db.addAllocation(rsp_id, sl_id, start, end, amount, realEndTime = prematureend)
            return nodeassignment, mustpreempt, transfers, db_rsp_ids
        else:
            raise SlotFittingException        
        
    def candidateNodesInRange(self, start, end, resreq, canpreempt):
        slottypes = resreq.keys()
        
        # This variable stores the candidate slots. It is a dictionary, where the key
        # is the slot type (i.e. this allows us easy access to the candidate slots for
        # one type of slot). Each item is a list containing:
        #    1. Node id
        #    2. Slot id
        #    3. Maximum available resources in that slot (without preemption)
        #    4. Maximum available resources in that slot (with preemption)
        candidateslots = {}
        
        # TODO: There are multiple points at which the slot fitting could be aborted
        # for lack of resources. Currently, this is only checked at the very end.
        for slottype in slottypes:
            candidateslots[slottype] = []
            needed = resreq[slottype]
            
            # This variable stores the (possibly multiple) availabilities of a slot
            # during the requested time interval. It is a dictionary with the slot id
            # as a key. The value is itself a dictionary, with the time (at which
            # availability is measured) as a key. The value is a list with two elements:
            #    [ available resources,
            #      available resources (assuming we can preempt ]
            capacity={}
            
            # This is a list of candidate slot id's
            slots = []

            # TODO: Lots of code here which can be factored out into a function

            # First sieve: select all slots that have enough resources at the beginning
            slotsbegin = []
            slots1 = self.db.findAvailableSlots(time=start, amount=needed, type=slottype, canpreempt=False)
            for slot in slots1:
                slot_id = slot["SL_ID"]
                slotsbegin.append(slot_id)
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = {}
                capacity[slot_id][start] =[slot["available"],0]

            # Find available resources if we can do preemption    
            if canpreempt:
                slots1 = self.db.findAvailableSlots(time=start, amount=needed, type=slottype, canpreempt=True)
                for slot in slots1:
                    slot_id = slot["SL_ID"]
                    if not slot_id in slots:
                        slotsbegin.append(slot_id)
                    if not capacity.has_key(slot_id):
                        capacity[slot_id] = {}
                        capacity[slot_id][start]=[0,slot["available"]]
                    else:
                        capacity[slot_id][start][1]=slot["available"]

            # Second sieve: remove slots that don't have enough resources at the end
            slots2 = self.db.findAvailableSlots(time=end, amount=needed, slots=slotsbegin, closed=False, canpreempt=False)
            slots = []
            for slot in slots2:
                nod_id = slot["NOD_ID"]
                slot_id = slot["SL_ID"]
                slots.append((nod_id,slot_id))
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = {}
                capacity[slot_id][end] = [slot["available"], 0]

            # Find available resources if we can do preemption    
            if canpreempt:
                slots2 = self.db.findAvailableSlots(time=end, amount=needed, slots=slotsbegin, closed=False, canpreempt=True)
                for slot in slots2:
                    nod_id = slot["NOD_ID"]
                    slot_id = slot["SL_ID"]
                    if not (nod_id,slot_id) in slots:
                        slots.append((nod_id,slot_id))
                    if not capacity.has_key(slot_id):
                        capacity[slot_id] = {}
                        capacity[slot_id][end]=[0,slot["available"]]
                    else:
                        if capacity[slot_id].has_key(end):
                            capacity[slot_id][end][1]=slot["available"]
                        else:
                            capacity[slot_id][end] = [0,slot["available"]]

            # Final sieve: Determine "resource change points" and make sure that 
            # there is enough resources at each point too (and determine maximum
            # available)
            cur = self.db.findChangePoints(start, end, closed=False, withSlots=True)
            changepointsslots = cur.fetchall()
            for slot in slots:
                slot_id=slot[1]
                             
                changepoints = [r for r in changepointsslots if r["sl_id"] == slot_id]
                for point in changepoints:
                    time = point["time"]

                    cur = self.db.findAvailableSlots(time=time, slots=[slot_id], canpreempt=False)
                    avail = cur.fetchone()["available"]
                    if canpreempt:
                        cur = self.db.findAvailableSlots(time=time, slots=[slot_id], canpreempt=True)
                        availpreempt = cur.fetchone()["available"]
                    else:
                        availpreempt = 0
                        
                    capacity[slot_id][time]=[avail, availpreempt]
                    
                maxavail = float("inf")
                if canpreempt:
                    maxavailpreempt = float("inf")
                for cap in capacity[slot_id].values():
                    if cap[0] < maxavail:
                        maxavail = cap[0]
                    if canpreempt and cap[1] < maxavailpreempt:
                        maxavailpreempt = cap[1]
                        
                if not canpreempt and maxavail >= needed:
                    candidateslots[slottype].append([slot[0],slot[1],maxavail, maxavail])
                elif canpreempt and maxavailpreempt >= needed:
                    candidateslots[slottype].append([slot[0],slot[1],maxavail, maxavailpreempt])
            
            info("Slot type %i has candidates %s" % (slottype,candidateslots[slottype]), constants.ST, self.rm.time)
        
        # Make sure that available resources are all available on the same node
        # (e.g. discard candidate nodes where we can provision memory but not the cpu)
        nodeset = Set()
        for slottype in slottypes:
            nodes = Set([s[0] for s in candidateslots[slottype]])
            nodeset |= nodes
            
        for slottype in slottypes:
            nodes = Set([s[0] for s in candidateslots[slottype]])
            nodeset &= nodes
                
        if len(nodeset) == 0:
            raise SlotFittingException, "No physical node has enough available resources for this request"
        
        # This variable contains essentially the same information as candidateslots, except
        # access is done by node id and slottype (i.e. provides an easy way of asking
        # "what is the slot id of the memory slot in node 5")
        # Items have the same information as candidateslots.
        candidatenodes = {}
        
        for slottype in slottypes:
            for slot in candidateslots[slottype]:
                nod_id = slot[0]
                if nod_id in nodeset:
                    if not candidatenodes.has_key(nod_id):
                        candidatenodes[nod_id]={}
                    candidatenodes[nod_id][slottype] = slot
        for node in nodeset:    
            info("Node %i has final candidates %s" % (node,candidatenodes[node]), constants.ST, self.rm.time)
            
        return candidatenodes

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


    def fitBestEffort(self, lease, earliest, canreserve, suspendable, canmigrate, mustresume):
        leaseID = lease.leaseID
        remdur = lease.remdur
        numnodes = lease.numnodes
        resreq = lease.resreq
        realdur = lease.realremdur
        
        slottypes = resreq.keys()

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
            mbtotransfer = lease.vmimagesize + resreq[constants.RES_MEM]
            migratetime = float(mbtotransfer) / self.rm.config.getBandwidth()
            migratetime = roundDateTimeDelta(TimeDelta(seconds=migratetime))
            earliesttransfer = self.rm.time + migratetime

            for n in earliest:
                earliest[n][0] = max(earliest[n][0],earliesttransfer)
        
        changepoints = list(set([x[0] for x in earliest.values()]))
        changepoints.sort()
        
        # If we can make reservations for best-effort leases,
        # we also consider future changepoints
        # (otherwise, we only allow the VMs to start "now", accounting
        #  for the fact that vm images will have to be deployed)
        if canreserve:
            futurecp = self.db.findChangePoints(changepoints[-1], closed=False)
            futurecp = [ISO.ParseDateTime(p["time"]) for p in futurecp.fetchall()]
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
            self.availabilitywindow.initWindow(p, resreq, curnodes)
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
        
        self.availabilitywindow.flushCache()

        # Make the reservation
        try:
            self.db.addReservation(leaseID, "LEASE #%i" % leaseID)
        except IntegrityError, msg:
            pass # Ignore these for now. TODO: Redesign DB to remove TB_RESERVATION

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
        vmnode = 1
        db_rsp_ids = []
        while vmnode <= numnodes:
            for n in physnodes:
                if canfit[n]>0:
                    canfit[n] -= 1
                    mappings[vmnode] = n
                    rsp_name = "VM %i" % (vmnode)
                    rsp_id = self.db.addReservationPart(leaseID, rsp_name, 1, preemptible=True)
                    db_rsp_ids.append(rsp_id)
                    for slottype in slottypes:
                        amount = resreq[slottype]
                        sl_id = self.availabilitywindow.slot_ids[n][slottype]
                        self.db.addAllocation(rsp_id, sl_id, start, end, amount, realEndTime = realend)
                    vmnode += 1
                    break
        resume_db_rsp_id = None
        if mustresume:
            rsp_name = "RESUME %i" % leaseID
            rsp_id = self.db.addReservationPart(leaseID, rsp_name, 4, preemptible=False)
            for vmnode in mappings:
                physnode = mappings[vmnode]
                sl_id = self.availabilitywindow.slot_ids[physnode][constants.RES_MEM]
                self.db.addAllocation(rsp_id, sl_id, start - resumetime, start, resreq[constants.RES_MEM], realEndTime=start)
            resume_db_rsp_id = rsp_id
        suspend_db_rsp_id = None
        if mustsuspend:
            rsp_name = "SUSPEND %i" % leaseID
            rsp_id = self.db.addReservationPart(leaseID, rsp_name, 3, preemptible=False)
            for vmnode in mappings:
                physnode = mappings[vmnode]
                sl_id = self.availabilitywindow.slot_ids[physnode][constants.RES_MEM]
                self.db.addAllocation(rsp_id, sl_id, end, end + suspendtime, resreq[constants.RES_MEM], realEndTime=end+suspendtime)
            suspend_db_rsp_id = rsp_id

        return mappings, start, end, realend, resumetime, suspendtime, reservation, db_rsp_ids, resume_db_rsp_id, suspend_db_rsp_id

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


    def prioritizenodes(self,candidatenodes,vmimage,start,resreq, canpreempt):
        # TODO2: Choose appropriate prioritizing function based on a
        # config file, instead of hardcoding it)
        #
        # TODO3: Basing decisions only on CPU allocations. This is ok for now,
        # since the memory allocation is proportional to the CPU allocation.
        # Later on we need to come up with some sort of weighed average.
        
        nodes = candidatenodes.keys()
        
        # TODO
        #reusealg = self.config.get(GENERAL_SEC, REUSEALG_OPT)
        nodeswithimg=[]
        #if reusealg=="cache":
        #    nodeswithimg = self.backend.getNodesWithCachedImg(imguri)
        #elif reusealg=="cowpool":
        #    nodeswithimg = self.backend.getNodesWithImgLater(imguri, startTime)
        
        # Compares node x and node y. 
        # Returns "x is ??? than y" (???=BETTER/WORSE/EQUAL)
        def comparenodes(x,y):
            hasimgX = x in nodeswithimg
            hasimgY = y in nodeswithimg

            need = resreq[constants.RES_CPU]
            # First comparison: A node with no preemptible VMs is preferible
            # to one with preemptible VMs (i.e. we want to avoid preempting)
            availX = candidatenodes[x][constants.RES_CPU][2]
            availpX = candidatenodes[x][constants.RES_CPU][3]
            preemptibleres = availpX - availX
            hasPreemptibleX = preemptibleres > 0
            
            availY = candidatenodes[y][constants.RES_CPU][2]
            availpY = candidatenodes[y][constants.RES_CPU][3]
            preemptibleres = availpY - availY
            hasPreemptibleY = preemptibleres > 0

            canfitX = availX / need
            canfitY = availY / need
            
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
                    if canfitX > canfitY: return constants.BETTER
                    elif canfitX < canfitY: return constants.WORSE
                    else: return constants.EQUAL
            elif hasPreemptibleX and hasPreemptibleY:
                # If both have (some) preemptible resources, we prefer those
                # that involve the less preemptions
                canfitpX = availpX / need
                canfitpY = availpY / need
                preemptX = canfitpX - canfitX
                preemptY = canfitpY - canfitY
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
    
    def updateEndTimes(self, db_rsp_ids, realend):
        for rsp_id in db_rsp_ids:
            self.db.updateEndTimes(rsp_id, realend)
        self.availabilitywindow.flushCache()
            
    def updateLeaseEnd(self, rsp_id, newend):
        self.db.endReservationPart(rsp_id, newend)
        self.availabilitywindow.flushCache()
        
    def removeReservationPart(self, rsp_id):
        self.db.removeReservationPart(rsp_id)
        self.availabilitywindow.flushCache()
        
    def isFull(self, time):
        return self.db.isFull(time, constants.RES_CPU)

class AvailEntry(object):
    def __init__(self, time, avail, availpreempt = None):
        self.time = time
        self.avail = avail
        self.canfit = None
        self.availpreempt = availpreempt
        self.canfitpreempt = None

# Does not support preemption just yet
# TODO: Constrain window to only specific nodes (to take into account
# earliest start times)
class AvailabilityWindow(object):
    def __init__(self, db):
        self.time = None
        self.resreq = None
        self.onlynodes = None
        self.db = db
        self.avail = None
        self.availcache = {}
        self.slot_ids = {}

    def findAvailableSlots(self, time, amount=None, type=None, canpreempt=False, slotfilter=None, nodes=None):
        if not self.availcache.has_key(time):
            # Cache miss
            self.availcache[time] = []
            availslots = self.db.quickFindAvailableSlots(time, canpreempt=canpreempt)
            slots = self.db.getSlots()
            slot_ids = set()
            for slot in availslots:
                slot_ids.add(slot["SL_ID"])
                self.availcache[time].append(slot)
            
            for slot in slots:
                if not slot["SL_ID"] in slot_ids:
                    self.availcache[time].append(slot)
        
        slots = self.availcache[time]
        
        if nodes != None:
            slotfilter = []
            for n in nodes:
                slotfilter.append(self.slot_ids[n].values())
            slotfilter = [x for y in slotfilter for x in y] # Flatten
        
        if slotfilter != None:
            slotfilter = set(slotfilter)
            slots = [s for s in slots if s["SL_ID"] in slotfilter]
        if type != None and amount != None:
            slots = [s for s in slots if s["SLT_ID"] == type and s["available"] >= amount]
         
        return slots
        
    # Generate raw availability at change points
    def genAvail(self):
        self.avail = {}
        slot_ids = {}
        slottypes = self.resreq.keys()
        
        # Initially, this will be a dictionary (key: physnode) of
        # dictionaries (key: slottype), containing AvailEntry objects
        # This is done because it makes it easier to extract the information
        # from the database. Later on, we will transform this into
        # a more convenient structure.
        avail_aux = {}
        # Availability at initial time
        for slottype in slottypes:
            needed = self.resreq[slottype]
            
            slots = self.findAvailableSlots(time=self.time, amount=needed, type=slottype, canpreempt=False, nodes=self.onlynodes)
            for slot in slots:
                nod_id = slot["NOD_ID"]
                slot_id = slot["SL_ID"]
                avail = slot["available"]
                if not avail_aux.has_key(nod_id):
                    avail_aux[nod_id] = {}
                    if not self.slot_ids.has_key(nod_id):
                        self.slot_ids[nod_id] = {}
                    slot_ids[nod_id] = {}
                avail_aux[nod_id][slottype] = [AvailEntry(self.time,avail)]
                self.slot_ids[nod_id][slottype] = slot_id
                slot_ids[nod_id][slottype] = slot_id

        # Filter out nodes that don't have enough resources for at
        # least one VM (the previous step may have selected nodes that,
        # for example, have enough CPU but not enough memory)
        for physnode in avail_aux.keys():
            # Check that the node has enough resources for at least one VM
            enough = True
            for slottype in slottypes:
                if not avail_aux[physnode].has_key(slottype):
                    enough = False
                elif avail_aux[physnode][slottype][0].avail < self.resreq[slottype]:
                    enough = False
            if not enough:
                del avail_aux[physnode]
                del slot_ids[physnode]
                

        # Determine the availability at the subsequent change points
        slot_ids = [v.values() for v in slot_ids.values()]
        slot_ids = [x for y in slot_ids for x in y] # Flatten
        slot_ids = set(slot_ids)
        changepoints = self.db.findChangePoints(self.time, slots=list(slot_ids), closed=False)
        cp = [ISO.ParseDateTime(p["time"]) for p in changepoints.fetchall()]
        for p in cp:
            slots = self.findAvailableSlots(time=p, slotfilter=list(slot_ids), canpreempt=False)
            for slot in slots:
                nod_id = slot["NOD_ID"]
                slot_id = slot["sl_id"]
                slottype = slot["slt_id"]
                avail = slot["available"]
                if avail < self.resreq[slottype]:
                    # If the available resources at this changepoint are
                    # less than what is required to run a single VM,
                    # this node has (de facto) no resources available
                    # in this availability window.
                    avail = 0
                    # No need to keep on checking this slot.
                    slot_ids.remove(slot_id)
                    
                # Only interested if the available resources decrease
                # in the window.
                prevavail = avail_aux[nod_id][slottype][-1].avail
                if avail < prevavail:
                    avail_aux[nod_id][slottype].append(AvailEntry(p,avail))

        # Now, we need to change the avail_aux into something more 
        # convenient. We will end up with a dictionary (key: physnode)
        # of AvailEntry objects. Now, the "avail" field will contain
        # a dictionary (key: slottype). 
        physnodes = avail_aux.keys()
        for n in physnodes:
            self.avail[n] = []
            slottypes = avail_aux[n].keys()
            l = []
            for s in slottypes:
                l += [(s,a) for a in avail_aux[n][s]]
            
            # Sort by time
            getTime = lambda x: x[1].time
            l.sort(key=getTime)
            
            prevtime = None
            res = dict([(s,None) for s in slottypes])
            
            for x in l:
                slottype = x[0]
                time = x[1].time
                avail = x[1].avail
                
                if time > prevtime:
                    a = AvailEntry(time,dict(res))
                    self.avail[n].append(a)
                    prevtime = time
                
                if avail == 0:
                    self.avail[n][-1].avail = None
                    break
                else: 
                    self.avail[n][-1].avail[slottype] = avail
                    res[slottype] = avail
        
        # Include (for convenience) the number of VMs we can fit
        # at each changepoint
        for n in physnodes:
            for e in self.avail[n]:
                if e.avail == None:
                    e.canfit = 0
                else:
                    canfit = []
                    for slottype in self.resreq.keys():
                        needed = self.resreq[slottype]
                        avail = e.avail[slottype]
                        canfit.append(int(avail / needed))
                    e.canfit = min(canfit)
                
    # Create avail structure
    def initWindow(self, time, resreq, onlynodes = None):
        self.time = time
        self.resreq = resreq
        self.onlynodes = onlynodes
        self.genAvail()
        
    def flushCache(self):
        self.availcache = {}
    
    def fitAtStart(self, nodes = None):
        if nodes != None:
            avail = [v for (k,v) in self.avail.items() if k in nodes]
        else:
            avail = self.avail.values()
        return sum([e[0].canfit for e in avail])
    
    def findPhysNodesForVMs(self, numnodes, maxend):
        # Returns the physical nodes that can run all VMs, and the
        # time at which the VMs must end
        canfit = dict([(n, v[0].canfit) for (n,v) in self.avail.items()])
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
                if x.avail == None:
                    contents += "END "
                else:
                    for s in x.avail.keys():
                        contents += "%s:%.2f " % (constants.res_str(s),x.avail[s])
                contents += "} (Fits: %i) ]  " % x.canfit
            edebug(contents, constants.ST, None)
                

                
                          
                          
            


        
        