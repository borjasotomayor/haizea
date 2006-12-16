import ConfigParser, os
from workspace.ears import db
from workspace.traces.files import TraceFile, TraceEntryV2
from sets import Set
from mx.DateTime import *
from mx.DateTime import ISO
from workspace.ears import srvlog, reslog, loglevel
from workspace.ears.control import SimulationControlBackend

STATUS_PENDING = 0
STATUS_RUNNING = 1
STATUS_DONE = 2
STATUS_SUSPENDED = 3


SLOTTYPE_CPU=1
SLOTTYPE_MEM=2
SLOTTYPE_OUTNET=4

GENERAL_SEC="general"
SIMULATION_SEC="simulation"

TYPE_OPT="type"
BATCHALG_OPT="batchalgorithm"
TEMPLATEDB_OPT="templatedb"
TARGETDB_OPT="targetdb"
DB_OPT="db"
NODES_OPT="nodes"
BANDWIDTH_OPT="bandwidth"
RESOURCES_OPT="resources"
STARTTIME_OPT="starttime"
DURADJUST_OPT="durationadjust"
LOGLEVEL_OPT="loglevel"
REDUNDANT_OPT="avoidredundanttransfers"
CACHE_OPT="cache"
MAXCACHESIZE_OPT="maxcache"
IMAGETRANSFERS_OPT="imagetransfers"

TRANSFER_REQUIRED=0
TRANSFER_REUSE=1
TRANSFER_CACHED=2
TRANSFER_NO=3

BETTER = -1
EQUAL = 0
WORSE = 1

def createEARS(config, tracefile):
    # Process configuration file
    if config.get(GENERAL_SEC, TYPE_OPT) == "simulation":
        # Are we using a template db or working directly on an existing database?
        if config.has_option(SIMULATION_SEC, TEMPLATEDB_OPT):
            templatedb = config.get(SIMULATION_SEC, TEMPLATEDB_OPT)
            if config.get(SIMULATION_SEC, TARGETDB_OPT) == "memory":
                resDB = db.SQLiteReservationDB.toMemFromFile(templatedb)
            else:
                # Target DB is a file
                targetdb = config.get(SIMULATION_SEC, TARGETDB_OPT)
                resDB = db.SQLiteReservationDB.toFileFromFile(templatedb, targetdb)
        elif config.has_option(SIMULATION_SEC, DB_OPT):
            dbfile=config.get(SIMULATION_SEC, DB_OPT)
            resDB = db.SQLiteReservationDB.fromFile(dbfile)
            
            
        trace = TraceFile.fromFile(tracefile, entryType=TraceEntryV2)
        
        # Adjust duration if necessary
        if config.has_option(SIMULATION_SEC, DURADJUST_OPT):
            adjust = config.getfloat(SIMULATION_SEC, DURADJUST_OPT)
            for i,v in enumerate(trace.entries):
                duration = int(int(v.fields["duration"]) * adjust)
                trace.entries[i].fields["duration"] = duration.__str__()
    
        return SimulatingServer(config, resDB, trace, commit=True)
    elif config.get(GENERAL_SEC,TYPE_OPT) == "real":
        return None #Not implemented yet

class SchedException(Exception):
    pass

class BaseServer(object):
    def __init__(self, config, resDB, trace, commit=True):
        self.config = config        
        self.resDB = resDB
        self.trace = trace
        self.reqnum=1
        self.commit=commit
        
        # Counters and lists for statistics
        self.acceptednum=0
        self.rejectednum=0
        self.batchcompletednum=0
        self.accepted=[]
        self.rejected=[]
        self.batchcompleted=[]
        
        self.batchqueue=[]
        self.batchreservations={}
        self.imagenodeslot_ar = None
        self.imagenodeslot_batch = None
        self.imagetransfers={}
        log = self.config.get(GENERAL_SEC, LOGLEVEL_OPT)
        srvlog.setLevel(loglevel[log])
        
    def processReservations(self, time, td):
        # Check for reservations which must end
        rescur = self.resDB.getReservationsWithEndingAllocationsInInterval(time, td, allocstatus=STATUS_RUNNING)
        reservations = rescur.fetchall()

        for res in reservations:          
            # Get reservation parts that have to end
            rspcur = self.resDB.getResPartsWithEndingAllocationsInInterval(time,td, allocstatus=STATUS_RUNNING, res=res["RES_ID"])
            resparts = rspcur.fetchall()
            
            for respart in resparts:
                if respart["RSP_STATUS"] == STATUS_RUNNING:
                    # Determine if we're suspending this reservation part, or if we actually
                    # have to end it.
                    rsp_id = respart["RSP_ID"]
                    allcur=self.resDB.getEndingAllocationsInInterval(time,td,respart["RSP_ID"])
                    suspending = False
                    ending = False
                    for alloc in allcur:
                        if alloc["ALL_NEXTSTART"] != None:
                            suspending = True
                        else:
                            ending = True
                            
                    if suspending and ending:
                        srvlog.error("Reservation part %i has allocations that are set to both suspend and end. This should not happen" % rsp_id)

                    if suspending:
                        self.suspendReservationPart(respart["RSP_ID"], row=respart, resname=res["RES_NAME"])
                    elif ending:
                        # Are we done with *all* the allocations for this resource part?
                        if self.isReservationPartEnd(respart["RSP_ID"], time, td):
                            self.stopReservationPart(respart["RSP_ID"], row=respart, resname=res["RES_NAME"])
                        else:
                            srvlog.warning("Resource allocation resizing not supported yet")

                    # All allocations are flagged as done, even if the reservation part is
                    # suspended (suspension is not recorded at the allocation level, only
                    # at the resource part level)
                    self.stopAllocations(respart["RSP_ID"], time, td)
                    
             # Check to see if this ends the reservation
            if self.isReservationDone(res["RES_ID"]):
                self.stopReservation(res["RES_ID"], row=res)

        # Check for reservations which must start
        rescur = self.resDB.getReservationsWithStartingAllocationsInInterval(time, td, allocstatus=STATUS_PENDING)
        reservations = rescur.fetchall()

        for res in reservations:
             # Check to see if this is the start of a reservation
            if res["RES_STATUS"] == STATUS_PENDING:
                self.startReservation(res["RES_ID"], row=res)
            elif res["RES_STATUS"] == STATUS_RUNNING:
                pass # do nothing
            
            # Get reservation parts that have to start
            rspcur = self.resDB.getResPartsWithStartingAllocationsInInterval(time,td, allocstatus=STATUS_PENDING, res=res["RES_ID"])
            resparts = rspcur.fetchall()
            
            for respart in resparts:
                if respart["RSP_STATUS"] == STATUS_PENDING:    
                    # The reservation part hasn't started yet
                    self.startReservationPart(respart["RSP_ID"], row=respart, resname=res["RES_NAME"])
                    self.startAllocations(respart["RSP_ID"], time, td)
                elif respart["RSP_STATUS"] == STATUS_RUNNING:
                    # The reservation part has already started
                    # This is a change in resource allocation
                    self.resizeAllocation(respart["RSP_ID"], row=respart)                
                elif respart["RSP_STATUS"] == STATUS_SUSPENDED:
                    # The reservation part was suspended and must now be resumed.
                    self.resumeReservationPart(respart["RSP_ID"], row=respart, resname=res["RES_NAME"])
                    self.startAllocations(respart["RSP_ID"], time, td)
        
        if self.commit: self.resDB.commit()

    def processTraceRequests(self, delta):
        seconds = delta.seconds
        reqToProcess = [r for r in self.trace.entries if int(r.fields["time"]) <= seconds]
        newtrace = [r for r in self.trace.entries if int(r.fields["time"]) > seconds]
        
        for r in reqToProcess:
            if r.fields["deadline"] == "NULL":
                self.processBatchRequest(r)
            else:
                self.processARRequest(r)
            self.reqnum+=1
                
        self.trace.entries = newtrace

    def processARRequest(self, r):
        reqTime = self.getTime() #Should be starttime + seconds
        startTime = reqTime + TimeDelta(seconds=int(r.fields["deadline"]))
        endTime = startTime + TimeDelta(seconds=int(r.fields["duration"]))
        numNodes = int(r.fields["numNodes"])
        dotransfer = self.config.getboolean(GENERAL_SEC, IMAGETRANSFERS_OPT)
        srvlog.info("%s: Received request for VW %i" % (reqTime, self.reqnum))
        srvlog.info("\tStart time: %s" % startTime)
        srvlog.info("\tEnd time: %s" % endTime)
        srvlog.info("\tNodes: %i" % numNodes)
        
        resources = {SLOTTYPE_CPU: float(r.fields["cpu"]), SLOTTYPE_MEM: float(r.fields["memory"])}
        
        res_id = self.resDB.addReservation("Test (AR) Reservation #%i" % self.reqnum)
        
        try:
            (mustpreempt) = self.scheduleMultipleVMs(res_id, startTime, endTime, "nfs:///foobar", numnodes=numNodes, resources=resources, preemptible=False, canpreempt=True)
            if len(mustpreempt) > 0:
                srvlog.info("Must preempt the following: %s", mustpreempt)
                self.preemptResources(mustpreempt, startTime)
            if dotransfer:
                self.scheduleImageTransferEDF(res_id, reqTime, startTime, numNodes, None, None, imgslot=self.imagenodeslot_ar)
            if self.commit: self.resDB.commit()
            self.acceptednum += 1
            self.accepted.append((reqTime, self.acceptednum))
        except SchedException, msg:
            srvlog.warning("Scheduling exception: %s" % msg)
            self.rejectednum += 1
            self.rejected.append((reqTime, self.rejectednum))
            self.resDB.rollback()
                
    def processBatchRequest(self, r):
        reqTime = self.getTime() #Should be starttime + second
        duration = TimeDelta(seconds=int(r.fields["duration"]))
        numNodes = int(r.fields["numNodes"])
        imgURI = r.fields["uri"]
        imgSize = int(r.fields["size"])
        srvlog.info("%s: Received batch request for VW %i" % (reqTime, self.reqnum))
        srvlog.info("\tDuration: %s" % duration)
        srvlog.info("\tNodes: %i" % numNodes)
        
        resources = {SLOTTYPE_CPU: float(r.fields["cpu"]), SLOTTYPE_MEM: float(r.fields["memory"])}

        # We create the reservation, but we don't make the reservation parts or
        # allocations yet (here we only queue requests... processQueue takes care 
        # of working through these --and other queued requests-- to see which ones 
        # should be dispatched)
        res_id = self.resDB.addReservation("Test (Batch) Reservation #%i" % self.reqnum)
        node = 1
        for vwnode in range(numNodes):
            nodeName = (vwnode+1).__str__()
            self.queueBatchRequest(res_id, duration, resources, nodeName, imgURI, imgSize)
        self.batchreservations[res_id] = [duration, resources, imgURI, imgSize, 0]
        
        if self.commit: self.resDB.commit()

    def queueBatchRequest(self,res_id, duration, resources, nodeName, imgURI, imgSize):
        vw = {}
        vw["res_id"]=res_id
        vw["duration"]=duration
        vw["resources"]=resources
        vw["node"] = nodeName
        vw["imgURI"] = imgURI
        vw["imgSize"] = imgSize
        self.batchqueue.append(vw)

    def processQueue(self):
        mustremove = []
        dotransfer = self.config.getboolean(GENERAL_SEC, IMAGETRANSFERS_OPT)
        srvlog.info("PROCESSING QUEUE")
        for i,vm in enumerate(self.batchqueue):
            res_id = vm["res_id"]
            duration = vm["duration"]
            resources = vm["resources"]
            nodename = "VM " + vm["node"]
            imgURI = vm["imgURI"]
            imgSize = vm["imgSize"]

            if dotransfer:
                startTimes = self.findEarliestStartingTimes(imgURI, imgSize, self.getTime())
            else:
                startTimes = dict([(node+1, (self.getTime(),TRANSFER_NO,None)) for node in range(self.getNumNodes()) ])

            batchAlgorithm = self.config.get(GENERAL_SEC,BATCHALG_OPT)
            if batchAlgorithm == "nopreemption":
                # Without any preemption support, we only schedule if there is enough
                # resources to run the job from beginning to end (not allowing
                # it to be preempted by an AR). As such, when the VMs are scheduled,
                # they are flagged as non-preemptible
                transfer = self.scheduleImageTransferFIFO(res_id, self.getTime(), 1, imgURI, imgSize, None, None, imgslot=self.imagenodeslot_batch)
                startTime = transfer[3]
                endTime = startTime + duration
                srvlog.info("Attempting to schedule VM for reservation %i from %s to %s" % (res_id,startTime,endTime))
                try:
                    self.scheduleMultipleVMs(res_id, startTime, endTime, imgURI, numnodes=1, resources=resources, preemptible=False, canpreempt=False, forceName=nodename)
                    if self.commit: self.resDB.commit()
                    mustremove.append(i)
                except SchedException, msg:
                    srvlog.warning("Can't schedule this batch VM now. Reason: %s" % msg)
                    self.resDB.rollback() # This will also roll back the image transfer
            elif batchAlgorithm == "onAR_resubmit":
                # Here we support preemption, so we flag the VM as preemptible
                # TODO: Currently. we only accept a request if we can run it from beginning 
                # to end. With our slot table, we should be able to preschedule batch jobs,
                # so the necessary images can be transferred while other VMs are running
                transfer = self.scheduleImageTransferFIFO(res_id, self.getTime(), 1, imgURI, imgSize, None, None, imgslot=self.imagenodeslot_batch)
                startTime = transfer[3]
                endTime = startTime + duration
                srvlog.info("Attempting to schedule VM for reservation %i from %s to %s" % (res_id,startTime,endTime))
                try:
                    self.scheduleMultipleVMs(res_id, startTime, endTime, imgURI, numnodes=1, resources=resources, preemptible=True, canpreempt=False, forceName=nodename)
                    if self.commit: self.resDB.commit()
                    mustremove.append(i)
                except SchedException, msg:
                    srvlog.warning("Can't schedule this batch VM now. Reason: %s" % msg)
                    self.resDB.rollback() # This will also roll back the image transfer
            elif batchAlgorithm == "onAR_suspend":
                try:
                    srvlog.info("Attempting to schedule VM for reservation %i starting at %s with duration %s" % (res_id,self.getTime(),duration))
                    srvlog.info("%s" % startTimes)
                    result = self.scheduleSinglePreemptibleVM(res_id, startTimes, duration, imgURI, resources=resources, forceName=nodename)
                    node = result[0]
                    VMrsp_id = result[1]
                    transfertype = startTimes[node][1]
                    if transfertype == TRANSFER_REQUIRED:
                        transfer = self.scheduleImageTransferFIFO(res_id, self.getTime(), 1, imgURI, imgSize, node, VMrsp_id, imgslot=self.imagenodeslot_batch)
                    elif transfertype == TRANSFER_REUSE:
                        srvlog.info("No need to schedule an image transfer (reusing an existing transfer to destination node)")
                        reusetransfer_rsp_id = startTimes[node][2]
                        self.imagetransfers[reusetransfer_rsp_id][3].append(VMrsp_id)
                    elif transfertype == TRANSFER_CACHED:
                        self.backend.completedImgTransferToNode(node, imgURI, imgSize, [VMrsp_id])
                        srvlog.info("No need to schedule an image transfer (image is already cached in destination node)")
                    elif transfertype == TRANSFER_NO:
                        srvlog.info("Assuming no image has to be transfered")
                    if self.commit: self.resDB.commit()
                    mustremove.append(i)
                except SchedException, msg:
                    srvlog.warning("Can't schedule this batch VM now. Reason: %s" % msg)
                    self.resDB.rollback() 


        newbatchqueue = [vm for i,vm in enumerate(self.batchqueue) if i not in mustremove]

        self.batchqueue = newbatchqueue
        

    # Note: The difference between scheduleMultipleVMs and scheduleSinglePreemptibleVM
    # is that scheduleMultipleVMs requires that the VM be able to run from beginning to
    # end. scheduleSinglePreemptibleVM can schedule a VM that is not able to run from
    # beginning to end  by prescheduling suspend/resume points (of course, this is only
    # acceptable in preemptible VMs)
    def scheduleMultipleVMs(self, res_id, startTime, endTime, imguri, numnodes, resources, preemptible=False, canpreempt=True, forceName=None):
        
        slottypes = resources.keys()
        
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
            needed = resources[slottype]
            
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
            slots1 = self.resDB.findAvailableSlots(time=startTime, amount=needed, type=slottype, canpreempt=False)
            for slot in slots1:
                slot_id = slot["SL_ID"]
                slotsbegin.append(slot_id)
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = {}
                capacity[slot_id][startTime] =[slot["available"],0]

            # Find available resources if we can do preemption    
            if canpreempt:
                slots1 = self.resDB.findAvailableSlots(time=startTime, amount=needed, type=slottype, canpreempt=True)
                for slot in slots1:
                    slot_id = slot["SL_ID"]
                    if not slot_id in slots:
                        slotsbegin.append(slot_id)
                    if not capacity.has_key(slot_id):
                        capacity[slot_id] = {}
                        capacity[slot_id][startTime]=[0,slot["available"]]
                    else:
                        capacity[slot_id][startTime][1]=slot["available"]

            # Second sieve: remove slots that don't have enough resources at the end
            slots2 = self.resDB.findAvailableSlots(time=endTime, amount=needed, slots=slotsbegin, closed=False, canpreempt=False)
            slots = []
            for slot in slots2:
                nod_id = slot["NOD_ID"]
                slot_id = slot["SL_ID"]
                slots.append((nod_id,slot_id))
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = {}
                capacity[slot_id][endTime] = [slot["available"], 0]

            # Find available resources if we can do preemption    
            if canpreempt:
                slots2 = self.resDB.findAvailableSlots(time=endTime, amount=needed, slots=slotsbegin, closed=False, canpreempt=True)
                for slot in slots2:
                    nod_id = slot["NOD_ID"]
                    slot_id = slot["SL_ID"]
                    if not (nod_id,slot_id) in slots:
                        slots.append((nod_id,slot_id))
                    if not capacity.has_key(slot_id):
                        capacity[slot_id] = {}
                        capacity[slot_id][endTime]=[0,slot["available"]]
                    else:
                        if capacity[slot_id].has_key(endTime):
                            capacity[slot_id][endTime][1]=slot["available"]
                        else:
                            capacity[slot_id][endTime] = [0,slot["available"]]

            # Final sieve: Determine "resource change points" and make sure that 
            # there is enough resources at each point too (and determine maximum
            # available)
            for slot in slots:
                slot_id=slot[1]
                changepoints = self.resDB.findChangePoints(startTime, endTime, slot_id, closed=False)

                for point in changepoints:
                    time = point["time"]

                    cur = self.resDB.findAvailableSlots(time=time, slots=[slot_id], canpreempt=False)
                    avail = cur.fetchone()["available"]
                    if canpreempt:
                        cur = self.resDB.findAvailableSlots(time=time, slots=[slot_id], canpreempt=True)
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
            
            srvlog.info("Slot type %i has candidates %s" % (slottype,candidateslots[slottype]))
        
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
            raise SchedException, "No physical node has enough available resources for this request"
        
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
            srvlog.info("Node %i has final candidates %s" % (node,candidatenodes[node]))


        # Decide if we can actually fit the entire VW
        # This is the point were we can apply different slot fitting algorithms
        # Currently, a greedy algorithm is used
        # This is O(numvirtualnodes*numphysicalnodes), but could be implemented
        # in O(numvirtualnodes). We'll worry about that later.
        allfits = True
        assignment = {}
        orderednodes = self.prioritizenodes(candidatenodes,imguri,resources, canpreempt)
        
        # We try to fit each virtual node into a physical node
        # First we iterate through the physical nodes trying to fit the virtual node
        # without using preemption. If preemption is allowed, we iterate through the
        # nodes again but try to use preemptible resources.
        
        # This variable keeps track of how many resources we have to preempt on a node
        mustpreempt={}
        srvlog.info("Node ordering: %s" % orderednodes)
        for vwnode in range(0,numnodes):
            assignment[vwnode] = {}
            # Without preemption
            for physnode in orderednodes:
                fits = True
                for slottype in slottypes:
                    res = resources[slottype]
                    if res > candidatenodes[physnode][slottype][2]:
                        fits = False
                if fits:
                    for slottype in slottypes:
                        res = resources[slottype]
                        assignment[vwnode][slottype] = candidatenodes[physnode][slottype][1]

                        candidatenodes[physnode][slottype][2] -= res
                        candidatenodes[physnode][slottype][3] -= res
                    break # Ouch
            else:
                if not canpreempt:
                    raise SchedException, "Could not fit node %i in any physical node (w/o preemption)" % vwnode
                # Try preemption
                for physnode in orderednodes:
                    fits = True
                    for slottype in slottypes:
                        res = resources[slottype]
                        if res > candidatenodes[physnode][slottype][3]:
                            fits = False
                    if fits:
                        for slottype in slottypes:
                            res = resources[slottype]
                            assignment[vwnode][slottype] = candidatenodes[physnode][slottype][1]
                            # See how much we have to preempt
                            # Precond: res > candidatenodes[physnode][slottype][2]
                            if candidatenodes[physnode][slottype][2] > 0:
                                res -= candidatenodes[physnode][slottype][2]
                                candidatenodes[physnode][slottype][2] = 0
                            candidatenodes[physnode][slottype][3] -= res
                            if not mustpreempt.has_key(physnode):
                                mustpreempt[physnode] = {}
                            if not mustpreempt[physnode].has_key(slottype):
                                mustpreempt[physnode][slottype]=res                                
                            else:
                                mustpreempt[physnode][slottype]+=res                                
                        break # Ouch
                else:
                    raise SchedException, "Could not fit node %i in any physical node (w/preemption)" % vwnode

        if allfits:
            srvlog.info( "This VW is feasible")
            for vwnode in range(0,numnodes):
                if forceName == None:
                    rsp_name = "VM %i" % (vwnode+1)
                else:
                    rsp_name = forceName
                    rsp_name += " (node %i)" % (vwnode+1)
                rsp_id = self.resDB.addReservationPart(res_id, rsp_name, 1, preemptible)
                for slottype in slottypes:
                    amount = resources[slottype]
                    sl_id = assignment[vwnode][slottype]
                    self.resDB.addAllocation(rsp_id, sl_id, startTime, endTime, amount)
            return mustpreempt
        else:
            raise SchedException

    def scheduleSinglePreemptibleVM(self, res_id, startTimes, duration, imguri, resources, forceName=False):
        slottypes = resources.keys()
        
        # This variable stores the candidate slots. It is a dictionary, where the key
        # is the node id and the slot type (i.e. provides an easy way of asking
        # "what is the slot id of the memory slot in node 5")
        # Each item is a list containing:
        #    1. Slot id
        #    2. Maximum available resources in that slot (without preemption)
        #    3. Earlies start time
        candidatenodes = {}

        for node in startTimes.keys():
            enoughInAllSlots = True
            candidatenodes[node] = {}
            for slottype in slottypes:
                needed = resources[slottype]
                startTime = startTimes[node][0]
                slots = self.resDB.findAvailableSlots(time=startTime, amount=needed, type=slottype, node=node, canpreempt=False)            
                slots = slots.fetchall()
                if len(slots) == 0:
                    enoughInAllSlots = False
                else:
                    slot = slots[0]
                    sl_id = slot["SL_ID"]
                    available = slot["available"]
                    candidatenodes[node][slottype]= (sl_id, available, startTime)
            if not enoughInAllSlots:
                del candidatenodes[node]
                                
        if len(candidatenodes) == 0:
            raise SchedException, "No physical node has enough available resources for this request at this time"

                    
        # We want to choose the node that will allow this VM to end at the earliest possible
        # time.
        optimalNode = None
        endTime = None
        suspendTime = None
        for node in candidatenodes.keys():    
            srvlog.info("Node %i has final candidates %s" % (node,candidatenodes[node]))
            sl_id = candidatenodes[node][SLOTTYPE_CPU][0]
            res_needed = resources[SLOTTYPE_CPU]
            startTime = startTimes[node][0]
            suspendpoints = self.findSuspendPoints(startTime, sl_id, res_needed, duration, suspensiontime=None)
            srvlog.info("Suspend points:")
            for point in suspendpoints:
                srvlog.info("%s %s %s" % (point[0],point[1],point[2]))
            nodeSuspendTime=suspendpoints[0][1]
            nodeEndTime=suspendpoints[-1][1]
            if endTime==None:
                endTime = nodeEndTime
                suspendTime = nodeSuspendTime
                optimalNode = node 
            elif endTime > nodeEndTime:
                endTime = nodeEndTime
                suspendTime = nodeSuspendTime
                optimalNode = node 
        srvlog.info("Node with best suspension points is %i" % optimalNode)
        needsSuspension = (endTime != suspendTime)
        startTime = startTimes[optimalNode][0]
        
        # Make allocations
        if forceName == None:
            rsp_name = "VM"
        else:
            rsp_name = forceName
        rsp_id = self.resDB.addReservationPart(res_id, rsp_name, 1, True)
        for slottype in slottypes:
            amount = resources[slottype]
            sl_id = candidatenodes[optimalNode][slottype][0]
            endTime = startTime + duration
            self.resDB.addAllocation(rsp_id, sl_id, startTime, endTime, amount)

        if needsSuspension:
            srvlog.info("This VM will require suspension.")
            self.rescheduleReservationPartForSuspension(rsp_id, suspendTime)
                
        return (optimalNode, rsp_id)
        


    def prioritizenodes(self,candidatenodes,imguri,resources, canpreempt):
        # TODO: This function should prioritize nodes greedily:
        #  * First, choose nodes where the required image is already cached 
        #    and more than one virtual node can be fit.
        #  * Then, choose nodes where the required image is already cached, 
        #    but only one virtual node can be fit.
        #  * Next, choose nodes where the image is not cached, but more than 
        #    one virtual node can be fit.
        #  * Finally, choose all remaining nodes. 
        # 
        # TODO2: Choose appropriate prioritizing function based on a
        # config file, instead of hardcoding it)
        #
        # TODO3: Basing decisions only on CPU allocations. This is ok for now,
        # since the memory allocation is proportional to the CPU allocation.
        # Later on we need to come up with some sort of weighed average.
        
        nodes = candidatenodes.keys()
        
        # Compares node x and node y. 
        # Returns "x is ??? than y" (???=BETTER/WORSE/EQUAL)
        def comparenodes(x,y):
            # TODO: Check caches
            need = resources[SLOTTYPE_CPU]
            # First comparison: A node with no preemptible VMs is preferible
            # to one with preemptible VMs (i.e. we want to avoid preempting)
            availX = candidatenodes[x][SLOTTYPE_CPU][2]
            availpX = candidatenodes[x][SLOTTYPE_CPU][3]
            preemptibleres = availpX - availX
            hasPreemptibleX = preemptibleres > 0
            
            availY = candidatenodes[y][SLOTTYPE_CPU][2]
            availpY = candidatenodes[y][SLOTTYPE_CPU][3]
            preemptibleres = availpY - availY
            hasPreemptibleY = preemptibleres > 0
            if hasPreemptibleX and not hasPreemptibleY:
                return WORSE
            elif not hasPreemptibleX and hasPreemptibleY:
                return BETTER
            else:
             #
                # Both have no preemptible resources XOR some preemptible resources
                
                # The next criteria is how many virtual nodes we could fit
                # on each physical node (more is best) *without* preemption
                canfitX = availX / need
                canfitY = availY / need
                if canfitX > canfitY:
                    return BETTER
                elif canfitX < canfitY:
                    return WORSE
                else:
                    if not hasPreemptibleX and not hasPreemptibleY:
                        # If both have no preemptible resources, we have no tie breaker
                        return EQUAL # No tie breaker at this point
                    elif hasPreemptibleX and hasPreemptibleY:
                        # If both have (some) preemptible resources, we prefer those
                        # that involve the less preemptions
                        canfitpX = availpX / need
                        canfitpY = availpY / need
                        preemptX = canfitpX - canfitX
                        preemptY = canfitpY - canfitY
                        if preemptX < preemptY:
                            return BETTER
                        elif preemptX > preemptY:
                            return WORSE
                        else:
                            return EQUAL # No tie breaker
        
        # Order nodes
        nodes.sort(comparenodes)
        return nodes

        
    def scheduleImageTransferEDF(self, res_id, reqTime, deadline, numnodes, imguri, imgsize, imgslot=None):
        # Algorithm for fitting image transfers is essentially the same as 
        # the one used in scheduleVMs. The main difference is that we can
        # scale down the code since we know a priori what slot we're fitting the
        # network transfer in, and the transfers might be moveable (which means
        # we will have to do some Earliest Deadline First magic)
        
        # Estimate image transfer time 
        imgTransferTime = TimeDelta(minutes=15) # We assume 15 minutes for tests
        
        # Find next schedulable transfer time
        # If there are no image transfers in progress, that means now.
        # If there is an image transfer in progress, then that means right after the transfer.
        transferscur = self.resDB.getCurrentAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_RUNNING)
        transfers = transferscur.fetchall()
        if len(transfers) == 0:
            startTime = reqTime
        else:
            startTime = transfers[0]["all_schedend"]
            startTime = ISO.ParseDateTime(startTime)
        
        # Take all the image transfers scheduled from the current time onwards
        # (not including image transfers which have already started)
        transferscur = self.resDB.getFutureAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_PENDING)
        transfers = []
        for t in transferscur:
            transfer={}
            transfer["sl_id"] = t["sl_id"]
            transfer["rsp_id"] = t["rsp_id"]
            transfer["all_schedstart"] = ISO.ParseDateTime(t["all_schedstart"])
            transfer["all_duration"] = TimeDelta(seconds=t["all_duration"])
            # Next line is very kludgy. We should move everything to mx.DateTime for sanity
            transfer["all_deadline"] = ISO.ParseDateTime(t["all_deadline"])
            transfer["new"] = False
            transfers.append(transfer)
            
        newtransfer = {}
        newtransfer["sl_id"] = imgslot
        newtransfer["rsp_id"] = self.resDB.addReservationPart(res_id, "Image transfer", 2)
        newtransfer["all_schedstart"] = None
        newtransfer["all_duration"] = imgTransferTime
        newtransfer["all_deadline"] = deadline
        newtransfer["new"] = True
        transfers.append(newtransfer)

        def comparedates(x,y):
            dx=x["all_deadline"]
            dy=y["all_deadline"]
            if dx>dy:
                return 1
            elif dx==dy:
                # If deadlines are equal, we break the tie by order of arrival
                # (currently, we just check the "new" attribute; in the future, an
                # arrival timestamp might be necessary)
                if not x["new"]:
                    return -1
                elif not y["new"]:
                    return 1
                else:
                    return 0
            else:
                return -1
        
        # Order transfers by deadline
        transfers.sort(comparedates)

        # Compute start times and make sure that deadlines are met
        fits = True
        for transfer in transfers:
             transfer["new_all_schedstart"] = startTime
             transfer["all_schedend"] = startTime + transfer["all_duration"]
             if transfer["all_schedend"] > transfer["all_deadline"]:
                 fits = False
                 break
             startTime = transfer["all_schedend"]
             
        if not fits:
             raise SchedException, "Adding this VW results in an unfeasible image transfer schedule."
 
        # Make changes in database     
        for t in transfers:
            if t["new"]:
                self.resDB.addAllocation(t["rsp_id"], t["sl_id"], t["new_all_schedstart"], t["all_schedend"], 100.0, moveable=True, deadline=t["all_deadline"], duration=t["all_duration"].seconds)
            else:
                self.resDB.updateAllocation(t["sl_id"], t["rsp_id"], t["all_schedstart"], newstart=t["new_all_schedstart"], end=t["all_schedend"])            


    def scheduleImageTransferFIFO(self, res_id, reqTime, numnodes, imguri, imgsize, destinationNode, VMrsp_id, imgslot=None, allocate=True):
        # This schedules image transfers in a FIFO manner, appropriate when there is no
        # deadline for the image to arrive. Unlike EDF, we don't consider the
        # transfer allocations to be moveable. This results in:
        #  - If there is no transfer currently happening, there is no transfer
        #    scheduled in the future.
        #  - If there is a transfer currently in progress, there will be no gaps
        #    between transfers. The next transfer should be scheduled after the
        #    last queued transfer.
        
        # Estimate image transfer time 
        imgTransferTime=self.estimateTransferTime(imgsize)
        
        
        # Find next schedulable transfer time
        # If there are no image transfers in progress, that means now.
        # If there is an image transfer in progress, then that means right after 
        # all the transfers in queue.
        transferscur = self.resDB.getCurrentAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_RUNNING)
        transfers = transferscur.fetchall()
        transferscur = self.resDB.getCurrentAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_PENDING)
        transfers += transferscur.fetchall()
        if len(transfers) == 0:
            # We can schedule the image transfer right now
            startTime = reqTime
        else:
            futuretransferscur = self.resDB.getFutureAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_PENDING)
            futuretransfers = futuretransferscur.fetchall()
            if len(futuretransfers) == 0:
                startTime = transfers[0]["all_schedend"]
            else:
                startTime = futuretransfers[-1]["all_schedend"]
            startTime = ISO.ParseDateTime(startTime)
            
        endTime = startTime+imgTransferTime
        rsp_id = None
        
        if allocate:
            rsp_id = self.resDB.addReservationPart(res_id, "Image transfer for rsp_id=%s" % VMrsp_id, type=2)
            self.resDB.addAllocation(rsp_id, imgslot, startTime, endTime, 100.0, moveable=False)
            self.imagetransfers[rsp_id]=(imguri, imgsize, destinationNode, [VMrsp_id])
        
        return (rsp_id, imgslot, startTime, endTime)


    def preemptResources(self, mustpreempt, time):
        # Given multiple choices, we prefer to preempt VWs that have made the least
        # progress (i.e. percentage of work completed: time-starttime / endtime-starttime)
        # TODO: Make this configurable, and offer better algorithms. For example,
        # the preemptability of a VW should be determined by a combination of:
        # (1) amount of resources it consumes, (2) how long it's been running, and
        # (3) how long it has left to run.
        def comparepreemptability(x,y):
            startX = ISO.ParseDateTime(x["ALL_SCHEDSTART"])
            endX = ISO.ParseDateTime(x["ALL_SCHEDEND"])
            completedX = (time - startX).seconds / (endX - startX).seconds

            startY = ISO.ParseDateTime(y["ALL_SCHEDSTART"])
            endY = ISO.ParseDateTime(y["ALL_SCHEDEND"])
            completedY = (time - startY).seconds / (endY - startY).seconds
            
            if completedX < completedY:
                return BETTER
            elif completedX > completedY:
                return WORSE
            else:
                return EQUAL
        
        # Get allocations at the specified time
        for node in mustpreempt.keys():
            preemptible = {}
            cur = self.resDB.getCurrentAllocationsInNode(time, node, rsp_preemptible=True)
            cur = cur.fetchall()
            for alloc in cur:
                restype=alloc["slt_id"]
                # Make sure this is not a future allocation
                start = ISO.ParseDateTime(alloc["all_schedstart"])
                if start <= time:
                    if not preemptible.has_key(restype):
                        preemptible[restype] = []
                    preemptible[restype].append(alloc)
            
            # Order preemptible resources
            for restype in preemptible.keys():
                preemptible[restype].sort(comparepreemptability)

            resparts = Set()
            respartsinfo = {}
            for restype in mustpreempt[node].keys():
                amountToPreempt = mustpreempt[node][restype]
                for alloc in preemptible[restype]:
                    amount = alloc["all_amount"]
                    amountToPreempt -= amount
                    rsp_key = alloc["rsp_id"]
                    resparts.add(rsp_key)
                    respartsinfo[rsp_key] = alloc
                    if amountToPreempt <= 0:
                        break # Ugh
            
            batchAlgorithm = self.config.get(GENERAL_SEC,BATCHALG_OPT)
            for rsp_key in resparts:
                respart = respartsinfo[rsp_key]
                rsp_name = respart["RSP_NAME"]
                rsp_status = respart["RSP_STATUS"]
                srvlog.info("Preempting resource part %s (%s) with status %i" % (rsp_key, rsp_name, rsp_status))
                
                if batchAlgorithm == "onAR_resubmit":
                    if rsp_status in (STATUS_PENDING, STATUS_RUNNING):
                        self.cancelReservationPart(rsp_key)
                        res_id = respart["RES_ID"]
                        duration = self.batchreservations[res_id][0]
                        resources = self.batchreservations[res_id][1]
                        imgURI = self.batchreservations[res_id][2]
                        imgSize = self.batchreservations[res_id][3]
                        self.batchreservations[res_id][4] += 1
                        nodeName = "VM R%i" % self.batchreservations[res_id][4]
                        self.queueBatchRequest(res_id, duration, resources, nodeName, imgURI, imgSize)
                    elif rsp_status == STATUS_DONE:
                        srvlog.error("Preempting a VW that is already done. This should not be happening.")
                elif batchAlgorithm == "onAR_suspend":
                    if rsp_status in (STATUS_PENDING, STATUS_RUNNING, STATUS_SUSPENDED):
                        self.rescheduleReservationPartForSuspension(rsp_key, time)
                    elif rsp_status == STATUS_DONE:
                        srvlog.error("Preempting a VW that is already done. This should not be happening.")

    def rescheduleReservationPartForSuspension(self, rsp_id, time):
        cur = self.resDB.getCurrentAllocationsInRespart(time, rsp_id)
        cur = cur.fetchall()
        for alloc in cur:
            sl_id = alloc["SL_ID"]
            rsp_id = alloc["RSP_ID"]
            all_schedstart = ISO.ParseDateTime(alloc["ALL_SCHEDSTART"])
            res_needed = alloc["ALL_AMOUNT"]
            time_needed = ISO.ParseDateTime(alloc["ALL_SCHEDEND"]) - time

            self.resDB.suspendAllocation(sl_id, rsp_id, all_schedstart, time, None)
            suspendpoints = self.findSuspendPoints(all_schedstart,sl_id,res_needed,time_needed,suspensiontime=time)
                
            current = suspendpoints.pop(0)
            if current[0] == current[1]:
                # If the rescheduling results in the suspension time of the first allocation
                # being equal to its start time, that means we are preempting a reservation
                # part right at the beginning. We need to eliminate this first allocation
                # as it is pointless.
                self.resDB.removeAllocation(rsp_id, sl_id, all_schedstart)
            else:
                self.resDB.suspendAllocation(sl_id, rsp_id, all_schedstart, current[1], current[2])
            for point in suspendpoints:
                self.resDB.addAllocation(rsp_id, sl_id, point[0], point[1], res_needed, nextstart=point[2])

        # TODO: Make sure all slots in a reservation part can be satisfied at the same time
        # in the same interval (i.e. avoid situations where, after a suspend, there is enough
        # CPU but not enough memory... the current implementation will not detect that)
        # Nonetheless, it should simply be a matter of merging the "suspendpoints" of each 
        # slot

    def findSuspendPoints(self,starttime,sl_id,res_needed,time_needed,suspensiontime=None):
        suspendpoints = [[starttime,suspensiontime,None]] # We don't know the next start time yet
        suspended = (suspensiontime != None)
        if suspended:
            changepoints = self.resDB.findChangePoints(suspensiontime, slot=sl_id, closed=False)
        else:
            changepoints = self.resDB.findChangePoints(starttime, slot=sl_id, closed=False)
        for point in changepoints:
            changetime = ISO.ParseDateTime(point["time"])
            cur = self.resDB.findAvailableSlots(time=changetime, slots=[sl_id])
            avail = cur.fetchone()["available"]
            if suspended and avail >= res_needed:
                # We have reached a point with enough resources. We can resume the VM.
                suspendpoints[-1][2] = changetime
                # We don't know when we will suspend/resume again
                suspendpoints.append([changetime, None, None])
                suspended = False
            elif not suspended and avail < res_needed:
                # We have reached a point where we might have to suspend.
                # Check if we can fit all remaning time here, or if we'll 
                # need to suspend/resume once more
                timediff = changetime - suspendpoints[-1][0]
                if time_needed - timediff > 0:
                    # We'll need to partitions more. Try to fit as much as possible.
                    suspendpoints[-1][1] = changetime
                    suspendpoints[-1][2] = None # We don't know resume time yet
                    time_needed -= timediff
                    suspended = True
                else:
                    # We can fit the entire virtual resource in this interval
                    suspendpoints[-1][1] = suspendpoints[-1][0] + time_needed
                    suspendpoints[-1][2] = None 
                    break  # Ugh
            
        # At this point, we have "infinite" time at our disposal
        # So, if there's still time left over...
        if time_needed > 0:
            laststart = suspendpoints[-1][0]
            end = laststart + time_needed
            suspendpoints[-1][1]=end
        
        return suspendpoints

    def findEarliestStartingTimes(self, imageURI, imageSize, time):
        avoidredundant = self.config.getboolean(GENERAL_SEC, REDUNDANT_OPT)
        cache = self.config.getboolean(GENERAL_SEC, CACHE_OPT)

        # Figure out starting time assuming we have to transfer the image
        transfer = self.scheduleImageTransferFIFO(res_id=None, reqTime=time, numnodes=1, imguri=imageURI, imgsize=imageSize, imgslot=self.imagenodeslot_batch, VMrsp_id=None, destinationNode=None, allocate=False)
        startTime = transfer[3]
        
        earliest = {}
        for node in range(self.getNumNodes()):
            earliest[node+1] = (startTime, TRANSFER_REQUIRED, None)
        
        if cache:
            nodeswithcached = self.backend.getNodesWithCachedImg(imageURI)
            for node in nodeswithcached:
                earliest[node] = (time, TRANSFER_CACHED, None) 
                

        
        if avoidredundant:
            cur = self.resDB.getCurrentAllocationsInSlot(self.getTime(), self.imagenodeslot_batch)
            transfers = cur.fetchall()
            for t in transfers:
                transferImg = self.imagetransfers[t["RSP_ID"]][0]
                node = self.imagetransfers[t["RSP_ID"]][2]
                if transferImg == imageURI:
                    startTime = ISO.ParseDateTime(t["ALL_SCHEDEND"])
                    if startTime < earliest[node]:
                        earliest[node] = (startTime, TRANSFER_REUSE, t["RSP_ID"])

        return earliest
                    

            
    def estimateTransferTime(self, imgsize):
        return TimeDelta(seconds=50) # We assume 15 minutes for tests

    def startReservation(self, res_id, row=None):
        if row != None:
            srvlog.info( "%s: Starting reservation '%s'" % (self.getTime(), row["RES_NAME"]))
        self.resDB.updateReservationStatus(res_id, STATUS_RUNNING)

    def stopReservation(self, res_id, row=None):
        if row != None:
            srvlog.info( "%s: Stopping reservation '%s'" % (self.getTime(), row["RES_NAME"]))
        self.resDB.updateReservationStatus(res_id, STATUS_DONE)
        if self.batchreservations.has_key(res_id):
            self.batchcompletednum += 1
            self.batchcompleted.append((self.getTime(), self.batchcompletednum))
            del self.batchreservations[res_id]
    
    def startReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Starting reservation part %i '%s' of reservation %s" % (self.getTime(), respart_id, row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_RUNNING)

    def startAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_RUNNING, respart=respart_id, start=(time,time+td))

    def stopAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_DONE, respart=respart_id, end=(time,time+td))

    def stopReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Stopping reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_DONE)
        if self.imagetransfers.has_key(respart_id):
            # This is an image transfer. Notify the backend that it is done.
            imgURI = self.imagetransfers[respart_id][0]
            imgSize = self.imagetransfers[respart_id][1]
            nod_id = self.imagetransfers[respart_id][2]
            VMrsp_ids = self.imagetransfers[respart_id][3]
            for VMrsp_id in VMrsp_ids:
                self.backend.completedImgTransferToNode(nod_id, imgURI, imgSize, VMrsp_id)

    def suspendReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Suspending reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_SUSPENDED)

    def resumeReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Resuming reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_RUNNING)

    def cancelReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Cancelling reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        else:
            srvlog.info( "%s: Cancelling reservation part %i" % (self.getTime(), respart_id))
        self.resDB.removeReservationPart(respart_id)

    def isReservationDone(self, res_id):
        return self.resDB.isReservationDone(res_id)

    def isReservationPartEnd(self, respart_id, time, td):
        # We don't support changing allocation sizes yet
        return True

    def resizeAllocation(self, respart_id, row=None):
        srvlog.error( "An allocation has to be resized here. This should not be happening!")


class SimulatingServer(BaseServer):
    def __init__(self, config, resDB, trace, commit):
        BaseServer.__init__(self, config, resDB, trace, commit)
        self.numnodes = None
        self.backend = None
        self.createDatabase()
        self.createBackend()
        self.time = None
        if self.config.has_option(SIMULATION_SEC,STARTTIME_OPT):
            self.time = ISO.ParseDateTime(self.config.get(SIMULATION_SEC,STARTTIME_OPT))
        else:
            self.time = DateTime.now()
        self.startTime = None
        
    def createDatabase(self):
        numnodes = self.config.getint(SIMULATION_SEC,NODES_OPT)
        resources = self.config.get(SIMULATION_SEC,RESOURCES_OPT).split(";")
        bandwidth = self.config.getint(SIMULATION_SEC, BANDWIDTH_OPT)
        
        slottypes = []
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            slottypes.append((self.resDB.getSlotTypeID(resourcename), resourcecapacity))
            
        # Create nodes
        for node in range(numnodes):
            nod_id = self.resDB.addNode("fakenode-%i.mcs.anl.gov" % (node+1))
            # Create slots
            for slottype in slottypes:
                self.resDB.addSlot(nod_id,slt_id=slottype[0],sl_capacity=slottype[1])
                
        # Create image nodes
        nod_id = self.resDB.addNode("fakeimagenode-ar.mcs.anl.gov")
        sl_id = self.resDB.addSlot(nod_id,slt_id=SLOTTYPE_OUTNET,sl_capacity=bandwidth)
        
        self.imagenodeslot_ar = sl_id

        nod_id = self.resDB.addNode("fakeimagenode-batch.mcs.anl.gov")
        sl_id = self.resDB.addSlot(nod_id,slt_id=SLOTTYPE_OUTNET,sl_capacity=bandwidth)
        
        self.imagenodeslot_batch = sl_id
        self.numnodes = numnodes
        self.resDB.commit()

    def createBackend(self):
        numnodes = self.config.getint(SIMULATION_SEC,NODES_OPT)
        caching = self.config.getboolean(GENERAL_SEC, CACHE_OPT)
        
        if caching:
            maxCacheSize = self.config.getint(GENERAL_SEC, MAXCACHESIZE_OPT)
        else:
            maxCacheSize = None
            
        self.backend=SimulationControlBackend(numnodes, caching, maxCacheSize)
        
    def start(self):
        self.startTime = self.time
        
        srvlog.info("Starting")          
        td = TimeDelta(minutes=1)
        self.accepted.append((self.startTime, self.acceptednum))
        self.rejected.append((self.startTime, self.rejectednum))
        self.batchcompleted.append((self.startTime, self.batchcompletednum))
        
        while self.resDB.existsRemainingReservations(self.time) or len(self.trace.entries) > 0 or len(self.batchqueue) > 0:
            if len(self.trace.entries) > 0:
                delta = self.time - self.startTime
                self.processTraceRequests(delta)
            if len(self.batchqueue) > 0 and self.time.second == 0:
                self.processQueue()
            # Temporary fix for incorrect processing of reservations when
            # there is a reservation that both starts and ends inside the delta
            self.processReservations(self.time, td)
            self.processReservations(self.time, td)
            self.time = self.time + td
            
        self.accepted.append((self.time, self.acceptednum))
        self.rejected.append((self.time, self.rejectednum))
        
        print "Number of accepted requests: %i" % self.acceptednum
        print "Number of rejected requests: %i" % self.rejectednum
        self.backend.printNodes()
#        srvlog.info("Number of accepted requests: %i" % len(self.accepted))
#        srvlog.info("Accepted requests: %s" % self.accepted )
#        srvlog.info("Number of rejected requests: %i" % len(self.rejected))
#        srvlog.info("Rejected requests: %s" % self.rejected )

    def getNumNodes(self):
        return self.numnodes        
        
    def getTime(self):
        return self.time

    def stop(self):
        pass

    def generateUtilizationStats(self, start, end):
        changepoints = self.resDB.findChangePoints(start, end)
        counting = False
        accumUtil=0
        prevTime = None
        startVM = None
        stats = []
        for point in changepoints:
            cur = self.resDB.getUtilization(point["time"], type=SLOTTYPE_CPU)
            totalcapacity = 0
            totalused =0
            for row in cur:
                totalcapacity += row["sl_capacity"]
                totalused += row["used"]
            utilization = totalused / totalcapacity
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
            stats.append((seconds, utilization, average))
            prevTime = time
            prevUtilization = utilization
        
        return stats
        #print "Average utilization (1): %f" % (accumUtil/seconds)
        #print "Average utilization (2): %f" % (accumUtil/(seconds-startVM))


class RealServer(BaseServer):
    pass


if __name__ == "__main__":
    configfile="ears.conf"
    tracefile="test_sdsc.trace"
    file = open (configfile, "r")
    config = ConfigParser.ConfigParser()
    config.readfp(file)    
    s = createEARS(config, tracefile)
    s.start()