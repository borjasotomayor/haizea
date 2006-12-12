import ConfigParser, os
from workspace.ears import db
from workspace.traces.files import TraceFile, TraceEntryV2
from sets import Set
from mx.DateTime import *
from mx.DateTime.ISO import *
from workspace.ears import srvlog, reslog

STATUS_PENDING = 0
STATUS_RUNNING = 1
STATUS_DONE = 2


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


#def str2datetime(str):
#    return datetime(*(ISO.ParseDateTime(str).tuple()[:6]))

def createEARS(configfile, tracefile):
    # Process configuration file
    file = open (configfile, "r")
    config = ConfigParser.ConfigParser()
    config.readfp(file)
    
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
        self.accepted=[]
        self.rejected=[]
        self.batchqueue=[]
        self.imagenodeslot_ar = None
        self.imagenodeslot_batch = None
        
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
                    # An allocation in this reservation part is ending.
                    # Is it the last one?
                    if self.isReservationPartEnd(respart["RSP_ID"], time, td):
                        self.stopReservationPart(respart["RSP_ID"], row=respart, resname=res["RES_NAME"])
                        self.stopAllocations(respart["RSP_ID"], time, td)
                    else:
                        srvlog.warning("Resource allocation resizing not supported yet")

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
        srvlog.info("%s: Received request for VW %i" % (reqTime, self.reqnum))
        srvlog.info("\tStart time: %s" % startTime)
        srvlog.info("\tEnd time: %s" % endTime)
        srvlog.info("\tNodes: %i" % numNodes)
        
        resources = {SLOTTYPE_CPU: float(r.fields["cpu"]), SLOTTYPE_MEM: float(r.fields["memory"])}
        
        res_id = self.resDB.addReservation("Test (AR) Reservation #%i" % self.reqnum)
        
        try:
            self.scheduleVMs(res_id, startTime, endTime, "nfs:///foobar", numnodes=numNodes, resources=resources)
            self.scheduleImageTransferEDF(res_id, reqTime, startTime, numNodes, None, imgslot=self.imagenodeslot_ar)
            if self.commit: self.resDB.commit()
            self.accepted.append(self.reqnum)
        except SchedException, msg:
            srvlog.warning("Scheduling exception: %s" % msg)
            self.rejected.append(self.reqnum)
            self.resDB.rollback()
                
    def processBatchRequest(self, r):
        reqTime = self.getTime() #Should be starttime + second
        duration = TimeDelta(seconds=int(r.fields["duration"]))
        numNodes = int(r.fields["numNodes"])
        srvlog.info("%s: Received batch request for VW %i" % (reqTime, self.reqnum))
        srvlog.info("\tDuration: %s" % duration)
        srvlog.info("\tNodes: %i" % numNodes)
        
        resources = {SLOTTYPE_CPU: float(r.fields["cpu"]), SLOTTYPE_MEM: float(r.fields["memory"])}

        # We create reservation and reservation parts, but we don't make allocations
        # yet (here we only queue requests... processQueue takes care of working
        # through these --and other queued requests-- to see which ones should
        # be dispatched)
        res_id = self.resDB.addReservation("Test (Batch) Reservation #%i" % self.reqnum)
        for vwnode in range(numNodes):
            vw = {}
            vw["res_id"]=res_id
            vw["duration"]=duration
            vw["resources"]=resources
            rsp_id = self.resDB.addReservationPart(res_id, "VM %i" % (vwnode+1), type=1)
            vw["vw_rsp"] = rsp_id
            rsp_id = self.resDB.addReservationPart(res_id, "Image transfer for VM %i" % (vwnode+1), type=2)
            vw["transfer_rsp"] = rsp_id
            self.batchqueue.append(vw)
        
        if self.commit: self.resDB.commit()

    def processQueue(self):
        mustremove = []
        for i,vm in enumerate(self.batchqueue):
            res_id = vm["res_id"]
            duration = vm["duration"]
            resources = vm["resources"]
            
            # Tentatively schedule an image transfer
            # We do this in case it turns out we *have* to transfer this image.
            # This way we can tell what the earliest start time would be.
            # Nonetheless, the slot-fitting algorithm will first try to schedule
            # the VM on a node with a cached image (starting immediately) and,
            # if that is not possible, then will schedule it at the earliest
            # possible time.
            
            transfer = self.scheduleImageTransferFIFO(vm["res_id"], vm["transfer_rsp"], self.getTime(), 1, None, imgslot=self.imagenodeslot_batch)
            startTime = transfer[3]
            
            batchAlgorithm = self.config.get(GENERAL_SEC,BATCHALG_OPT)
            if batchAlgorithm == "nopreemption":
                # Without any preemption support, we only schedule if there is enough
                # resources to run the job from beginning to end (not allowing
                # it to be preempted by an AR). As such, when the VMs are scheduled,
                # they are flagged as non-preemptible
                endTime = startTime + duration
                srvlog.info("Attempting to schedule VM for reservation %i from %s to %s" % (res_id,startTime,endTime))
                try:
                    self.scheduleVMs(res_id, startTime, endTime, "nfs:///foobar", numnodes=1, resources=resources, preemptible=False)
                    if self.commit: self.resDB.commit()
                    mustremove.append(i)
                except SchedException, msg:
                    srvlog.warning("Can't schedule this batch VM now. Reason: %s" % msg)
                    self.resDB.rollback() # This will also roll back the image transfer
            elif batchAlgorithm == "onAR_resubmit":
                # Here we support preemption, but in the form of cancelling a running
                # batch vw and resubmitting it if an AR starts.
                # As in the previous case, we're only interested in accepting a request
                # if we can run it from beginning to end. This algorithm differs in that,
                # if an AR is requested, whatever batch vm is running on that node is
                # resubmitted (in the previous case, batch vms were non-preemptible,
                # which means the AR would have been rejected)
                endTime = startTime + duration
                srvlog.info("Attempting to schedule VM for reservation %i from %s to %s" % (res_id,startTime,endTime))
                try:
                    self.scheduleVMs(res_id, startTime, endTime, "nfs:///foobar", numnodes=1, resources=resources, preemptible=True)
                    if self.commit: self.resDB.commit()
                    mustremove.append(i)
                except SchedException, msg:
                    srvlog.warning("Can't schedule this batch VM now. Reason: %s" % msg)
                    self.resDB.rollback() # This will also roll back the image transfer
            elif batchAlgorithm == "onAR_suspend":
                pass

        newbatchqueue = [vm for i,vm in enumerate(self.batchqueue) if i not in mustremove]

        self.batchqueue = newbatchqueue
        
    def scheduleVMs(self, res_id, startTime, endTime, imguri, numnodes, resources, preemptible=False):
        
        slottypes = resources.keys()
        
        # First, find candidate slots
        candidateslots = {}
        
        # TODO: There are multiple points at which the slot fitting could be aborted
        # for lack of resources. Currently, this is only checked at the very end.
        for slottype in slottypes:
            candidateslots[slottype] = []
            needed = resources[slottype]
            slots = []
            capacity={}
            
            # First sieve: select all slots that have enough resources at the beginning
            slots1 = self.resDB.findAvailableSlots(time=startTime, amount=needed, type=slottype)
            for slot in slots1:
                slot_id = slot["SL_ID"]
                slots.append(slot_id)
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = []
                capacity[slot_id].append((startTime,slot["available"]))

            # Second sieve: remove slots that don't have enough resources at the end
            slots2 = self.resDB.findAvailableSlots(time=endTime, amount=needed, slots=slots, closed=False)
            slots = []
            for slot in slots2:
                nod_id = slot["NOD_ID"]
                slot_id = slot["SL_ID"]
                slots.append((nod_id,slot_id))
                if not capacity.has_key(slot_id):
                    capacity[slot_id] = []
                capacity[slot_id].append((endTime,slot["available"]))

            # Final sieve: Determine "resource change points" and make sure that 
            # there is enough resources at each point too (and determine maximum
            # available)
            for slot in slots:
                slot_id=slot[1]
                changepoints = self.resDB.findChangePoints(startTime, endTime, slot_id, closed=False)
                for point in changepoints:
                    time = point["time"]
                    cur = self.resDB.findAvailableSlots(time=time, slots=[slot_id])
                    avail = cur.fetchone()["available"]
                    capacity[slot_id].append((time,avail))
                    
                maxavail = float("inf")    
                for cap in capacity[slot_id]:
                    if cap[1] < maxavail:
                        maxavail = cap[1]
                        
                if maxavail >= needed:
                    candidateslots[slottype].append([slot[0],slot[1],maxavail])
            
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
        
        # Keep only slots from nodeset
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
        orderednodes = self.prioritizenodes(nodeset,candidatenodes,imguri,resources)
        
        for vwnode in range(0,numnodes):
            assignment[vwnode] = {}
            for physnode in nodeset:
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
                    break # Ouch
            else:
                raise SchedException, "Could not fit node %i in any physical node" % vwnode

        if allfits:
            srvlog.info( "This VW is feasible")
            for vwnode in range(0,numnodes):
                rsp_id = self.resDB.addReservationPart(res_id, "VM %i" % (vwnode+1), 1)
                for slottype in slottypes:
                    amount = resources[slottype]
                    sl_id = assignment[vwnode][slottype]
                    self.resDB.addAllocation(rsp_id, sl_id, startTime, endTime, amount)
        else:
            raise SchedException
        
    def scheduleImageTransferEDF(self, res_id, reqTime, deadline, numnodes, imgsize, imgslot=None):
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


    def scheduleImageTransferFIFO(self, res_id, rsp_id, reqTime, numnodes, imgsize, imgslot=None):
        # This schedules image transfers in a FIFO manner, appropriate when there is no
        # deadline for the image to arrive. Unlike EDF, we don't consider the
        # transfer allocations to be moveable. This results in:
        #  - If there is no transfer currently happening, there is no transfer
        #    scheduled in the future.
        #  - If there is a transfer currently in progress, there will be no gaps
        #    between transfers. The next transfer should be scheduled after the
        #    last queued transfer.
        
        # Estimate image transfer time 
        imgTransferTime = TimeDelta(minutes=15) # We assume 15 minutes for tests
        
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
        
        self.resDB.addAllocation(rsp_id, imgslot, startTime, endTime, 100.0, moveable=False)
        
        return (rsp_id, imgslot, startTime, endTime)


        
    def prioritizenodes(self,nodeset,candidatenodes,imguri,resources):
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
        return nodeset
        

    def startReservation(self, res_id, row=None):
        if row != None:
            srvlog.info( "%s: Starting reservation '%s'" % (self.getTime(), row["RES_NAME"]))
        self.resDB.updateReservationStatus(res_id, STATUS_RUNNING)

    def stopReservation(self, res_id, row=None):
        if row != None:
            srvlog.info( "%s: Stopping reservation '%s'" % (self.getTime(), row["RES_NAME"]))
        self.resDB.updateReservationStatus(res_id, STATUS_DONE)
    
    def startReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Starting reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_RUNNING)

    def startAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_RUNNING, respart=respart_id, start=(time,time+td))

    def stopAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_DONE, respart=respart_id, end=(time,time+td))

    def stopReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            srvlog.info( "%s: Stopping reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname))
        self.resDB.updateReservationPartStatus(respart_id, STATUS_DONE)

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
        self.createDatabase()
        self.time = None
        if self.config.has_option(SIMULATION_SEC,STARTTIME_OPT):
            self.time = ISO.ParseDateTime(self.config.get(SIMULATION_SEC,STARTTIME_OPT))
        else:
            self.time = DateTime.now()
            
        
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
        
        self.resDB.commit()

        
    def start(self):
        startTime = self.time
        
        srvlog.info("Starting")            
        td = TimeDelta(minutes=1)
        
        while self.resDB.existsRemainingReservations(self.time) or len(self.trace.entries) > 0:
            if len(self.trace.entries) > 0:
                delta = self.time - startTime
                self.processTraceRequests(delta)
            if len(self.batchqueue) > 0:
                self.processQueue()
            self.processReservations(self.time, td)
            self.time = self.time + td

        srvlog.info("Number of accepted requests: %i" % len(self.accepted))
        srvlog.info("Accepted requests: %s" % self.accepted )
        srvlog.info("Number of rejected requests: %i" % len(self.rejected))
        srvlog.info("Rejected requests: %s" % self.rejected )

        self.generateUtilizationStats(startTime, self.time)
        
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
            print "%i %f %f" % (seconds, utilization, average)
            prevTime = time
            prevUtilization = utilization
            
        print "Average utilization (1): %f" % (accumUtil/seconds)
        print "Average utilization (2): %f" % (accumUtil/(seconds-startVM))


class RealServer(BaseServer):
    pass


if __name__ == "__main__":
    configfile="ears.conf"
    tracefile="test_mixed.trace"
    s = createEARS(configfile, tracefile)
    s.start()