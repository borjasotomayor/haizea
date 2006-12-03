from workspace.ears import db
from workspace.traces.files import TraceFile, TraceEntryV2
from datetime import datetime, timedelta
from sets import Set
from mx.DateTime import *
from mx.DateTime.ISO import *

STATUS_PENDING = 0
STATUS_RUNNING = 1
STATUS_DONE = 2

SLOTTYPE_CPU=1
SLOTTYPE_MEM=2

SLOT_IMGNODE=5 # This should be fetched from the database

class SchedException(Exception):
    pass

class BaseServer(object):
    def __init__(self, resDB, trace, commit=True):
        self.resDB = resDB
        self.trace = trace
        self.reqnum=4
        self.commit=commit
        self.accepted=[]
        self.rejected=[]
        
    def processReservations(self, time, td):
        # Check for reservations which must end
        rescur = self.resDB.getReservationsWithEndingAllocationsInInterval(time, td, allocstatus=STATUS_RUNNING)
        reservations = rescur.fetchall()
        some = False
        for res in reservations:
            some = True
          
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
                        print "Resource allocation resizing not supported yet"

             # Check to see if this ends the reservation
            if self.isReservationDone(res["RES_ID"]):
                self.stopReservation(res["RES_ID"], row=res)
        if some: print ""

        # Check for reservations which must start
        rescur = self.resDB.getReservationsWithStartingAllocationsInInterval(time, td, allocstatus=STATUS_PENDING)
        reservations = rescur.fetchall()
        some = False
        for res in reservations:
            some = True
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
            
        if some: print ""
        
        if self.commit: self.resDB.commit()

    def processTraceRequests(self, delta):
        seconds = delta.seconds
        reqToProcess = [r for r in self.trace.entries if int(r.fields["time"]) <= seconds]
        newtrace = [r for r in self.trace.entries if int(r.fields["time"]) > seconds]
        
        for r in reqToProcess:
            reqTime = self.getTime() #Should starttime + seconds
            startTime = reqTime + timedelta(seconds=int(r.fields["deadline"]))
            endTime = startTime + timedelta(seconds=int(r.fields["duration"]))
            numNodes = int(r.fields["numNodes"])
            print "%s: Received request for VW %i" % (reqTime, self.reqnum)
            print "\tStart time: %s" % startTime
            print "\tEnd time: %s" % endTime
            print "\tNodes: %i" % numNodes
            
            resources = {SLOTTYPE_CPU: float(r.fields["cpu"]), SLOTTYPE_MEM: float(r.fields["memory"])}
            
            res_id = self.resDB.addReservation("Test Reservation #%i" % self.reqnum)
            
            try:
                self.scheduleVMs(res_id, startTime, endTime, "nfs:///foobar", numnodes=numNodes, resources=resources)
                self.scheduleImageTransfer(res_id, reqTime, startTime, numNodes, None, imgslot=SLOT_IMGNODE)
                if self.commit: resDB.commit()
                self.accepted.append(self.reqnum)
            except SchedException, msg:
                print "Scheduling exception: %s" % msg
                self.rejected.append(self.reqnum)
                self.resDB.rollback()
                
            self.reqnum+=1


                
        self.trace.entries = newtrace
        
    def scheduleVMs(self, res_id, startTime, endTime, imguri, numnodes, resources):
        
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
            
            print "Slot type %i has candidates %s" % (slottype,candidateslots[slottype])
        
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
            print "Node %i has final candidates %s" % (node,candidatenodes[node])


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
            print "This VW is feasible"
            for vwnode in range(0,numnodes):
                rsp_id = self.resDB.addReservationPart(res_id, "VM %i" % (vwnode+1), 1)
                for slottype in slottypes:
                    amount = resources[slottype]
                    sl_id = assignment[vwnode][slottype]
                    self.resDB.addSlot(rsp_id, sl_id, startTime, endTime, amount)
        else:
            raise SchedException
        
    def scheduleImageTransfer(self, res_id, reqTime, deadline, numnodes, imgsize, imgslot=None):
        # Algorithm for fitting image transfers is essentially the same as 
        # the one used in scheduleVMs. The main difference is that we can
        # scale down the code since we know a priori what slot we're fitting the
        # network transfer in, and the transfers might be moveable (which means
        # we will have to do some Earliest Deadline First magic)
        
        # Estimate image transfer time 
        imgTransferTime = timedelta(minutes=15) # We assume 15 minutes for tests
        
        # Find next schedulable transfer time
        # If there are no image transfers in progress, that means now.
        # If there is an image transfer in progress, then that means right after the transfer.
        transferscur = self.resDB.getCurrentAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_RUNNING)
        transfers = transferscur.fetchall()
        if len(transfers) == 0:
            startTime = reqTime
        else:
            startTime = transfers[0]["all_schedend"]
            startTime = datetime(*(ISO.ParseDateTime(startTime).tuple()[:6]))
        
        # Take all the image transfers scheduled from the current time onwards
        # (not including image transfers which have already started)
        transferscur = self.resDB.getFutureAllocationsInSlot(reqTime, imgslot, allocstatus=STATUS_PENDING)
        transfers = []
        for t in transferscur:
            transfer={}
            transfer["sl_id"] = t["sl_id"]
            transfer["rsp_id"] = t["rsp_id"]
            transfer["all_schedstart"] = datetime(*(ISO.ParseDateTime(t["all_schedstart"]).tuple()[:6]))
            transfer["all_duration"] = timedelta(seconds=t["all_duration"])
            # Next line is very kludgy. We should move everything to mx.DateTime for sanity
            transfer["all_deadline"] = datetime(*(ISO.ParseDateTime(t["all_deadline"]).tuple()[:6]))
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
                self.resDB.addSlot(t["rsp_id"], t["sl_id"], t["new_all_schedstart"], t["all_schedend"], 100.0, moveable=True, deadline=t["all_deadline"], duration=t["all_duration"].seconds)
            else:
                self.resDB.updateAllocation(t["sl_id"], t["rsp_id"], t["all_schedstart"], newstart=t["new_all_schedstart"], end=t["all_schedend"])            

        
    def prioritizenodes(nodeset,candidatenodes,imguri,resources):
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
            print "%s: Starting reservation '%s'" % (self.getTime(), row["RES_NAME"])
        self.resDB.updateReservationStatus(res_id, STATUS_RUNNING)

    def stopReservation(self, res_id, row=None):
        if row != None:
            print "%s: Stopping reservation '%s'" % (self.getTime(), row["RES_NAME"])
        self.resDB.updateReservationStatus(res_id, STATUS_DONE)
    
    def startReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            print "%s: Starting reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname)
        self.resDB.updateReservationPartStatus(respart_id, STATUS_RUNNING)

    def startAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_RUNNING, respart=respart_id, start=(time,time+td))

    def stopAllocations(self, respart_id, time, td):
        self.resDB.updateAllocationStatusInInterval(STATUS_DONE, respart=respart_id, end=(time,time+td))

    def stopReservationPart(self, respart_id, row=None, resname=None):
        if row != None:
            print "%s: Stopping reservation part '%s' of reservation %s" % (self.getTime(), row["RSP_NAME"], resname)
        self.resDB.updateReservationPartStatus(respart_id, STATUS_DONE)

    def isReservationDone(self, res_id):
        return self.resDB.isReservationDone(res_id)

    def isReservationPartEnd(self, respart_id, time, td):
        # We don't support changing allocation sizes yet
        return True

    def resizeAllocation(self, respart_id, row=None):
        print "An allocation has to be resized here. This should not be happening!"
    

class SimulatingServer(BaseServer):
    def __init__(self, resDB, trace, commit):
        BaseServer.__init__(self, resDB, trace, commit)
        self.time = None
        
    def start(self, forceStartTime=None):
        if forceStartTime == None:
            self.time = startTime = datetime.now()
        else:
            self.time = startTime = forceStartTime
            
        td = timedelta(minutes=1)
        
        while self.resDB.existsRemainingReservations(self.time):
            if len(self.trace.entries) > 0:
                delta = self.time - startTime
                self.processTraceRequests(delta)
            self.processReservations(self.time, td)
            self.time = self.time + td

        print ""
        print "Number of accepted requests: %i" % len(self.accepted)
        print "Accepted requests: %s" % self.accepted 
        print ""
        print "Number of rejected requests: %i" % len(self.rejected)
        print "Rejected requests: %s" % self.rejected 

    def getTime(self):
        return self.time


        

    def stop(self):
        pass

class RealServer(BaseServer):
    pass


if __name__ == "__main__":
    dbfile = "/home/borja/files/db/reservations.db"
    resDB = db.SQLiteReservationDB(dbfile)
    
    trace = TraceFile.fromFile("test.trace", entryType=TraceEntryV2)
    
    s = SimulatingServer(resDB, trace, commit=True)
    startTime = datetime(2006, 11, 25, 13, 00, 00) 
    s.start(forceStartTime=startTime)