from workspace.ears import db
from workspace.traces.files import TraceFile
from datetime import datetime, timedelta

STATUS_PENDING = 0
STATUS_RUNNING = 1
STATUS_DONE = 2

class BaseServer(object):
    def __init__(self, resDB, trace):
        self.resDB = resDB
        self.trace = trace
        
    def processReservations(self, time, td):
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
        
        #self.resDB.commit()

    def processTraceRequests(self, delta):
        seconds = delta.seconds
        reqToProcess = [r for r in self.trace.entries if int(r.fields["time"]) <= seconds]
        newtrace = [r for r in self.trace.entries if int(r.fields["time"]) > seconds]
        
        for r in reqToProcess:
            reqTime = self.getTime() #Should starttime + seconds
            startTime = reqTime + timedelta(seconds=int(r.fields["deadline"]))
            endTime = startTime + timedelta(seconds=int(r.fields["duration"]))
            print "%s: Received request for VW" % reqTime
            print "\tStart time: %s" % startTime
            print "\tEnd time: %s" % endTime
            print "\tNodes: %i" % int(r.fields["numNodes"])
            
            self.processRequest(startTime, endTime)
        
        self.trace.entries = newtrace
        
    def processRequest(self, startTime, endTime, mem=512.0, cpu=25.0, imguri, numnodes):
        # Estimate image transfer time
        imgTransferTime = timedelta(minutes=15)
        
        # Let the slot fitting begin!
        
        # Is image transfer feasible?


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
    def __init__(self, resDB, trace):
        BaseServer.__init__(self, resDB, trace)
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

        print "done"

    def getTime(self):
        return self.time


        

    def stop(self):
        pass

class RealServer(BaseServer):
    pass


if __name__ == "__main__":
    dbfile = "/home/borja/files/db/reservations.db"
    resDB = db.SQLiteReservationDB(dbfile)
    
    trace = TraceFile.fromFile("test.trace")
    
    s = SimulatingServer(resDB, trace)
    startTime = datetime(2006, 11, 25, 13, 00, 00) 
    s.start(forceStartTime=startTime)