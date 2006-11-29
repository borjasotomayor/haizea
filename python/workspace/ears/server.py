from workspace.ears import db
from datetime import datetime, timedelta

STATUS_PENDING = 0
STATUS_RUNNING = 1
STATUS_DONE = 2

class BaseServer(object):
    def __init__(self, resDB):
        self.resDB = resDB
        
    def processReservations(self, time, td):
        # Check for reservations which must start
        allocs = self.resDB.getAllocationsInInterval(time, td, status=STATUS_PENDING)
        some = False
        allocs2 = allocs.fetchall()
        for alloc in allocs2:
            some = True
            # Check to see if this is the start of a reservation
            if alloc["RES_STATUS"] == STATUS_PENDING:
                self.startReservation(alloc["RES_ID"], cursor=alloc)
            # Check to see if this is the start of a reservation part    
            if alloc["RSP_STATUS"] == STATUS_PENDING:    
                # The reservation part hasn't started yet
                self.startReservationPart(alloc["RSP_ID"], cursor=alloc)
            elif alloc["RSP_STATUS"] == STATUS_RUNNING:
                # The reservation part has already started
                # This is a change in resource allocation
                self.resizeAllocation(alloc)
            
        if some: print ""
        # Check for reservations which must end
        allocs = self.resDB.getAllocationsInInterval(time, td, status=STATUS_RUNNING)
        
        #self.resDB.commit()

    def startReservation(self, res_id, cursor=None):
        self.resDB.updateReservationStatus(res_id, STATUS_RUNNING)
        if cursor != None:
            print "%s: Starting reservation '%s'" % (self.getTime(), cursor["RES_NAME"])
        
    
    def startReservationPart(self, respart_id, cursor=None):
        if cursor != None:
            print "%s: Starting reservation part '%s'" % (self.getTime(), cursor["RSP_NAME"])
        self.resDB.updateReservationPartStatus(respart_id, STATUS_RUNNING)

    def resizeAllocation(self, res_id, cursor=None):
        print "An allocation has to be resized here. This should not be happening!"
    

class SimulatingServer(BaseServer):
    def __init__(self, resDB):
        BaseServer.__init__(self, resDB)
        self.time = None
        
    def start(self, forceStartTime=None):
        if forceStartTime == None:
            self.time = datetime.now()
        else:
            self.time = forceStartTime
        td = timedelta(minutes=1)
        while self.resDB.existsRemainingReservations(self.time):
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
    s = SimulatingServer(resDB)
    startTime = datetime(2006, 11, 25, 13, 00, 00) 
    s.start(forceStartTime=startTime)