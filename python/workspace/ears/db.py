from pysqlite2 import dbapi2 as sqlite


class ReservationDB(object):
    def __init__(self):
        pass
    
    def printReservations(self):
        conn = self.getConn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM V_ALLOCATION")
        
        for row in cur:
            print row
    
    def getConn(self):
        return None
    
    def existsRemainingReservations(self, afterTime):
        sql = "SELECT COUNT(*) FROM tb_alloc WHERE all_schedend >= ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (afterTime,))
        count = cur.fetchone()[0]

        if count == 0:
            return False
        else:
            return True

    def getAllocationsInInterval(self, time, td, eventfield, distinct=None, allocstatus=None, res=None):
        if distinct==None:
            sql = "SELECT * FROM v_allocation"
        else:
            sql = "SELECT DISTINCT"
            for field in distinct:
                sql += " %s," % field
            sql=sql[:-1]
            sql += " FROM v_allocation" 
                
        sql += " WHERE %s >= ? AND %s < ?" % (eventfield, eventfield)        

        if allocstatus != None:
            sql += " AND all_status = %i" % allocstatus

        if res != None:
            sql += " AND res_id = %i" % res

        cur = self.getConn().cursor()
        cur.execute(sql, (time, time+td))
        
        return cur        

    def findAvailableSlots(self, time, amount=None, type=None, slots=None, closed=True):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
            
        # Select slots which are partially occupied at that time
        sql = """select nod_id, sl_id, sl_capacity - sum(all_amount) as available  
        from v_allocslot 
        where ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND""" % (gt)

        if type != None:
            filter = "slt_id = %i" % type
        if slots != None:
            filter = "sl_id in (%s)" % slots.__str__().strip('[]') 
       
        sql += " AND %s" % filter
        
        sql += " group by sl_id" 
        
        if amount != None:
            sql += " having available >= %f" % amount

        # And add slots which are completely free
        sql += " union select nod_id as nod_id, sl_id as sl_id, sl_capacity as available from v_allocslot va"
        sql += " where %s" % filter
        sql += """and not exists 
        (select * from tb_alloc a 
         where a.sl_id=va.sl_id and  
         ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND)""" % (gt)

        cur = self.getConn().cursor()
        cur.execute(sql, (time, time, time, time))
        
        return cur          

    def findChangePoints(self, start, end, slot, closed=True):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
        
        sql = """select distinct all_schedstart as time from tb_alloc 
        where sl_id=? and all_schedstart %s ? and all_schedstart %s ?
        union select distinct all_schedend as time from tb_alloc 
        where sl_id=? and all_schedend %s ? and all_schedend %s ?"""  % (gt,lt,gt,lt)

        cur = self.getConn().cursor()
        cur.execute(sql, (slot, start, end, slot, start, end))
        
        return cur
        
        
    def getReservationsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td,"all_schedstart", distinct=distinctfields, **kwargs)

    def getResPartsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS")
        return self.getAllocationsInInterval(time,td,"all_schedstart", distinct=distinctfields, **kwargs)

    def getReservationsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td,"all_schedend", distinct=distinctfields, **kwargs)

    def getResPartsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS")
        return self.getAllocationsInInterval(time,td,"all_schedend", distinct=distinctfields, **kwargs)
    
    def updateReservationStatus(self, res_id, status):
        sql = "UPDATE TB_RESERVATION SET RES_STATUS = ? WHERE RES_ID = ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (status,res_id))

    
    def updateReservationPartStatus(self, respart_id, status):
        sql = "UPDATE TB_RESPART SET RSP_STATUS = ? WHERE RSP_ID = ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (status,respart_id))

    def updateAllocationStatusInInterval(self, status, respart=None, start=None, end=None):
        sql = "UPDATE TB_ALLOC SET ALL_STATUS = ? WHERE "
        if respart != None:
            sql += " RSP_ID=%i" % respart
            
        if start != None:
            sql += " AND all_schedstart >= ? AND all_schedstart < ?"
            interval = start
        elif end != None:
            sql += " AND all_schedend >= ? AND all_schedend < ?"
            interval = end
        
        cur = self.getConn().cursor()
        cur.execute(sql, (status,) + interval)

    def addReservation(self, name):
        #TODO: DB code
        return 0
    
    def addReservationPart(self, res_id, name, type):
        #TODO: DB code
        return 0
    
    def addSlot(self, rsp_id, sl_id, startTime, endTime, amount):
        print "Reserving %f in slot %i from %s to %s" % (amount, sl_id, startTime, endTime)
        #TODO: DB code
    

    def isReservationDone(self, res_id):
        sql = "SELECT COUNT(*) FROM V_ALLOCATION WHERE res_id=? AND all_status in (0,1)" # Hardcoding bad!
        cur = self.getConn().cursor()
        cur.execute(sql, (res_id,))
        
        count = cur.fetchone()[0]

        if count == 0:
            return True
        else:
            return False

    def commit(self):
        self.getConn().commit()
    
class SQLiteReservationDB(ReservationDB):
    def __init__(self, dbfile):
        self.conn = sqlite.connect(dbfile)
        self.conn.row_factory = sqlite.Row
        
    def getConn(self):
        return self.conn