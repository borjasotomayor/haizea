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