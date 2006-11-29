from pysqlite2 import dbapi2 as sqlite


class ReservationDB(object):
    def __init__(self):
        pass
    
    def printReservations(self):
        conn = self.getConn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM TB_ALLOC")
        
        for row in cur:
            print row
    
    def getConn(self):
        return None
    
    def existsRemainingReservations(self, afterTime):
        sql = "SELECT COUNT(*) FROM tb_alloc WHERE all_schedend > ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (afterTime,))
        count = cur.fetchone()[0]

        if count == 0:
            return False
        else:
            return True

    def getAllocationsInInterval(self, time, td, status=None):
        sql = """SELECT * FROM v_allocation WHERE 
        all_schedstart >= ? and all_schedstart < ?"""

        if status != None:
            sql += " and all_status = %i" % status
        
        cur = self.getConn().cursor()
        cur.execute(sql, (time, time+td))
        
        return cur
    
    def updateReservationStatus(self, res_id, status):
        sql = "UPDATE TB_RESERVATION SET RES_STATUS = ? WHERE RES_ID = ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (status,res_id))

    
    def updateReservationPartStatus(self, respart_id, status):
        sql = "UPDATE TB_RESPART SET RSP_STATUS = ? WHERE RSP_ID = ?"
        cur = self.getConn().cursor()
        cur.execute(sql, (status,respart_id))

    def commit(self):
        self.getConn().commit()
    
class SQLiteReservationDB(ReservationDB):
    def __init__(self, dbfile):
        self.conn = sqlite.connect(dbfile)
        self.conn.row_factory = sqlite.Row
        
    def getConn(self):
        return self.conn