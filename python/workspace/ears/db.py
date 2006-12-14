from pysqlite2 import dbapi2 as sqlite
from mx.DateTime import *
from mx.DateTime.ISO import *
from workspace.ears import srvlog, reslog

def adapt_datetime(datetime):
    return ISO.str(datetime)

sqlite.register_adapter(DateTimeType, adapt_datetime)

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

    def getAllocationsInInterval(self, time, eventfield, td=None, distinct=None, allocstatus=None, res=None, nod_id=None, sl_id=None, rsp_preemptible=None, view="v_allocation"):
        if distinct==None:
            sql = "SELECT * FROM %s" % view
        else:
            sql = "SELECT DISTINCT"
            for field in distinct:
                sql += " %s," % field
            sql=sql[:-1]
            sql += " FROM %s" % view
                
        if td != None:
            sql += " WHERE %s >= ? AND %s < ?" % (eventfield, eventfield)        
        else:
            sql += " WHERE %s >= ? " % (eventfield)        

        if allocstatus != None:
            sql += " AND all_status = %i" % allocstatus

        if res != None:
            sql += " AND res_id = %i" % res

        if sl_id != None:
            sql += " AND sl_id = %i" % sl_id

        if nod_id != None:
            sql += " AND nod_id = %i" % nod_id

        if rsp_preemptible != None:
            sql += " AND rsp_preemptible = %i" % rsp_preemptible

        cur = self.getConn().cursor()
        if td != None:
            cur.execute(sql, (time, time+td))
        else:
            cur.execute(sql, (time,))

        return cur        

    def findAvailableSlots(self, time, amount=None, type=None, slots=None, closed=True, canpreempt=False):
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
        
       # If we can preempt slots, then we're only interested in finding out
       # the available resource when ONLY counting the non-preemptible slots
       # (as we will be able to preempt all others if necessary)
       
        if canpreempt:
            preemptfilter = " and rsp_preemptible = 0"
        else:
            preemptfilter = ""

        sql += preemptfilter
        
        sql += " group by sl_id" 
        
        if amount != None:
            sql += " having available >= %f" % amount

        # And add slots which are completely free
        sql += " union select nod_id as nod_id, sl_id as sl_id, sl_capacity as available from v_allocslot va"
        sql += " where %s" % filter
        sql += """ and not exists 
        (select * from v_allocslot a 
         where a.sl_id=va.sl_id %s and  
         ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND)""" % (preemptfilter, gt)
        # print sql
        # print time
        cur = self.getConn().cursor()
        cur.execute(sql, (time, time, time, time))
        
        return cur          

    def findChangePoints(self, start, end, slot=None, closed=True):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
        
        if slot != None:
            filter = "and sl_id = %i" % slot 
        else:
            filter = ""
        
        sql = """select distinct all_schedstart as time from tb_alloc where
        all_schedstart %s ? and all_schedstart %s ? %s
        union select distinct all_schedend as time from tb_alloc where
        all_schedend %s ? and all_schedend %s ? %s"""  % (gt,lt,filter,gt,lt,filter)

        cur = self.getConn().cursor()
        cur.execute(sql, (start, end, start, end))
        
        return cur

    def getUtilization(self, time, type=None, slots=None):
        # Select slots which are partially occupied at that time
        sql = """select nod_id, sl_id, sl_capacity, sum(all_amount) as used  
        from v_allocslot where ? >= ALL_SCHEDSTART AND ? < ALL_SCHEDEND"""

        if type != None:
            filter = "slt_id = %i" % type
        if slots != None:
            filter = "sl_id in (%s)" % slots.__str__().strip('[]') 
       
        sql += " AND %s" % filter
        
        sql += " group by sl_id"

        # And add slots which are completely free
        sql += " union select nod_id as nod_id, sl_id as sl_id, sl_capacity as sl_capacity, 0 as used from v_allocslot va"
        sql += " where %s" % filter
        sql += """ and not exists 
        (select * from tb_alloc a 
         where a.sl_id=va.sl_id and  
         ? >= ALL_SCHEDSTART AND ? < ALL_SCHEDEND)"""

        cur = self.getConn().cursor()
        cur.execute(sql, (time, time, time, time))
        
        return cur           
        
    def getReservationsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedstart", distinct=distinctfields, **kwargs)

    def getResPartsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedstart", distinct=distinctfields, **kwargs)

    def getReservationsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedend", distinct=distinctfields, **kwargs)

    def getResPartsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedend", distinct=distinctfields, **kwargs)

    def getFutureAllocationsInSlot(self, time, sl_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedstart",sl_id=sl_id,**kwargs)

    def getCurrentAllocationsInSlot(self, time, sl_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedend",sl_id=sl_id,**kwargs)

    def getCurrentAllocationsInNode(self, time, nod_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedend",nod_id=nod_id, view="v_allocslot",**kwargs)

    def getImageNodeSlot(self):
        # Ideally, we should do this by flagging nodes as either image nodes or worker nodes
        # For now, we simply seek out the node with the outbound network slot (only the image
        # node will have such a slot in the current implementation)
        sql = "SELECT DISTINCT SL_ID FROM V_ALLOCSLOT WHERE SLT_ID=4" # Hardcoding BAD!
        cur = self.getConn().cursor()
        cur.execute(sql)

        return cur.fetchone()[0]
    
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

    def updateAllocation(self, sl_id, rsp_id, all_schedstart, newstart=None, end=None):
        srvlog.info( "Updating allocation %i,%i beginning at %s with start time %s and end time %s" % (sl_id, rsp_id, all_schedstart, newstart, end))
        sql = """UPDATE tb_alloc 
        SET all_schedstart=?, all_schedend=?
        WHERE sl_id = ? AND rsp_id = ? AND all_schedstart = ?"""
        cur = self.getConn().cursor()
        cur.execute(sql, (newstart,end,sl_id,rsp_id,all_schedstart))

    def addReservation(self, name):
        sql = "INSERT INTO tb_reservation(res_name,res_status) values (?,0)"
        cur = self.getConn().cursor()
        cur.execute(sql, (name,))
        res_id=cur.lastrowid
        return res_id
    
    def addReservationPart(self, res_id, name, type, preemptible=False):
        sql = "INSERT INTO tb_respart(rsp_name,res_id,rspt_id,rsp_status, rsp_preemptible) values (?,?,?,0,?)"
        cur = self.getConn().cursor()
        cur.execute(sql, (name,res_id,type, preemptible))
        rsp_id=cur.lastrowid
        return rsp_id
    
    def addAllocation(self, rsp_id, sl_id, startTime, endTime, amount, moveable=False, deadline=None, duration=None):
        srvlog.info( "Reserving %f in slot %i from %s to %s" % (amount, sl_id, startTime, endTime))
        sql = "INSERT INTO tb_alloc(rsp_id,sl_id,all_schedstart,all_schedend,all_amount,all_moveable,all_deadline,all_duration,all_status) values (?,?,?,?,?,?,?,?,0)"
        cur = self.getConn().cursor()
        cur.execute(sql, (rsp_id, sl_id, startTime, endTime, amount, moveable, deadline, duration))            

    def addNode(self, nod_hostname, nod_enabled=True):
        sql = "INSERT INTO tb_node(nod_hostname, nod_enabled) VALUES (?,?)"
        cur = self.getConn().cursor()
        cur.execute(sql, (nod_hostname, nod_enabled))
        return cur.lastrowid

    def addSlot(self, nod_id,slt_id,sl_capacity):
        sql = "INSERT INTO tb_slot(nod_id, slt_id, sl_capacity) VALUES (?,?,?)"
        cur = self.getConn().cursor()
        cur.execute(sql, (nod_id,slt_id,sl_capacity))
        return cur.lastrowid
    
    def removeReservationPart(self, rsp_id):
        sql = "DELETE FROM tb_respart WHERE rsp_id=?"
        cur = self.getConn().cursor()
        cur.execute(sql, (rsp_id,))
        sql = "DELETE FROM tb_alloc WHERE rsp_id=?"
        cur.execute(sql, (rsp_id,))


    def isReservationDone(self, res_id):
        sql = "SELECT COUNT(*) FROM V_ALLOCATION WHERE res_id=? AND rsp_status in (0,1)" # Hardcoding bad!
        cur = self.getConn().cursor()
        cur.execute(sql, (res_id,))
        
        count = cur.fetchone()[0]

        if count == 0:
            return True
        else:
            return False

    def getSlotTypeID(self, resourcename):
        sql = "SELECT SLT_ID FROM TB_SLOT_TYPE WHERE SLT_NAME=?"
        cur = self.getConn().cursor()
        cur.execute(sql, (resourcename,))
        
        return cur.fetchone()[0]
    
    


    def commit(self):
        self.getConn().commit()

    def rollback(self):
        self.getConn().rollback()
    
class SQLiteReservationDB(ReservationDB):
    def __init__(self, conn):
        self.conn = conn
        self.conn.row_factory = sqlite.Row
        
    def getConn(self):
        return self.conn
    
    @classmethod
    def dump(cls, templatedb, targetdb):
        conn = sqlite.connect(targetdb, detect_types=sqlite.PARSE_DECLTYPES)
        cur = conn.cursor()
        cur.execute("attach '%s' as __templatedb" % templatedb)
        
        cur.execute("select name, sql from __templatedb.sqlite_master where type='table'")
        tables = cur.fetchall()
        for table in tables:
            # Poor man's "drop table if exists". Can be removed with sqlite 3.3
            try:
                cur.execute("drop table main.%s" % table[0])
            except:
                pass
            cur.execute(table[1])
            cur.execute("insert into main.%s select * from __templatedb.%s" % (table[0], table[0]))
        
        cur.execute("select name,sql from __templatedb.sqlite_master where type='view'")
        views = cur.fetchall()
        for view in views:
            try:
                cur.execute("drop view main.%s" % view[0])
            except:
                pass
            cur.execute(view[1])

        cur.execute("detach __templatedb")
        conn.commit()
        
        return cls(conn)
    
    @classmethod
    def toMemFromFile(cls, templatedb):
        return cls.dump(templatedb, ":memory:")
    
    @classmethod
    def toFileFromFile(cls, templatedb, targetdb):
        return cls.dump(templatedb, targetdb)
    