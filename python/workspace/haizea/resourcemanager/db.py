from pysqlite2 import dbapi2 as sqlite
from mx.DateTime import *
from mx.DateTime import ISO
import workspace.haizea.common.constants as constants
from workspace.haizea.common.log import info, debug, warning

def adapt_datetime(datetime):
    return str(datetime)

sqlite.register_adapter(DateTimeType, adapt_datetime)

class SlotTableDB(object):
    def __init__(self):
        self.changePointCacheDirty = True
        self.nextChangePoints = []
    
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

    def getAllocationsInInterval(self, time, eventfield, td=None, distinct=None, allocstatus=None, res=None, nod_id=None, sl_id=None, rsp_id = None, rsp_preemptible=None, view="v_allocation"):
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

        if rsp_id != None:
            sql += " AND rsp_id = %i" % rsp_id

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

    def findAvailableSlots(self, time, amount=None, type=None, slots=None, node=None, closed=True, canpreempt=False):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
            
        # Select slots which are partially occupied at that time
        sql = """select nod_id, sl_id, slt_id, sl_capacity - sum(all_amount) as available  
        from v_allocslot 
        where ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND""" % (gt)

        filter = ""
        if type != None:
            filter = "slt_id = %i" % type
            if node != None:
                filter += " and nod_id = %i" % node
        elif node != None:
            filter = "nod_id = %i" % node
        if slots != None:
            filter = "sl_id in (%s)" % slots.__str__().strip('[]') 

        if filter != "":
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
        sql += " union select nod_id as nod_id, sl_id as sl_id, slt_id as slt_id, sl_capacity as available from v_allocslot va"
        sql += " where"
        if filter != "":
            sql += " %s and " % filter
        sql += """ not exists 
        (select * from v_allocslot a 
         where a.sl_id=va.sl_id %s and  
         ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND)""" % (preemptfilter, gt)
        #print sql
        # print time
        cur = self.getConn().cursor()
        cur.execute(sql, (time, time, time, time))
        
        return cur          

    def quickFindAvailableSlots(self, time, closed=True, canpreempt=False):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
            
        # Select slots which are partially occupied at that time
        sql = """select nod_id, sl_id, slt_id, sl_capacity - sum(all_amount) as available  
        from v_allocslot2
        where ? %s ALL_SCHEDSTART AND ? < ALL_SCHEDEND""" % (gt)
        
       # If we can preempt slots, then we're only interested in finding out
       # the available resource when ONLY counting the non-preemptible slots
       # (as we will be able to preempt all others if necessary)
       
        if canpreempt:
            preemptfilter = " and rsp_preemptible = 0"
        else:
            preemptfilter = ""

        sql += preemptfilter
        
        sql += " group by sl_id" 

        #print sql
        # print time
        cur = self.getConn().cursor()
        cur.execute(sql, (time, time))
        
        return cur          

    def getSlots(self):
        sql = "select nod_id, sl_id, slt_id, sl_capacity as available from tb_slot"
        cur = self.getConn().cursor()
        cur.execute(sql)
        
        return cur

    def findChangePoints(self, start, end=None, slot=None, slots=None, closed=True, includeReal=False, withSlots=False):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
        
        if slot != None:
            filter = " and sl_id = %i" % slot 
        elif slots!= None:
            filter = " and sl_id in (%s)" % slots.__str__().strip('[]') 
        else:
            filter = ""
        
        field = "all_schedstart as time"
        if withSlots:
            field += ", sl_id"        
        sql = """select distinct %s from tb_alloc where
        all_schedstart %s '%s'""" % (field,gt,start)
        if end != None:
            sql+=" and all_schedstart %s '%s'" % (lt,end)
        sql += filter

        field = "all_schedend as time"
        if withSlots:
            field += ", sl_id"        
        sql += """ union select distinct %s from tb_alloc where
        all_schedend %s '%s'""" % (field,gt,start)
        if end != None:
            sql +=" and all_schedend %s '%s'" % (lt,end) 
        sql+=filter
        if includeReal:
            field = "all_realend as time"
            if withSlots:
                field += ", sl_id"        
            sql += """ union select distinct %s from tb_alloc where
            all_realend %s '%s'""" % (field,gt,start)
            if end != None:
                sql +=" and all_schedend %s '%s'" % (lt,end) 
            sql+=filter
        sql += " order by time"
        cur = self.getConn().cursor()
        cur.execute(sql)

        return cur

    def findChangePointsInNode(self, start, nodes, end=None, closed=True, includeReal=False):
        if closed:
            gt = ">="
            lt = "<="
        else:
            gt = ">"
            lt = "<"
        
        filter = " and nod_id in (%s)" % ",".join([`n` for n in nodes])
        
        
        field = ""
        sql = """select distinct all_schedstart as time from v_allocslot2 where
        all_schedstart %s '%s'""" % (gt,start)
        if end != None:
            sql+=" and all_schedstart %s '%s'" % (lt,end)
        sql += filter
    
        sql += """ union select distinct all_schedend from v_allocslot2 where
        all_schedend %s '%s'""" % (gt,start)
        if end != None:
            sql +=" and all_schedend %s '%s'" % (lt,end) 
        sql+=filter
        
        if includeReal:     
            sql += """ union select distinct all_realend from v_allocslot2 where
            all_realend %s '%s'""" % (gt,start)
            if end != None:
                sql +=" and all_schedend %s '%s'" % (lt,end) 
            sql+=filter
        sql += " order by time"
        cur = self.getConn().cursor()
        cur.execute(sql)
        
        return cur

    
    def popNextChangePoint(self, time):
        next = self.peekNextChangePoint(time)
        if next != None:
            self.nextChangePoints.pop()
        return next
            
    def peekNextChangePoint(self, time):
        if not self.changePointCacheDirty:
            #info("Not Dirty", constants.DB, None)
            if len(self.nextChangePoints)>0:
                return ISO.ParseDateTime(self.nextChangePoints[-1])
            else:
                return None
        else:
            #info("Dirty", constants.DB, None)
            changePoints = [p["time"] for p in self.findChangePoints(time,includeReal=True,closed=False).fetchall()]
            changePoints.reverse()
            if len(changePoints)>0:
                next = ISO.ParseDateTime(changePoints[-1])
            else:
                next = None
            self.nextChangePoints = changePoints
            self.changePointCacheDirty = False
            return next

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
    
    def isFull(self, time, type=None):
        # Select slots which are partially occupied at that time
        sql = """select nod_id, sl_id, sl_capacity - sum(all_amount) as available  
        from v_allocslot where ? >= ALL_SCHEDSTART AND ? < ALL_SCHEDEND"""

        if type != None:
            sql += " AND slt_id = %i" % type
        
        sql += " group by sl_id having available > 0"

        # And add slots which are completely free
        sql += " union select nod_id as nod_id, sl_id as sl_id, sl_capacity as available from v_allocslot va"
        if type != None:
            sql += " where slt_id = %i and " % type
        else:
            sql += " where"
        sql += """ not exists 
        (select * from tb_alloc a 
         where a.sl_id=va.sl_id and  
         ? >= ALL_SCHEDSTART AND ? < ALL_SCHEDEND)"""

        cur = self.getConn().cursor()
        cur.execute(sql, (time, time, time, time))
        
        rows = cur.fetchall()
        
        return len(rows)==0
        
    def getReservationsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedstart", distinct=distinctfields, **kwargs)

    def getResPartsWithStartingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS","RES_ID")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedstart", distinct=distinctfields, **kwargs)

    def getReservationsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedend", distinct=distinctfields, **kwargs)

    def getReservationsWithRealEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RES_ID","RES_NAME","RES_STATUS")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_realend", distinct=distinctfields, **kwargs)

    def getResPartsWithEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS", "RES_ID")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedend", distinct=distinctfields, **kwargs)

    def getResPartsWithFutureAllocations(self, time, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS", "RES_ID")
        return self.getAllocationsInInterval(time,eventfield="all_schedstart", distinct=distinctfields, **kwargs)

    def getResPartsWithRealEndingAllocationsInInterval(self, time, td, **kwargs):
        distinctfields=("RSP_ID","RSP_NAME","RSP_STATUS", "RES_ID")
        return self.getAllocationsInInterval(time,td=td,eventfield="all_realend", distinct=distinctfields, **kwargs)

    def getEndingAllocationsInInterval(self, time, td, rsp_id, **kwargs):
        return self.getAllocationsInInterval(time,td=td,eventfield="all_schedend", rsp_id=rsp_id, **kwargs)

    def getRealEndingAllocationsInInterval(self, time, td, rsp_id, **kwargs):
        return self.getAllocationsInInterval(time,td=td,eventfield="all_realend", rsp_id=rsp_id, **kwargs)

    def getFutureAllocationsInSlot(self, time, sl_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedstart",sl_id=sl_id,**kwargs)

    def getFutureAllocationsInResPart(self, time, rsp_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedstart",rsp_id=rsp_id,**kwargs)

    def getCurrentAllocationsInSlot(self, time, sl_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedend",sl_id=sl_id,**kwargs)

    def getCurrentAllocationsInNode(self, time, nod_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedend",nod_id=nod_id, view="v_allocslot",**kwargs)

    def getCurrentAllocationsInRespart(self, time, rsp_id, **kwargs):
        return self.getAllocationsInInterval(time, td=None, eventfield="all_schedend",rsp_id=rsp_id,**kwargs)

    def getResID(self, rsp_id):
        sql = "SELECT RES_ID FROM TB_RESPART WHERE RSP_ID=%i" % rsp_id
        cur = self.getConn().cursor()
        cur.execute(sql)

        return cur.fetchone()[0]

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

    def updateReservationPartTimes(self, rsp_ids, start, end, realend):
        sql = "UPDATE TB_ALLOC SET all_schedstart = ?, all_schedend = ?, all_realend = ?"
        sql += " WHERE RSP_ID in (%s)" % ",".join([`r` for r in rsp_ids])
        cur = self.getConn().cursor()
        cur.execute(sql, (start,end,realend))    
        self.changePointCacheDirty = True
    
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

    def updateStartTimes(self, respart, start):
        sql = "UPDATE TB_ALLOC SET ALL_SCHEDSTART = ? WHERE "
        sql += " RSP_ID=%i" % respart
                
        cur = self.getConn().cursor()
        cur.execute(sql, (start,))
        self.changePointCacheDirty = True

    def updateEndTimes(self, respart, end):
        sql = "UPDATE TB_ALLOC SET ALL_SCHEDEND = ? WHERE "
        sql += " RSP_ID=%i" % respart
                
        cur = self.getConn().cursor()
        cur.execute(sql, (end,))
        self.changePointCacheDirty = True

    def updateAllocation(self, sl_id, rsp_id, all_schedstart, newstart=None, end=None, realend=None):
        srvlog.info( "Updating allocation %i,%i beginning at %s with start time %s and end time %s" % (sl_id, rsp_id, all_schedstart, newstart, end))
        sql = """UPDATE tb_alloc 
        SET all_schedstart=?, all_schedend=?, all_realend=?
        WHERE sl_id = ? AND rsp_id = ? AND all_schedstart = ?"""
        cur = self.getConn().cursor()
        cur.execute(sql, (newstart,end,realend,sl_id,rsp_id,all_schedstart))
        self.changePointCacheDirty = True

    def endReservationPart(self, rsp_id, newend):
        info( "Updating reservation part %i with new end time %s" % (rsp_id, newend), constants.DB, None)
        sql = """UPDATE tb_alloc 
        SET all_schedend=?
        WHERE rsp_id = ?"""
        cur = self.getConn().cursor()
        cur.execute(sql, (newend,rsp_id))
        self.changePointCacheDirty = True

    def suspendAllocation(self, sl_id, rsp_id, all_schedstart, newend=None, nextstart=None, realend=None):
        srvlog.info( "Updating allocation (sl_id: %i, rsp_id: %i) beginning at %s with end time %s and next start time %s" % (sl_id, rsp_id, all_schedstart, newend, nextstart))
        sql = """UPDATE tb_alloc 
        SET all_schedend=?, all_nextstart=?, all_realend=?
        WHERE sl_id = ? AND rsp_id = ? AND all_schedstart = ?"""
        cur = self.getConn().cursor()
        cur.execute(sql, (newend,nextstart,realend,sl_id,rsp_id,all_schedstart))
        self.changePointCacheDirty = True

    def addReservation(self, res_id, name):
        sql = "INSERT INTO tb_reservation(res_id, res_name,res_status) values (?,?,0)"
        cur = self.getConn().cursor()
        cur.execute(sql, (res_id,name))
        return res_id
    
    def addReservationPart(self, res_id, name, type, preemptible=False):
        sql = "INSERT INTO tb_respart(rsp_name,res_id,rspt_id,rsp_status, rsp_preemptible) values (?,?,?,0,?)"
        cur = self.getConn().cursor()
        cur.execute(sql, (name,res_id,type, preemptible))
        rsp_id=cur.lastrowid
        return rsp_id
    
    def addAllocation(self, rsp_id, sl_id, startTime, endTime, amount, realEndTime=None, realDuration=None, moveable=False, deadline=None, duration=None, nextstart=None):
        info( "Reserving %f in slot %i from %s to %s (rsp_id: %i) [with nextstart=%s]" % (amount, sl_id, startTime, endTime, rsp_id, nextstart), constants.DB, None)
        sql = "INSERT INTO tb_alloc(rsp_id,sl_id,all_schedstart,all_schedend,all_realend,all_amount,all_moveable,all_deadline,all_duration,all_realduration,all_nextstart,all_status) values (?,?,?,?,?,?,?,?,?,?,?,0)"
        cur = self.getConn().cursor()
        cur.execute(sql, (rsp_id, sl_id, startTime, endTime, realEndTime, amount, moveable, deadline, duration, realDuration, nextstart))            
        self.changePointCacheDirty = True

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
        self.changePointCacheDirty = True

    def removeAllocation(self, rsp_id, sl_id, all_schedstart):
        sql = "DELETE FROM tb_alloc WHERE rsp_id=? and sl_id=? and all_schedstart=?"
        cur = self.getConn().cursor()
        cur.execute(sql, (rsp_id,sl_id,all_schedstart))
        self.changePointCacheDirty = True

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
    
class SQLiteSlotTableDB(SlotTableDB):
    def __init__(self, conn):
        self.conn = conn
        self.conn.row_factory = sqlite.Row
        SlotTableDB.__init__(self)
        
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

        cur.execute("select name,sql from __templatedb.sqlite_master where type='index'")
        indices = cur.fetchall()
        for index in indices:
            if index[1] != None:
                try:
                    cur.execute("drop index main.%s" % index[0])
                except:
                    pass
                cur.execute(index[1])

        cur.execute("detach __templatedb")
        conn.commit()
        
        return cls(conn)
    
    @classmethod
    def toMemFromFile(cls, templatedb):
        return cls.dump(templatedb, ":memory:")
    
    @classmethod
    def toFileFromFile(cls, templatedb, targetdb):
        return cls.dump(templatedb, targetdb)
    