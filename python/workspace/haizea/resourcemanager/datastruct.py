from workspace.haizea.common.constants import res_str, state_str, rstate_str, DS, RES_STATE_SCHEDULED, RES_STATE_ACTIVE
from workspace.haizea.common.log import edebug
from workspace.haizea.common.utils import roundDateTimeDelta
from operator import attrgetter
leaseID = 1

def getLeaseID():
    global leaseID
    l = leaseID
    leaseID += 1
    return l

def resetLeaseID():
    global leaseID
    leaseID = 1

def prettyNodemap(nodes):
    pnodes = list(set(nodes.values()))
    normmap = [([y[0] for y in nodes.items() if y[1]==x], x) for x in pnodes]
    for m in normmap: m[0].sort()
    s = "   ".join([", ".join(["V"+`y` for y in x[0]])+" -> P" + `x[1]` for x in normmap])
    return s

class LeaseBase(object):
    def __init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq):
        self.leaseID = getLeaseID()
        self.tSubmit = tSubmit
        self.state = None
        self.vmimage = vmimage
        self.vmimagesize = vmimagesize
        self.numnodes = numnodes
        self.resreq = resreq
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
    def printContents(self):
        edebug("Lease ID       : %i" % self.leaseID, DS, None)
        edebug("Submission time: %s" % self.tSubmit, DS, None)
        edebug("State          : %s" % state_str(self.state), DS, None)
        edebug("VM image       : %s" % self.vmimage, DS, None)
        edebug("VM image size  : %s" % self.vmimagesize, DS, None)
        edebug("Num nodes      : %s" % self.numnodes, DS, None)
        edebug("Resource req   : %s" % "  ".join([res_str(r[0]) + ": " + `r[1]` for r in self.resreq.items()]), DS, None)
        edebug("VM image map   : %s" % self.vmimagemap, DS, None)
        edebug("Mem image map  : %s" % self.memimagemap, DS, None)

    def printRR(self):
        edebug("RESOURCE RESERVATIONS", DS, None)
        edebug("~~~~~~~~~~~~~~~~~~~~~", DS, None)
        for r in self.rr:
            r.printContents()
            edebug("##", DS, None)
            
    def appendRR(self, rr):
        self.rr.append(rr)
        
    def hasStartingReservations(self, time):
        return len(self.getStartingReservations(time)) > 0

    def hasEndingReservations(self, time):
        return len(self.getEndingReservations(time)) > 0

    def getStartingReservations(self, time):
        return [r for r in self.rr if r.start == time and r.state == RES_STATE_SCHEDULED]

    def getEndingReservations(self, time):
        return [r for r in self.rr if (r.end == time or r.realend == time) and r.state == RES_STATE_ACTIVE]
        
class ExactLease(LeaseBase):
    def __init__(self, tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, prematureend = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.start = start
        self.end = end
        self.prematureend = prematureend
        
    def printContents(self):
        edebug("__________________________________________________", DS, None)
        LeaseBase.printContents(self)
        edebug("Type           : EXACT", DS, None)
        edebug("Start time     : %s" % self.start, DS, None)
        edebug("End time       : %s" % self.end, DS, None)
        edebug("Early end time : %s" % self.prematureend, DS, None)
        self.printRR()
        edebug("--------------------------------------------------", DS, None)
        
    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        duration = self.end - self.start
        realduration = self.prematureend - self.start
        self.end = self.start + roundDateTimeDelta(duration * factor)
        self.prematureend = self.start + roundDateTimeDelta(realduration * factor)
        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.maxdur = maxdur
        self.remdur = maxdur
        self.realremdur = realdur
        self.realdur = realdur

    def printContents(self):
        edebug("__________________________________________________", DS, None)
        LeaseBase.printContents(self)
        edebug("Type           : BEST-EFFORT", DS, None)
        edebug("Max duration   : %s" % self.maxdur, DS, None)
        edebug("Rem duration   : %s" % self.remdur, DS, None)
        edebug("Real duration  : %s" % self.realremdur, DS, None)
        self.printRR()
        edebug("--------------------------------------------------", DS, None)

    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        self.maxdur = roundDateTimeDelta(self.maxdur * factor)
        self.remdur = roundDateTimeDelta(self.remdur * factor)
        self.realremdur = roundDateTimeDelta(self.realremdur * factor)
        self.realdur = roundDateTimeDelta(self.realdur * factor)
        
class ResourceReservationBase(object):
    def __init__(self, start, end, db_rsp_ids, realend = None):
        self.start = start
        self.end = end
        self.realend = realend
        self.state = None
        self.db_rsp_ids = db_rsp_ids
        
    def printContents(self):
        edebug("Start          : %s" % self.start, DS, None)
        edebug("End            : %s" % self.end, DS, None)
        edebug("Real End       : %s" % self.realend, DS, None)
        edebug("State          : %s" % rstate_str(self.state), DS, None)
        
class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, transfers):
        ResourceReservationBase.__init__(self, start, end)
        self.transfers = transfers

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : FILE TRANSFER", DS, None)
        edebug("Transfers      : %s" % self.transfers, DS, None)

                
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, realend, nodes, oncomplete, backfillres, db_rsp_ids):
        ResourceReservationBase.__init__(self, start, end, db_rsp_ids, realend=realend)
        self.nodes = nodes
        self.oncomplete = oncomplete
        self.backfillres = backfillres

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : VM", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        edebug("On Complete    : %s" % self.oncomplete, DS, None)
        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, db_rsp_ids):
        ResourceReservationBase.__init__(self, start, end, db_rsp_ids)
        self.nodes = nodes

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : SUSPEND", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, db_rsp_ids):
        ResourceReservationBase.__init__(self, start, end, db_rsp_ids)
        self.nodes = nodes

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : RESUME", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
class Queue(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.q = []
        
    def isEmpty(self):
        return len(self.q)==0
    
    def enqueue(self, r):
        self.q.append(r)
    
    def dequeue(self):
        return self.q.pop(0)
    
    def enqueueInOrder(self, r):
        self.q.append(r)
        self.q.sort(key=attrgetter("tSubmit"))
        
class LeaseTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.entries = {}
        
    def getLease(self, leaseID):
        return self.entries[leaseID]
    
    def isEmpty(self):
        return len(self.entries)==0
    
    def remove(self, lease):
        del self.entries[lease.leaseID]
        
    def add(self, lease):
        self.entries[lease.leaseID] = lease
        
    def getLeases(self, type=None):
        if type==None:
            return self.entries.values()
        else:
            return [e for e in self.entries.values() if isinstance(e,type)]
        
    def getLeaseFromRSPIDs(self, rsp_ids):
        ids = set(rsp_ids)
        leases = []
        for l in self.entries.values():
            for rr in l.rr:
                if len(ids & set(rr.db_rsp_ids)) > 0:
                    leases.append(l)
                    break
        return leases
    
    def getNextLeasesScheduledInNodes(self, time, nodes):
        nodes = set(nodes)
        leases = []
        earliestEndTime = {}
        for l in self.entries.values():
            start = l.rr[-1].start
            nodes2 = set(l.rr[-1].nodes.values())
            if len(nodes & nodes2) > 0 and start > time:
                leases.append(l)
                end = l.rr[-1].end
                for n in nodes2:
                    if not earliestEndTime.has_key(n):
                        earliestEndTime[n] = end
                    else:
                        if end < earliestEndTime[n]:
                            earliestEndTime[n] = end
        leases2 = set()
        for n in nodes:
            if earliestEndTime.has_key(n):
                end = earliestEndTime[n]
                l = [l for l in leases if n in l.rr[-1].nodes.values() and l.rr[-1].start < end]
                leases2.update(l)
        return list(leases2)

