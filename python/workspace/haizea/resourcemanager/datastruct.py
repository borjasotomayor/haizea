from workspace.haizea.common.constants import res_str, state_str, rstate_str, DS, RES_STATE_SCHEDULED, RES_STATE_ACTIVE
from workspace.haizea.common.log import edebug
from workspace.haizea.common.utils import roundDateTimeDelta
from operator import attrgetter
from mx.DateTime import TimeDelta
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
    def __init__(self, scheduler, tSubmit, vmimage, vmimagesize, numnodes, resreq):
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
        self.scheduler = scheduler
        
    def printContents(self, logfun=edebug):
        logfun("Lease ID       : %i" % self.leaseID, DS, None)
        logfun("Submission time: %s" % self.tSubmit, DS, None)
        logfun("State          : %s" % state_str(self.state), DS, None)
        logfun("VM image       : %s" % self.vmimage, DS, None)
        logfun("VM image size  : %s" % self.vmimagesize, DS, None)
        logfun("Num nodes      : %s" % self.numnodes, DS, None)
        logfun("Resource req   : %s" % "  ".join([res_str(r[0]) + ": " + `r[1]` for r in self.resreq.items()]), DS, None)
        logfun("VM image map   : %s" % prettyNodemap(self.vmimagemap), DS, None)
        logfun("Mem image map  : %s" % prettyNodemap(self.memimagemap), DS, None)

    def printRR(self, logfun=edebug):
        logfun("RESOURCE RESERVATIONS", DS, None)
        logfun("~~~~~~~~~~~~~~~~~~~~~", DS, None)
        for r in self.rr:
            r.printContents(logfun)
            logfun("##", DS, None)
            
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
    
    def getLastVMRR(self):
        if isinstance(self.rr[-1],VMResourceReservation):
            return (self.rr[-1], None)
        elif isinstance(self.rr[-1],SuspensionResourceReservation):
            return (self.rr[-2], self.rr[-1])
        
    def getEnd(self):
        vmrr, resrr = self.getLastVMRR()
        return vmrr.end
        
    def nextRRs(self, rr):
        return self.rr[self.rr.index(rr)+1:]

    def prevRR(self, rr):
        return self.rr[self.rr.index(rr)-1]

    
    def removeRR(self, rr):
        if not rr in self.rr:
            raise Exception, "Tried to remove an RR not contained in this lease"
        else:
            rr.removeDBentries()
            self.rr.remove(rr)
        
class ExactLease(LeaseBase):
    def __init__(self, scheduler, tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, prematureend = None):
        LeaseBase.__init__(self, scheduler, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.start = start
        self.end = end
        self.prematureend = prematureend
        
    def printContents(self, logfun=edebug):
        logfun("__________________________________________________", DS, None)
        LeaseBase.printContents(self, logfun)
        logfun("Type           : EXACT", DS, None)
        logfun("Start time     : %s" % self.start, DS, None)
        logfun("End time       : %s" % self.end, DS, None)
        logfun("Early end time : %s" % self.prematureend, DS, None)
        self.printRR(logfun)
        logfun("--------------------------------------------------", DS, None)
        
    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        duration = self.end - self.start
        realduration = self.prematureend - self.start
        self.end = self.start + roundDateTimeDelta(duration * factor)
        self.prematureend = self.start + roundDateTimeDelta(realduration * factor)
        
    def addBootOverhead(self, t):
        self.end += TimeDelta(seconds=t)
        self.prematureend += TimeDelta(seconds=t)
        
class BestEffortLease(LeaseBase):
    def __init__(self, scheduler, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur = None, maxqueuetime=None):
        LeaseBase.__init__(self, scheduler, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.maxdur = maxdur
        self.remdur = maxdur
        self.realremdur = realdur
        self.realdur = realdur
        self.maxqueuetime = maxqueuetime
        self.imagesavail = None

    def printContents(self, logfun=edebug):
        logfun("__________________________________________________", DS, None)
        LeaseBase.printContents(self, logfun)
        logfun("Type           : BEST-EFFORT", DS, None)
        logfun("Max duration   : %s" % self.maxdur, DS, None)
        logfun("Rem duration   : %s" % self.remdur, DS, None)
        logfun("Real duration  : %s" % self.realremdur, DS, None)
        logfun("Max queue time : %s" % self.maxqueuetime, DS, None)
        logfun("Images Avail @ : %s" % self.imagesavail, DS, None)
        self.printRR(logfun)
        logfun("--------------------------------------------------", DS, None)

    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        self.maxdur = roundDateTimeDelta(self.maxdur * factor)
        self.remdur = roundDateTimeDelta(self.remdur * factor)
        self.realremdur = roundDateTimeDelta(self.realremdur * factor)
        self.realdur = roundDateTimeDelta(self.realdur * factor)

    def addBootOverhead(self, t):
        self.maxdur += TimeDelta(seconds=t)
        self.remdur += TimeDelta(seconds=t)
        self.realremdur += TimeDelta(seconds=t)
        self.realdur += TimeDelta(seconds=t)
        
    def mustBeCancelled(self, t):
        if self.maxqueuetime == None:
            return False
        else:
            return t >= self.maxqueuetime
        
    def getSlowdown(self, end, bound=10, runtimeoverhead=None):
        timeOnDedicated = self.maxdur - self.remdur
        if runtimeoverhead != None:
            # Remove runtime overhead
            factor = 1 + float(runtimeoverhead)/100
            timeOnDedicated = roundDateTimeDelta(timeOnDedicated / factor)
        timeOnLoaded = end - self.tSubmit
        bound = TimeDelta(seconds=bound)
        if timeOnDedicated < bound:
            timeOnDedicated = bound
        return timeOnLoaded / timeOnDedicated


        
class ResourceReservationBase(object):
    def __init__(self, lease, start, end, db_rsp_ids, realend = None):
        self.start = start
        self.end = end
        self.realend = realend
        self.lease = lease
        self.state = None
        self.db_rsp_ids = db_rsp_ids
        
    def printContents(self, logfun=edebug):
        logfun("Start          : %s" % self.start, DS, None)
        logfun("End            : %s" % self.end, DS, None)
        logfun("Real End       : %s" % self.realend, DS, None)
        logfun("State          : %s" % rstate_str(self.state), DS, None)
        logfun("RSP_ID         : %s" % self.db_rsp_ids, DS, None)
        
    def removeDBentries(self):
        for rsp_id in self.db_rsp_ids:
            self.lease.scheduler.slottable.removeReservationPart(rsp_id)
            
    def updateDBentry(self):
        self.lease.scheduler.slottable.updateStartTimes(self.db_rsp_ids, self.start)
        self.lease.scheduler.slottable.updateEndTimes(self.db_rsp_ids, self.end)
        
class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start=None, end=None, db_rsp_ids=None):
        ResourceReservationBase.__init__(self, lease, start, end, db_rsp_ids)
        self.deadline = None
        self.file = None
        # Dictionary of  physnode -> [ (leaseID, vnode)* ]
        self.transfers = {}

    def printContents(self, logfun=edebug):
        ResourceReservationBase.printContents(self, logfun)
        logfun("Type           : FILE TRANSFER", DS, None)
        logfun("Deadline       : %s" % self.deadline, DS, None)
        logfun("File           : %s" % self.file, DS, None)
        logfun("Transfers      : %s" % self.transfers, DS, None)
        
    def piggyback(self, leaseID, vnode, physnode):
        if self.transfers.has_key(physnode):
            self.transfers[physnode].append((leaseID, vnode))
        else:
            self.transfers[physnode] = [(leaseID, vnode)]
            

                
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, realend, nodes, oncomplete, backfillres, db_rsp_ids):
        ResourceReservationBase.__init__(self, lease, start, end, db_rsp_ids, realend=realend)
        self.nodes = nodes
        self.oncomplete = oncomplete
        self.backfillres = backfillres

    def printContents(self, logfun=edebug):
        ResourceReservationBase.printContents(self, logfun)
        logfun("Type           : VM", DS, None)
        logfun("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        logfun("On Complete    : %s" % self.oncomplete, DS, None)
        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, nodes, db_rsp_ids):
        ResourceReservationBase.__init__(self, lease, start, end, db_rsp_ids)
        self.nodes = nodes

    def printContents(self, logfun=edebug):
        ResourceReservationBase.printContents(self, logfun)
        logfun("Type           : SUSPEND", DS, None)
        logfun("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, nodes, db_rsp_ids):
        ResourceReservationBase.__init__(self, lease, start, end, db_rsp_ids)
        self.nodes = nodes

    def printContents(self, logfun=edebug):
        ResourceReservationBase.printContents(self, logfun)
        logfun("Type           : RESUME", DS, None)
        logfun("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
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
    
    def getNextCancelPoint(self):
        if self.isEmpty():
            return None
        else:
            return min([l.maxqueuetime for l in self.q])
        
    def purgeCancelled(self):
        cancelled = [l.leaseID for l in self.q if l.mustBeCancelled(self.scheduler.rm.time)]
        self.q = [l for l in self.q if not l.mustBeCancelled(self.scheduler.rm.time)]
        return cancelled
        
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

