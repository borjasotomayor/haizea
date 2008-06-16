from haizea.common.constants import res_str, state_str, rstate_str, DS, RES_STATE_SCHEDULED, RES_STATE_ACTIVE, RES_MEM, MIGRATE_NONE, MIGRATE_MEM, MIGRATE_MEMVM, TRANSFER_NONE
from haizea.common.utils import roundDateTimeDelta
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

def prettyRes(res):
    return "  ".join([res_str(i) + ": " + `x` for i,x in enumerate(res.res)])

class ResourceTuple(object):
    def __init__(self, res):
        self.res = res
        
    @classmethod
    def fromList(cls, l):
        return cls(l[:])

    @classmethod
    def copy(cls, rt):
        return cls(rt.res[:])
        
    def fitsIn(self, res2):
        fits = True
        for i in xrange(len(self.res)):
            if self.res[i] > res2.res[i]:
                fits = False
                break
        return fits
    
    def getNumFitsIn(self, res2):
        canfit = 10000 # Arbitrarily large
        for i in xrange(len(self.res)):
            if self.res[i] != 0:
                f = res2.res[i] / self.res[i]
                if f < canfit:
                    canfit = f
        return canfit
    
    def decr(self, res2):
        for slottype in xrange(len(self.res)):
            self.res[slottype] -= res2.res[slottype]

    def incr(self, res2):
        for slottype in xrange(len(self.res)):
            self.res[slottype] += res2.res[slottype]
            
    def get(self, slottype):
        return self.res[slottype]

    def set(self, slottype, value):
        self.res[slottype] = value
        
    def isZeroOrLess(self):
        return sum([v for v in self.res]) <= 0

class DateTime(object):
    def __init__(self, requested):
        self.requested = requested
        self.scheduled = None
        self.actual = None
        
class Duration(object):
    def __init__(self, requested, known=None):
        self.requested = requested
        self.accumulated = TimeDelta()
        self.actual = None
        # The following two are ONLY used in simulation
        self.known = known
        self.accumknown = TimeDelta()

class LeaseBase(object):
    def __init__(self, tSubmit, diskImageID, diskImageSize, numnodes, resreq):
        self.leaseID = getLeaseID()
        self.enactID = None
        self.tSubmit = tSubmit
        self.state = None
        self.diskImageID = diskImageID
        self.diskImageSize = diskImageSize
        self.numnodes = numnodes
        self.resreq = resreq
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
    def setScheduler(self, scheduler):
        self.scheduler = scheduler
        self.logger = scheduler.rm.logger
        
    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "Lease ID       : %i" % self.leaseID, DS)
        self.logger.log(loglevel, "Submission time: %s" % self.tSubmit, DS)
        self.logger.log(loglevel, "State          : %s" % state_str(self.state), DS)
        self.logger.log(loglevel, "VM image       : %s" % self.diskImageID, DS)
        self.logger.log(loglevel, "VM image size  : %s" % self.diskImageSize, DS)
        self.logger.log(loglevel, "Num nodes      : %s" % self.numnodes, DS)
        self.logger.log(loglevel, "Resource req   : %s" % prettyRes(self.resreq), DS)
        self.logger.log(loglevel, "VM image map   : %s" % prettyNodemap(self.vmimagemap), DS)
        self.logger.log(loglevel, "Mem image map  : %s" % prettyNodemap(self.memimagemap), DS)

    def printRR(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "RESOURCE RESERVATIONS", DS)
        self.logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~", DS)
        for r in self.rr:
            r.printContents(loglevel)
            self.logger.log(loglevel, "##", DS)
            
    def appendRR(self, rr):
        self.rr.append(rr)
        
    def hasStartingReservations(self, time):
        return len(self.getStartingReservations(time)) > 0

    def hasEndingReservations(self, time):
        return len(self.getEndingReservations(time)) > 0

    def getStartingReservations(self, time):
        return [r for r in self.rr if r.start <= time and r.state == RES_STATE_SCHEDULED]

    def getEndingReservations(self, time):
        return [r for r in self.rr if (r.end <= time or r.realend <= time) and r.state == RES_STATE_ACTIVE]
    
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

    def replaceRR(self, rrold, rrnew):
        self.rr[self.rr.index(rrold)] = rrnew
    
    def removeRR(self, rr):
        if not rr in self.rr:
            raise Exception, "Tried to remove an RR not contained in this lease"
        else:
            self.rr.remove(rr)

    def removeRRs(self):
        self.rr = []
        
    def estimateSuspendResumeTime(self):
        rate = self.scheduler.rm.config.getSuspendResumeRate()
        time = float(self.resreq.res[RES_MEM]) / rate
        time = roundDateTimeDelta(TimeDelta(seconds = time))
        return time
    
    # TODO: Should be in common package
    def estimateTransferTime(self, size):
        bandwidth = self.scheduler.rm.config.getBandwidth()
        bandwidthMBs = float(bandwidth) / 8
        seconds = size / bandwidthMBs
        return roundDateTimeDelta(TimeDelta(seconds = seconds)) 
    
    def estimateImageTransferTime(self):
        forceTransferTime = self.scheduler.rm.config.getForceTransferTime()
        if forceTransferTime != None:
            return forceTransferTime
        else:      
            return self.estimateTransferTime(self.diskImageSize)
        
    def estimateMigrationTime(self):
        whattomigrate = self.scheduler.rm.config.getMustMigrate()
        if whattomigrate == MIGRATE_NONE:
            return TimeDelta(seconds=0)
        else:
            if whattomigrate == MIGRATE_MEM:
                mbtotransfer = self.resreq.res[RES_MEM]
            elif whattomigrate == MIGRATE_MEMVM:
                mbtotransfer = self.diskImageSize + self.resreq.res[RES_MEM]
            return self.estimateTransferTime(mbtotransfer)
        
    def getSuspendThreshold(self, initial, migrating=False):
        threshold = self.scheduler.rm.config.getSuspendThreshold()
        if threshold != None:
            # If there is a hard-coded threshold, use that
            return threshold
        else:
            transfertype = self.scheduler.rm.config.getTransferType()
            if transfertype == TRANSFER_NONE:
                deploytime = TimeDelta(seconds=0)
            else: 
                deploytime = self.estimateImageTransferTime()
            # The threshold will be a multiple of the overhead
            if not initial:
                # Overestimating, just in case (taking into account that the lease may be
                # resumed, but also suspended again)
                if migrating:
                    threshold = self.estimateMigrationTime() + self.estimateSuspendResumeTime() * 2
                else:
                    threshold = self.estimateSuspendResumeTime() * 2
            else:
                threshold = self.scheduler.rm.config.getBootOverhead() + deploytime + self.estimateSuspendResumeTime()
            factor = self.scheduler.rm.config.getSuspendThresholdFactor() + 1
            return roundDateTimeDelta(threshold * factor)
            
      
        
class ExactLease(LeaseBase):
    def __init__(self, tSubmit, start, end, diskImageID, diskImageSize, numnodes, resreq, prematureend = None):
        LeaseBase.__init__(self, tSubmit, diskImageID, diskImageSize, numnodes, resreq)
        self.start = start
        self.end = end
        self.prematureend = prematureend
        
    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : EXACT", DS)
        self.logger.log(loglevel, "Start time     : %s" % self.start, DS)
        self.logger.log(loglevel, "End time       : %s" % self.end, DS)
        self.logger.log(loglevel, "Early end time : %s" % self.prematureend, DS)
        self.printRR(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)
    
    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        duration = self.end - self.start
        realduration = self.prematureend - self.start
        self.end = self.start + roundDateTimeDelta(duration * factor)
        self.prematureend = self.start + roundDateTimeDelta(realduration * factor)
        
    def addBootOverhead(self, t):
        self.end += t
        self.prematureend += t
        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, maxdur, diskImageID, diskImageSize, numnodes, resreq, realdur = None, maxqueuetime=None, timeOnDedicated=None):
        LeaseBase.__init__(self, tSubmit, diskImageID, diskImageSize, numnodes, resreq)
        self.maxdur = maxdur   # Maximum duration the lease can run for (this value is not modified)
        self.realdur = realdur # Real duration (this value is not modified)
        self.remdur = maxdur   # Remaining duration (until maximum). This value is decreased as the lease runs.
        self.realremdur = realdur # REAL remaining duration (until reaching realdur).
        self.accumdur = TimeDelta()   # How much duration the lease has accumulated
        self.maxqueuetime = maxqueuetime
        self.timeOnDedicated = timeOnDedicated
        self.imagesavail = None        

    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : BEST-EFFORT", DS)
        self.logger.log(loglevel, "Max duration   : %s" % self.maxdur, DS)
        self.logger.log(loglevel, "Rem duration   : %s" % self.remdur, DS)
        self.logger.log(loglevel, "Real duration  : %s" % self.realremdur, DS)
        self.logger.log(loglevel, "Max queue time : %s" % self.maxqueuetime, DS)
        self.logger.log(loglevel, "Images Avail @ : %s" % self.imagesavail, DS)
        self.printRR(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)

    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        self.maxdur = roundDateTimeDelta(self.maxdur * factor)
        self.remdur = roundDateTimeDelta(self.remdur * factor)
        self.realremdur = roundDateTimeDelta(self.realremdur * factor)
        self.realdur = roundDateTimeDelta(self.realdur * factor)

    def addBootOverhead(self, t):
        self.maxdur += t
        self.remdur += t
        self.realremdur += t
        self.realdur += t
        
    def mustBeCancelled(self, t):
        if self.maxqueuetime == None:
            return False
        else:
            return t >= self.maxqueuetime
        
    def getSlowdown(self, end, bound=10):
        timeOnDedicated = self.timeOnDedicated
        timeOnLoaded = end - self.tSubmit
        bound = TimeDelta(seconds=bound)
        if timeOnDedicated < bound:
            timeOnDedicated = bound
        return timeOnLoaded / timeOnDedicated


        
class ResourceReservationBase(object):
    def __init__(self, lease, start, end, res, realend = None):
        self.start = start
        self.end = end
        self.realend = realend
        self.lease = lease
        self.state = None
        self.res = res
        self.logger = lease.scheduler.rm.logger
        
    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "Start          : %s" % self.start, DS)
        self.logger.log(loglevel, "End            : %s" % self.end, DS)
        self.logger.log(loglevel, "Real End       : %s" % self.realend, DS)
        self.logger.log(loglevel, "State          : %s" % rstate_str(self.state), DS)
        self.logger.log(loglevel, "Resources      : \n%s" % "\n".join(["N%i: %s" %(i,prettyRes(x)) for i,x in self.res.items()]), DS)
                
class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, lease, res, start=None, end=None):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.deadline = None
        self.file = None
        # Dictionary of  physnode -> [ (leaseID, vnode)* ]
        self.transfers = {}

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : FILE TRANSFER", DS)
        self.logger.log(loglevel, "Deadline       : %s" % self.deadline, DS)
        self.logger.log(loglevel, "File           : %s" % self.file, DS)
        self.logger.log(loglevel, "Transfers      : %s" % self.transfers, DS)
        
    def piggyback(self, leaseID, vnode, physnode):
        if self.transfers.has_key(physnode):
            self.transfers[physnode].append((leaseID, vnode))
        else:
            self.transfers[physnode] = [(leaseID, vnode)]
            
    def isPreemptible(self):
        return False        
                
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, realend, nodes, res, oncomplete, backfillres):
        ResourceReservationBase.__init__(self, lease, start, end, res, realend=realend)
        self.nodes = nodes
        self.oncomplete = oncomplete
        self.backfillres = backfillres

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : VM", DS)
        self.logger.log(loglevel, "Nodes          : %s" % prettyNodemap(self.nodes), DS)
        self.logger.log(loglevel, "On Complete    : %s" % self.oncomplete, DS)
        
    def isPreemptible(self):
        if isinstance(self.lease,BestEffortLease):
            return True
        elif isinstance(self.lease, ExactLease):
            return False
        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, res, nodes):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : SUSPEND", DS)
        self.logger.log(loglevel, "Nodes          : %s" % prettyNodemap(self.nodes), DS)
        
    def isPreemptible(self):
        return False        
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, res, nodes):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : RESUME", DS)
        self.logger.log(loglevel, "Nodes          : %s" % prettyNodemap(self.nodes), DS)

    def isPreemptible(self):
        return False        
        
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
        self.q.sort(key=attrgetter("leaseID"))
    
    def getNextCancelPoint(self):
        if self.isEmpty():
            return None
        else:
            return min([l.maxqueuetime for l in self.q])
        
    def purgeCancelled(self):
        cancelled = [l.leaseID for l in self.q if l.mustBeCancelled(self.scheduler.rm.clock.getTime())]
        self.q = [l for l in self.q if not l.mustBeCancelled(self.scheduler.rm.clock.getTime())]
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
    
    # TODO: Should be moved to slottable module
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
    
    def getPercentSubmittedTime(self, percent, leasetype=None):
        leases = self.entries.values()
        leases.sort(key=attrgetter("tSubmit"))
        if leasetype != None:
            leases = [l for l in leases if isinstance(l, leasetype)]
        pos = int((len(leases) * (percent / 100.0)) - 1)
        firstsubmission = leases[0].tSubmit
        pctsubmission = leases[pos].tSubmit
        return (pctsubmission - firstsubmission).seconds

    def getLastSubmissionTime(self, leasetype=None):
        return self.getPercentSubmittedTime(100, leasetype)
