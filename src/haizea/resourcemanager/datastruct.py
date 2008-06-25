# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                       #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

from haizea.common.constants import state_str, rstate_str, DS, RES_STATE_SCHEDULED, RES_STATE_ACTIVE, RES_MEM, MIGRATE_NONE, MIGRATE_MEM, MIGRATE_MEMVM, TRANSFER_NONE
from haizea.common.utils import roundDateTimeDelta
from operator import attrgetter
from mx.DateTime import TimeDelta
import haizea.common.constants as constants
from math import floor
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

class ResourceTuple(object):
    def __init__(self, res):
        self.res = res
        
    @classmethod
    def fromList(cls, l):
        return cls(l[:])

    @classmethod
    def copy(cls, rt):
        return cls(rt.res[:])
    
    @classmethod
    def setResourceTypes(cls, resourcetypes):
        cls.type2pos = dict([(x[0],i) for i,x in enumerate(resourcetypes)])
        cls.descriptions = dict([(i,x[2]) for i,x in enumerate(resourcetypes)])
        cls.tuplelength = len(resourcetypes)

    @classmethod
    def createEmpty(cls):
        return cls([0 for x in range(cls.tuplelength)])
        
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
        return int(floor(canfit))
    
    def decr(self, res2):
        for slottype in xrange(len(self.res)):
            self.res[slottype] -= res2.res[slottype]

    def incr(self, res2):
        for slottype in xrange(len(self.res)):
            self.res[slottype] += res2.res[slottype]
        
    def getByType(self, resourcetype):
        return self.res[self.type2pos[resourcetype]]

    def setByType(self, resourcetype, value):
        self.res[self.type2pos[resourcetype]] = value        
        
    def isZeroOrLess(self):
        return sum([v for v in self.res]) <= 0
    
    def __repr__(self):
        r=""
        for i,x in enumerate(self.res):
            r += "%s:%.2f " % (self.descriptions[i],x)
        return r

class Timestamp(object):
    def __init__(self, requested):
        self.requested = requested
        self.scheduled = None
        self.actual = None

    def __repr__(self):
        return "REQ: %s  |  SCH: %s  |  ACT: %s" % (self.requested, self.scheduled, self.actual)
        
class Duration(object):
    def __init__(self, requested, known=None):
        self.original = requested
        self.requested = requested
        self.accumulated = TimeDelta()
        self.actual = None
        # The following is ONLY used in simulation
        self.known = known
        
    def incr(self, t):
        self.requested += t
        if self.known != None:
            self.known += t
        
    def accumulateDuration(self, t):
        self.accumulated += t
            
    def getRemainingDuration(self):
        return self.requested - self.accumulated

    # ONLY in simulation
    def getRemainingKnownDuration(self):
        return self.known - self.accumulated
            
    def __repr__(self):
        return "REQ: %s  |  ACC: %s  |  ACT: %s  |  KNW: %s" % (self.requested, self.accumulated, self.actual,self.known)

class LeaseBase(object):
    def __init__(self, tSubmit, start, duration, diskImageID, diskImageSize, numnodes, resreq):
        # Lease ID (read only)
        self.leaseID = getLeaseID()
        
        # Request attributes (read only)
        self.tSubmit = tSubmit
        self.start = start
        self.duration = duration
        self.end = None
        self.diskImageID = diskImageID
        self.diskImageSize = diskImageSize
        # TODO: The following assumes homogeneous nodes. Should be modified
        # to account for heterogeneous nodes.
        self.numnodes = numnodes
        self.resreq = resreq

        # Bookkeeping attributes
        # (keep track of the lease's state, resource reservations, etc.)
        self.state = None
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
        # Enactment information. Should only be manipulated by enactment module
        self.enactmentInfo = None
        self.vnodeEnactmentInfo = None

                
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
        self.logger.log(loglevel, "Resource req   : %s" % self.resreq, DS)
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
        return [r for r in self.rr if r.end <= time and r.state == RES_STATE_ACTIVE]
    
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
        
    def addBootOverhead(self, t):
        self.duration.incr(t)        
        
    def estimateSuspendResumeTime(self, rate):
        time = float(self.resreq.getByType(RES_MEM)) / rate
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
                mbtotransfer = self.resreq.getByType(RES_MEM)
            elif whattomigrate == MIGRATE_MEMVM:
                mbtotransfer = self.diskImageSize + self.resreq.getByType(RES_MEM)
            return self.estimateTransferTime(mbtotransfer)
        
    def getSuspendThreshold(self, initial, suspendrate, migrating=False):
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
                    threshold = self.estimateMigrationTime() + self.estimateSuspendResumeTime(suspendrate) * 2
                else:
                    threshold = self.estimateSuspendResumeTime(suspendrate) * 2
            else:
                threshold = self.scheduler.rm.config.getBootOverhead() + deploytime + self.estimateSuspendResumeTime(suspendrate)
            factor = self.scheduler.rm.config.getSuspendThresholdFactor() + 1
            return roundDateTimeDelta(threshold * factor)
            
      
        
class ARLease(LeaseBase):
    def __init__(self, tSubmit, tStart, dur, diskImageID, diskImageSize, numnodes, resreq, realdur = None):
        start = Timestamp(tStart)
        duration = Duration(dur)
        duration.known = realdur # ONLY for simulation
        LeaseBase.__init__(self, tSubmit, start, duration, diskImageID, diskImageSize, numnodes, resreq)
        
    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : AR", DS)
        self.logger.log(loglevel, "Start time     : %s" % self.start, DS)
        self.logger.log(loglevel, "Duration       : %s" % self.duration, DS)
        self.printRR(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)
    
    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        duration = self.end - self.start
        realduration = self.prematureend - self.start
        self.end = self.start + roundDateTimeDelta(duration * factor)
        self.prematureend = self.start + roundDateTimeDelta(realduration * factor)

        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, reqdur, diskImageID, diskImageSize, numnodes, resreq, realdur = None):
        start = Timestamp(None) # i.e., start on a best-effort bais
        duration = Duration(reqdur)
        duration.known = realdur # ONLY for simulation
        # When the images will be available
        self.imagesavail = None        
        LeaseBase.__init__(self, tSubmit, start, duration, diskImageID, diskImageSize, numnodes, resreq)

    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : BEST-EFFORT", DS)
        self.logger.log(loglevel, "Duration       : %s" % self.duration, DS)
        self.logger.log(loglevel, "Images Avail @ : %s" % self.imagesavail, DS)
        self.printRR(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)

    def addRuntimeOverhead(self, percent):
        factor = 1 + float(percent)/100
        self.maxdur = roundDateTimeDelta(self.maxdur * factor)
        self.remdur = roundDateTimeDelta(self.remdur * factor)
        self.realremdur = roundDateTimeDelta(self.realremdur * factor)
        self.realdur = roundDateTimeDelta(self.realdur * factor)

        
    def getSlowdown(self, end, bound=10):
#        timeOnDedicated = self.timeOnDedicated
#        timeOnLoaded = end - self.tSubmit
#        bound = TimeDelta(seconds=bound)
#        if timeOnDedicated < bound:
#            timeOnDedicated = bound
#        return timeOnLoaded / timeOnDedicated
        return 42

        
class ResourceReservationBase(object):
    def __init__(self, lease, start, end, res):
        self.start = start
        self.end = end
        self.lease = lease
        self.state = None
        self.res = res
        self.logger = lease.scheduler.rm.logger
                        
    def printContents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "Start          : %s" % self.start, DS)
        self.logger.log(loglevel, "End            : %s" % self.end, DS)
        self.logger.log(loglevel, "State          : %s" % rstate_str(self.state), DS)
        self.logger.log(loglevel, "Resources      : \n%s" % "\n".join(["N%i: %s" %(i,x) for i,x in self.res.items()]), DS)
        
                
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
    def __init__(self, lease, start, end, nodes, res, oncomplete, backfillres):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes
        self.oncomplete = oncomplete
        self.backfillres = backfillres

        # ONLY for simulation
        if lease.duration.known != None:
            remdur = lease.duration.getRemainingKnownDuration()
            rrdur = self.end - self.start
            if remdur < rrdur:
                self.prematureend = self.start + remdur
            else:
                self.prematureend = None
        else:
            self.prematureend = None 

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        if self.prematureend != None:
            self.logger.log(loglevel, "Premature end  : %s" % self.prematureend, DS)
        self.logger.log(loglevel, "Type           : VM", DS)
        self.logger.log(loglevel, "Nodes          : %s" % prettyNodemap(self.nodes), DS)
        self.logger.log(loglevel, "On Complete    : %s" % self.oncomplete, DS)
        
    def isPreemptible(self):
        if isinstance(self.lease,BestEffortLease):
            return True
        elif isinstance(self.lease, ARLease):
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
