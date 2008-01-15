import workspace.haizea.resourcemanager.interface as interface
import workspace.haizea.resourcemanager.scheduler as scheduler
import workspace.haizea.resourcemanager.enactment as enactment
import workspace.haizea.resourcemanager.stats as stats
import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
from workspace.haizea.common.log import info, debug, status, error

class ResourceManager(object):
    def __init__(self, requests, config):
        self.requests = requests
        self.config = config
        
        self.interface = interface.Interface(self)
        self.scheduler = scheduler.Scheduler(self)
        self.enactment = enactment.SimulatedEnactment(self)
        self.stats = stats.Stats(self)
                
        for r in self.requests:
            r.scheduler = self.scheduler

        self.starttime = config.getInitialTime()
        self.time = self.starttime
        
        # Add runtime overhead, if necessary
        overhead = self.config.getRuntimeOverhead()
        if overhead != None:
            for r in self.requests:
                if isinstance(r,BestEffortLease):
                    r.addRuntimeOverhead(overhead)
                elif isinstance(r,ExactLease):
                    if not self.config.overheadOnlyBestEffort():
                        r.addRuntimeOverhead(overhead)
                        
        # Add boot + shutdown overhead
        overhead = self.config.getBootOverhead()
        for r in self.requests:
            r.addBootOverhead(overhead)
        
    def existsPendingReq(self):
        return len(self.requests) != 0

    def getNextReqTime(self):
        if self.existsPendingReq():
            return self.requests[0].tSubmit
        else:
            return None
        
    def run(self):
        status("Starting resource manager", constants.RM, self.time)
        self.stats.addInitialMarker()
        prevstatustime = self.time
        done = False
        while not done:
            debug("Starting iteration", constants.RM, self.time)
            nowreq = [r for r in self.requests if r.tSubmit == self.time]
            self.requests = [r for r in self.requests if r.tSubmit > self.time]
            
            exact = [r for r in nowreq if isinstance(r,ExactLease)]
            besteffort = [r for r in nowreq if isinstance(r,BestEffortLease)]
            
            for r in besteffort:
                self.scheduler.enqueue(r)
                self.stats.incrQueueSize(r.leaseID)
                self.stats.startQueueWait(r.leaseID)
            
            try:
                self.scheduler.schedule(exact)
            except Exception, msg:
                error("Exception in scheduling function. Dumping state..." ,constants.RM, self.time)
                self.printStats(error, verbose=True)
                raise
                
            debug("Ending iteration", constants.RM, self.time)
            if (self.time - prevstatustime).minutes >= 15:
                status("STATUS ---Begin---", constants.RM, self.time)
                status("STATUS Completed best-effort leases: %i" % self.stats.besteffortcompletedcount, constants.RM, self.time)
                status("STATUS Queue size: %i" % self.stats.queuesizecount, constants.RM, self.time)
                status("STATUS Best-effort reservations: %i" % self.scheduler.numbesteffortres, constants.RM, self.time)
                status("STATUS Accepted exact leases: %i" % self.stats.exactacceptedcount, constants.RM, self.time)
                status("STATUS Rejected exact leases: %i" % self.stats.exactrejectedcount, constants.RM, self.time)
                status("STATUS ----End----", constants.RM, self.time)
                prevstatustime = self.time
            nextchangepoint = self.scheduler.slottable.peekNextChangePoint(self.time)
            nextreqtime = self.getNextReqTime()
            nextcancelpoint = self.scheduler.queue.getNextCancelPoint()
            self.printStats(debug, nextchangepoint, nextreqtime)
            
            prevtime = self.time
            if nextchangepoint != None and nextreqtime == None:
                self.time = nextchangepoint
            elif nextchangepoint == None and nextreqtime != None:
                self.time = nextreqtime
            elif nextchangepoint != None and nextreqtime != None:
                self.time = min(nextchangepoint, nextreqtime)
            if nextcancelpoint != None:
                self.time = min(nextcancelpoint, self.time)
            if nextchangepoint == self.time:
                self.time = self.scheduler.slottable.getNextChangePoint(self.time)
                
            if not self.scheduler.existsScheduledLeases():
                if not self.existsPendingReq():
                    if self.scheduler.isQueueEmpty():
                        done = True
            stopwhen = self.config.stopWhen()
            scheduledbesteffort = self.scheduler.scheduledleases.getLeases(type=BestEffortLease)
            pendingbesteffort = [r for r in self.requests if isinstance(r,BestEffortLease)]
            if stopwhen == constants.STOPWHEN_BEDONE:
                if self.scheduler.isQueueEmpty() and len(scheduledbesteffort) + len(pendingbesteffort) == 0:
                    done = True
            elif stopwhen == constants.STOPWHEN_BESUBMITTED:
                if len(pendingbesteffort) == 0:
                    done = True
                    
            if self.time == prevtime and done != True:
                error("RM has fallen into an infinite loop. Dumping state..." ,constants.RM, self.time)
                self.printStats(error, nextchangepoint, nextreqtime, verbose=True)
                raise Exception, "Resource manager has fallen into an infinite loop."

        status("STATUS ---Begin---", constants.RM, self.time)
        status("STATUS Completed best-effort leases: %i" % self.stats.besteffortcompletedcount, constants.RM, self.time)
        status("STATUS Queue size: %i" % self.stats.queuesizecount, constants.RM, self.time)
        status("STATUS Best-effort reservations: %i" % self.scheduler.numbesteffortres, constants.RM, self.time)
        status("STATUS Accepted exact leases: %i" % self.stats.exactacceptedcount, constants.RM, self.time)
        status("STATUS Rejected exact leases: %i" % self.stats.exactrejectedcount, constants.RM, self.time)
        status("STATUS ----End----", constants.RM, self.time)
        status("Simulation done, generating stats...", constants.RM, self.time)
        self.stats.addFinalMarker()
        
        # Get utilization stats
        util = self.scheduler.slottable.genUtilizationStats(self.starttime)
        self.stats.utilization[constants.RES_CPU] = util
        status("Stopping resource manager", constants.RM, self.time)
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
            
        
    def printStats(self, logfun, nextcp="NONE", nextreqtime="NONE", verbose=False):
        logfun("Next change point (in slot table): %s" % nextcp,constants.RM, nextcp)
        logfun("Next request time: %s" % nextreqtime,constants.RM, nextreqtime)
        scheduled = self.scheduler.scheduledleases.entries.keys()
        logfun("Scheduled requests: %i" % len(scheduled),constants.RM, self.time)
        if verbose and len(scheduled)>0:
            logfun("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",constants.RM, None)
            for k in scheduled:
                lease = self.scheduler.scheduledleases.getLease(k)
                lease.printContents(logfun=error)
            logfun("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",constants.RM, None)
        logfun("Pending requests: %i" % len(self.requests),constants.RM, self.time)
        if verbose and len(self.requests)>0:
            logfun("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",constants.RM, None)
            for lease in self.requests:
                lease.printContents(logfun=error)
            logfun("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",constants.RM, None)
        logfun("Queue size: %i" % len(self.scheduler.queue.q),constants.RM, self.time)
        if verbose and len(self.scheduler.queue.q)>0:
            logfun("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",constants.RM, None)
            for lease in self.scheduler.queue.q:
                lease.printContents(logfun=logfun)
            logfun("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",constants.RM, None)
           
            
            
            
        