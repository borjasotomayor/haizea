import workspace.haizea.resourcemanager.interface as interface
import workspace.haizea.resourcemanager.scheduler as scheduler
import workspace.haizea.resourcemanager.enactment as enactment
import workspace.haizea.resourcemanager.stats as stats
import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
from workspace.haizea.common.log import info, debug, status

class ResourceManager(object):
    def __init__(self, requests, config):
        self.requests = requests
        self.config = config
        
        self.interface = interface.Interface(self)
        self.scheduler = scheduler.Scheduler(self)
        self.enactment = enactment.Enactment(self)
        self.stats = stats.Stats(self)
                
        self.starttime = config.getInitialTime()
        self.time = self.starttime
        

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
        while self.scheduler.existsScheduledLeases() or self.existsPendingReq() or not self.scheduler.isQueueEmpty():
            debug("Starting iteration", constants.RM, self.time)
            nowreq = [r for r in self.requests if r.tSubmit == self.time]
            self.requests = [r for r in self.requests if r.tSubmit > self.time]
            
            exact = [r for r in nowreq if isinstance(r,ExactLease)]
            besteffort = [r for r in nowreq if isinstance(r,BestEffortLease)]
            
            for r in besteffort:
                self.scheduler.enqueue(r)
                self.stats.incrQueueSize()
                self.stats.startQueueWait(r.leaseID)
            
            self.scheduler.schedule(exact)
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
            debug("Next change point (in slot table): %s" % nextchangepoint,constants.RM, self.time)
            nextreqtime = self.getNextReqTime()
            if nextchangepoint != None and nextreqtime == None:
                self.time = nextchangepoint
            if nextchangepoint == None and nextreqtime != None:
                self.time = nextreqtime
            elif nextchangepoint != None and nextreqtime != None:
                self.time = min(nextchangepoint, nextreqtime)
            if nextchangepoint == self.time:
                self.time = self.scheduler.slottable.getNextChangePoint(self.time)

        status("Simulation done, generating stats...", constants.RM, self.time)
        self.stats.addFinalMarker()
        
        # Get utilization stats
        util = self.scheduler.slottable.genUtilizationStats(self.starttime)
        self.stats.utilization[constants.RES_CPU] = util
        status("Stopping resource manager", constants.RM, self.time)
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
            
        
                
            
            
            
        