import workspace.haizea.resourcemanager.interface as interface
import workspace.haizea.resourcemanager.scheduler as scheduler
import workspace.haizea.resourcemanager.enactment as enactment
import workspace.haizea.resourcemanager.stats as stats
import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
from workspace.haizea.common.log import info, debug

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
        info("Starting resource manager", constants.RM, self.time)
        self.stats.addInitialMarker()

        while self.scheduler.existsScheduledLeases() or self.existsPendingReq() or not self.scheduler.isQueueEmpty():
            debug("Starting iteration", constants.RM, self.time)
            nowreq = [r for r in self.requests if r.tSubmit == self.time]
            self.requests = [r for r in self.requests if r.tSubmit > self.time]
            
            exact = [r for r in nowreq if isinstance(r,ExactLease)]
            besteffort = [r for r in nowreq if isinstance(r,BestEffortLease)]
            
            for r in besteffort:
                self.scheduler.enqueue(r)
                self.stats.incrQueueSize()
            
            self.scheduler.schedule(exact)
            debug("Ending iteration", constants.RM, self.time)
        
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

        self.stats.addFinalMarker()
        
        # Get utilization stats
        util = self.scheduler.slottable.genUtilizationStats(self.starttime)
        cpuutilization = [(v[0],v[1]) for v in util]
        cpuutilizationavg = [(v[0],v[2]) for v in util]
        self.stats.utilization[constants.RES_CPU] = cpuutilization
        self.stats.utilizationavg[constants.RES_CPU] = cpuutilizationavg
        print self.stats.queuesize
        info("Stopping resource manager", constants.RM, self.time)
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
            
        
                
            
            
            
        