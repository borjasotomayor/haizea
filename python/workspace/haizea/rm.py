import workspace.haizea.interface as interface
import workspace.haizea.scheduler as scheduler
import workspace.haizea.enactment as enactment
import workspace.haizea.stats as stats
import workspace.haizea.constants as constants
from workspace.haizea.datastruct import ExactLease, BestEffortLease 
from workspace.haizea.log import info, debug

class ResourceManager(object):
    def __init__(self, requests, config):
        self.requests = requests
        self.config = config
        
        self.interface = interface.Interface(self)
        self.scheduler = scheduler.Scheduler(self)
        self.enactment = enactment.Enactment(self)
        self.stats = stats.Stats(self)
                
        self.time = config.getInitialTime()
        

    def existsPendingReq(self):
        return len(self.requests) != 0

    def getNextReqTime(self):
        if self.existsPendingReq():
            return self.requests[0].tSubmit
        else:
            return None
        
    def run(self):
        info("Starting resource manager", constants.RM, self.time)
        self.stats.addMarker()

        while self.scheduler.existsScheduledLeases() or self.existsPendingReq() or not self.scheduler.isQueueEmpty():
            debug("Starting iteration", constants.RM, self.time)
            nowreq = [r for r in self.requests if r.tSubmit == self.time]
            self.requests = [r for r in self.requests if r.tSubmit > self.time]
            
            exact = [r for r in nowreq if isinstance(r,ExactLease)]
            besteffort = [r for r in nowreq if isinstance(r,BestEffortLease)]
            
            # TODO: Update queue with new best effort requests
            
            self.scheduler.schedule(exact)
            debug("Ending iteration", constants.RM, self.time)
        
            nextchangepoint = self.scheduler.slottable.getNextChangePoint(self.time)
            debug("Next change point: %s" % nextchangepoint,constants.RM, self.time)
            nextreqtime = self.getNextReqTime()
            if nextchangepoint != None and nextreqtime == None:
                self.time = nextchangepoint
            if nextchangepoint == None and nextreqtime != None:
                self.time = nextreqtime
            elif nextchangepoint != None and nextreqtime != None:
                self.time = min(nextchangepoint, nextreqtime)

        self.stats.addMarker()
        info("Stopping resource manager", constants.RM, self.time)
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
            
        print self.stats.exactaccepted
        print self.stats.exactrejected
                
            
            
            
        