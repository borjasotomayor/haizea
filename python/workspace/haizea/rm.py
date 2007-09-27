import workspace.haizea.interface as interface
import workspace.haizea.scheduler as scheduler
import workspace.haizea.enactment as enactment

class ResourceManager(object):
    def __init__(self, requests, config):
        self.requests = requests
        self.config = config
        
        self.interface = interface.Interface(self)
        self.scheduler = scheduler.Scheduler(self)
        self.enactment = enactment.Enactment(self)
        
        self.time = config.getInitialTime()
        
    def run(self):
        done = False
        while not done: # while we still have pending requests and reservations
            exact = None # Get exact requests at current time
            besteffort = None # Get BE requests at current time
            
            # Update queue with new best effort requests
            
            self.scheduler.schedule(exact)
            
            self.time = self.scheduler.slottable.getNextChangePoint(self.time)
            done = True
            
        