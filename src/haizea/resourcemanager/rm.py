import haizea.resourcemanager.scheduler as scheduler
from haizea.resourcemanager.frontends.tracefile import TracefileFrontend
from haizea.resourcemanager.frontends.opennebula import OpenNebulaFrontend
from haizea.resourcemanager.resourcepool import ResourcePool
from haizea.resourcemanager.log import Logger
import haizea.resourcemanager.stats as stats
import haizea.common.constants as constants
from haizea.common.utils import abstract
from haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
import operator
from mx.DateTime import now, TimeDelta
from haizea.common.utils import roundDateTime
import time

class ResourceManager(object):
    def __init__(self, config):
        self.config = config
        
        # Create the RM components

        # Common components
        self.logger = Logger(self)
        
        # Mode-specific components
        mode = config.getMode()

        if mode == "simulation":
            # The clock
            starttime = config.getInitialTime()
            self.clock = SimulatedClock(self, starttime)

            # Resource pool
            self.resourcepool = ResourcePool(self)
        
            # Scheduler
            self.scheduler = scheduler.Scheduler(self)
    
            # Lease request frontends
            # In simulation, we can only use the tracefile frontend
            self.frontends = [TracefileFrontend(self, self.clock.getStartTime())]
        elif mode == "opennebula":
            # The clock
            self.clock = RealClock(self, 5)
    
            # Resource pool
            self.resourcepool = ResourcePool(self)
    
            # Scheduler
            self.scheduler = scheduler.Scheduler(self)

            # Lease request frontends
            # TODO: Get this from config file
            self.frontends = [OpenNebulaFrontend(self)]


        
        # Statistics collection 
        self.stats = stats.Stats(self)

        
    def start(self):
        self.logger.status("Starting resource manager", constants.RM)
        self.stats.addInitialMarker()
        self.clock.run()
        
    def stop(self):
        self.logger.status("Stopping resource manager", constants.RM)
        self.stats.addFinalMarker()
        # TODO: Get stats dir from config file
        statsdir="/home/borja/docs/uchicago/research/haizea/results"
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
        self.stats.dumpStatsToDisk(statsdir)
        
    def processRequests(self, nexttime):        
        requests = []
        for f in self.frontends:
            requests += f.getAccumulatedRequests()
        requests.sort(key=operator.attrgetter("tSubmit"))
                
        exact = [r for r in requests if isinstance(r,ExactLease)]
        besteffort = [r for r in requests if isinstance(r,BestEffortLease)]
        
        for r in besteffort:
            self.scheduler.enqueue(r)
        
        try:
            self.scheduler.schedule(exact, nexttime)
        except Exception, msg:
            self.logger.error("Exception in scheduling function. Dumping state..." ,constants.RM)
            self.printStats("ERROR", verbose=True)
            raise      

    def processReservations(self, time):                
        try:
            self.scheduler.processReservations(time)
        except Exception, msg:
            self.logger.error("Exception when processing reservations. Dumping state..." ,constants.RM)
            self.printStats("ERROR", verbose=True)
            raise      

        
    def printStats(self, loglevel, verbose=False):
        self.clock.printStats(loglevel)
        self.logger.log(loglevel, "Next change point (in slot table): %s" % self.getNextChangePoint(),constants.RM)
        scheduled = self.scheduler.scheduledleases.entries.keys()
        self.logger.log(loglevel, "Scheduled requests: %i" % len(scheduled),constants.RM)
        if verbose and len(scheduled)>0:
            self.logger.log(loglevel, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",constants.RM)
            for k in scheduled:
                lease = self.scheduler.scheduledleases.getLease(k)
                lease.printContents(loglevel=loglevel)
            self.logger.log(loglevel, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",constants.RM)
        self.logger.log(loglevel, "Queue size: %i" % len(self.scheduler.queue.q),constants.RM)
        if verbose and len(self.scheduler.queue.q)>0:
            self.logger.log(loglevel, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",constants.RM)
            for lease in self.scheduler.queue.q:
                lease.printContents(loglevel=loglevel)
            self.logger.log(loglevel, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",constants.RM)
            
    def printStatus(self):
        self.logger.status("STATUS ---Begin---", constants.RM)
        self.logger.status("STATUS Completed best-effort leases: %i" % self.stats.besteffortcompletedcount, constants.RM)
        self.logger.status("STATUS Queue size: %i" % self.stats.queuesizecount, constants.RM)
        self.logger.status("STATUS Best-effort reservations: %i" % self.scheduler.numbesteffortres, constants.RM)
        self.logger.status("STATUS Accepted exact leases: %i" % self.stats.exactacceptedcount, constants.RM)
        self.logger.status("STATUS Rejected exact leases: %i" % self.stats.exactrejectedcount, constants.RM)
        self.logger.status("STATUS ----End----", constants.RM)

    def getNextChangePoint(self):
       return self.scheduler.slottable.peekNextChangePoint(self.clock.getTime())
   
    def existsLeasesInRM(self):
       return self.scheduler.existsScheduledLeases() or not self.scheduler.isQueueEmpty()
 
            
class Clock(object):
    def __init__(self, rm):
        self.rm = rm
    
    def getTime(self): abstract()

    def getStartTime(self): abstract()
    
    # Remove this once premature end handling is taken
    # out of handleEndVM 
    def getNextSchedulableTime(self): abstract()
    
    def run(self): abstract()     
    
    def printStats(self, loglevel):
        pass       
    
        
class SimulatedClock(Clock):
    def __init__(self, rm, starttime):
        Clock.__init__(self,rm)
        self.starttime = starttime
        self.time = starttime
       
    def getTime(self):
        return self.time

    def getPreciseTime(self):
        return self.time
    
    def getStartTime(self):
        return self.starttime

    # Remove this once premature end handling is taken
    # out of handleEndVM 
    def getNextSchedulableTime(self):
        return self.time    
    
    def run(self):
        self.rm.logger.status("Starting simulated clock", constants.CLOCK)
        prevstatustime = self.time
        self.prevtime = None
        done = False
        while not done:
            self.rm.processReservations(self.time)
            self.rm.processRequests(self.time)
            # Since...
            self.rm.processReservations(self.time)
            if (self.time - prevstatustime).minutes >= 15:
                self.rm.printStatus()
                prevstatustime = self.time
                
            self.time, done = self.getNextTime()
                    
        self.rm.printStatus()
        self.rm.logger.status("Stopping simulated clock", constants.CLOCK)
        self.rm.stop()
    
    def getNextTime(self):
        done = False
        tracefrontend = self.getTraceFrontend()
        nextchangepoint = self.rm.getNextChangePoint()
        nextreqtime = tracefrontend.getNextReqTime()
        self.rm.logger.debug("Next change point (in slot table): %s" % nextchangepoint,constants.CLOCK)
        self.rm.logger.debug("Next request time: %s" % nextreqtime,constants.CLOCK)
        
        prevtime = self.time
        newtime = self.time
        
        if nextchangepoint != None and nextreqtime == None:
            newtime = nextchangepoint
        elif nextchangepoint == None and nextreqtime != None:
            newtime = nextreqtime
        elif nextchangepoint != None and nextreqtime != None:
            newtime = min(nextchangepoint, nextreqtime)
            
        if nextchangepoint == newtime:
            newtime = self.rm.scheduler.slottable.getNextChangePoint(newtime)
            
        if not self.rm.existsLeasesInRM() and not tracefrontend.existsPendingReq():
            done = True
        
        stopwhen = self.rm.config.stopWhen()
        scheduledbesteffort = self.rm.scheduler.scheduledleases.getLeases(type=BestEffortLease)
        pendingbesteffort = [r for r in tracefrontend.requests if isinstance(r,BestEffortLease)]
        if stopwhen == constants.STOPWHEN_BEDONE:
            if self.rm.scheduler.isQueueEmpty() and len(scheduledbesteffort) + len(pendingbesteffort) == 0:
                done = True
        elif stopwhen == constants.STOPWHEN_BESUBMITTED:
            if len(pendingbesteffort) == 0:
                done = True
                
        if newtime == prevtime and done != True:
            self.rm.logger.error("Simulated clock has fallen into an infinite loop. Dumping state..." ,constants.CLOCK)
            self.rm.printStats("ERROR", verbose=True)
            raise Exception, "Simulated clock has fallen into an infinite loop."
        
        return newtime, done

    def getTraceFrontend(self):
        frontends = self.rm.frontends
        tracef = [f for f in frontends if isinstance(f,TracefileFrontend)]
        if len(tracef) != 1:
            raise Exception, "The simulated clock can only work with a tracefile request frontend."
        else:
            return tracef[0] 
        
class RealClock(Clock):
    def __init__(self, rm, quantum):
        Clock.__init__(self,rm)
        self.starttime = self.getTime()
        self.lastwakeup = None
        self.nextwakeup = None
        self.quantum = TimeDelta(seconds=quantum)
               
    def getTime(self):
        return now()
    
    def getStartTime(self):
        return self.starttime

    # Remove this once premature end handling is taken
    # out of handleEndVM 
    def getNextSchedulableTime(self):
        return self.nextwakeup    
    
    def run(self):
        self.rm.logger.status("Starting simulated clock", constants.CLOCK)
        # TODO: Add signal capturing so we can stop gracefully
        done = False
        while not done:
            self.rm.logger.status("Waking up to manage resources", constants.CLOCK)
            self.lastwakeup = self.getTime()
            self.nextwakeup = self.lastwakeup + self.quantum
            self.rm.logger.status("Wake-up time recorded as %s" % self.lastwakeup, constants.CLOCK)
            self.rm.processReservations(self.lastwakeup)
            self.rm.processRequests(self.nextwakeup)
            self.rm.logger.status("Going back to sleep", constants.CLOCK)
            time.sleep((self.nextwakeup - now()).seconds)

                    
        self.rm.printStatus()
        self.rm.logger.status("Stopping simulated clock", constants.CLOCK)
        self.rm.stop()
          

if __name__ == "__main__":
    from haizea.common.config import RMConfig
    configfile="../../../etc/sample_opennebula.conf"
    config = RMConfig.fromFile(configfile)
    rm = ResourceManager(config)
    rm.start()