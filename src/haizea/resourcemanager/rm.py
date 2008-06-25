# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
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

import haizea.resourcemanager.scheduler as scheduler
from haizea.resourcemanager.frontends.tracefile import TracefileFrontend
from haizea.resourcemanager.frontends.opennebula import OpenNebulaFrontend
from haizea.resourcemanager.resourcepool import ResourcePool
from haizea.resourcemanager.log import Logger
import haizea.resourcemanager.stats as stats
import haizea.common.constants as constants
from haizea.common.utils import abstract
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease 
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

        if mode == "simulated":
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
        self.stats = stats.Stats(self, self.config.getStatsDir())

        
    def start(self):
        self.logger.status("Starting resource manager", constants.RM)

        self.stats.createCounter(constants.COUNTER_ARACCEPTED, constants.AVERAGE_NONE)
        self.stats.createCounter(constants.COUNTER_ARREJECTED, constants.AVERAGE_NONE)
        self.stats.createCounter(constants.COUNTER_BESTEFFORTCOMPLETED, constants.AVERAGE_NONE)
        self.stats.createCounter(constants.COUNTER_QUEUESIZE, constants.AVERAGE_TIMEWEIGHTED)
        self.stats.createCounter(constants.COUNTER_DISKUSAGE, constants.AVERAGE_NONE)
        self.stats.createCounter(constants.COUNTER_CPUUTILIZATION, constants.AVERAGE_TIMEWEIGHTED)
        
        self.clock.run()
        
    def stop(self):
        self.logger.status("Stopping resource manager", constants.RM)
        self.stats.stop()
        for l in self.scheduler.completedleases.entries.values():
            l.printContents()
        #self.stats.dumpStatsToDisk(statsdir)
        
    def processRequests(self, nexttime):        
        requests = []
        for f in self.frontends:
            requests += f.getAccumulatedRequests()
        requests.sort(key=operator.attrgetter("tSubmit"))
                
        ar = [r for r in requests if isinstance(r,ARLease)]
        besteffort = [r for r in requests if isinstance(r,BestEffortLease)]
        
        for r in besteffort:
            self.scheduler.enqueue(r)
        
        try:
            self.scheduler.schedule(ar, nexttime)
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
        self.logger.status("STATUS Completed best-effort leases: %i" % self.stats.counters[constants.COUNTER_BESTEFFORTCOMPLETED], constants.RM)
        self.logger.status("STATUS Queue size: %i" % self.stats.counters[constants.COUNTER_QUEUESIZE], constants.RM)
        self.logger.status("STATUS Best-effort reservations: %i" % self.scheduler.numbesteffortres, constants.RM)
        self.logger.status("STATUS Accepted AR leases: %i" % self.stats.counters[constants.COUNTER_ARACCEPTED], constants.RM)
        self.logger.status("STATUS Rejected AR leases: %i" % self.stats.counters[constants.COUNTER_ARREJECTED], constants.RM)
        self.logger.status("STATUS ----End----", constants.RM)

    def getNextChangePoint(self):
       return self.scheduler.slottable.peekNextChangePoint(self.clock.getTime())
   
    def existsLeasesInRM(self):
       return self.scheduler.existsScheduledLeases() or not self.scheduler.isQueueEmpty()
    
    def notifyEndVM(self, l, rr):
        self.scheduler.handlePrematureEndVM(l, rr)
            
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
        self.rm.stats.start(self.getStartTime())
        prevstatustime = self.time
        self.prevtime = None
        done = False
        while not done:
            prematureends = self.rm.scheduler.slottable.getPrematurelyEndingRes(self.time)
            for rr in prematureends:
                self.rm.notifyEndVM(rr.lease, rr)
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
        nextprematureend = self.rm.scheduler.slottable.getNextPrematureEnd(self.time)
        nextreqtime = tracefrontend.getNextReqTime()
        self.rm.logger.debug("Next change point (in slot table): %s" % nextchangepoint,constants.CLOCK)
        self.rm.logger.debug("Next request time: %s" % nextreqtime,constants.CLOCK)
        self.rm.logger.debug("Next premature end: %s" % nextprematureend,constants.CLOCK)
        
        prevtime = self.time
        newtime = self.time
        
        if nextchangepoint != None and nextreqtime == None:
            newtime = nextchangepoint
        elif nextchangepoint == None and nextreqtime != None:
            newtime = nextreqtime
        elif nextchangepoint != None and nextreqtime != None:
            newtime = min(nextchangepoint, nextreqtime)
            
        if nextprematureend != None:
            newtime = min(nextprematureend, newtime)
            
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
    def __init__(self, rm, quantum, fastforward = False):
        Clock.__init__(self,rm)
        self.fastforward = fastforward
        if not self.fastforward:
            self.lastwakeup = None
        else:
            self.lastwakeup = roundDateTime(now())
        self.starttime = self.getTime()
        self.nextperiodicwakeup = None
        self.quantum = TimeDelta(seconds=quantum)
               
    def getTime(self):
        if not self.fastforward:
            return now()
        else:
            return self.lastwakeup
    
    def getStartTime(self):
        return self.starttime

    # Remove this once premature end handling is taken
    # out of handleEndVM 
    def getNextSchedulableTime(self):
        return self.nextperiodicwakeup    
    
    def run(self):
        self.rm.logger.status("Starting simulated clock", constants.CLOCK)
        self.rm.stats.start(self.getStartTime())
        # TODO: Add signal capturing so we can stop gracefully
        done = False
        while not done:
            self.rm.logger.status("Waking up to manage resources", constants.CLOCK)
            if not self.fastforward:
                self.lastwakeup = roundDateTime(self.getTime())
            self.nextperiodicwakeup = roundDateTime(self.lastwakeup + self.quantum)
            self.rm.logger.status("Wake-up time recorded as %s" % self.lastwakeup, constants.CLOCK)
            self.rm.processReservations(self.lastwakeup)
            self.rm.processRequests(self.nextperiodicwakeup)
            
            nextchangepoint = self.rm.getNextChangePoint()
            if nextchangepoint != None and nextchangepoint <= self.nextperiodicwakeup:
                nextwakeup = nextchangepoint
                self.rm.scheduler.slottable.getNextChangePoint(self.lastwakeup)
                self.rm.logger.status("Going back to sleep. Waking up at %s to handle slot table event." % nextwakeup, constants.CLOCK)
            else:
                nextwakeup = self.nextperiodicwakeup
                self.rm.logger.status("Going back to sleep. Waking up at %s to see if something interesting has happened by then." % nextwakeup, constants.CLOCK)
            
            if not self.fastforward:
                time.sleep((nextwakeup - now()).seconds)
            else:
                self.lastwakeup = nextwakeup

                    
        self.rm.printStatus()
        self.rm.logger.status("Stopping clock", constants.CLOCK)
        self.rm.stop()
          

if __name__ == "__main__":
    from haizea.common.config import RMConfig
    configfile="../../../etc/sample.conf"
    config = RMConfig.fromFile(configfile)
    rm = ResourceManager(config)
    rm.start()