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

"""The rm (resource manager) module is the root of Haizea. If you want to
see where the ball starts rolling, look at the following two functions:

* rm.ResourceManager.__init__()
* rm.ResourceManager.start()

This module provides the following classes:

* ResourceManager: The resource manager itself. Pretty much everything else
  is contained in this class.
* Clock: A base class for the resource manager's clock.
* SimulatedClock: A clock for simulations.
* RealClock: A clock that advances in realtime.
"""

import haizea.resourcemanager.stats as stats
import haizea.common.constants as constants
from haizea.resourcemanager.frontends.tracefile import TracefileFrontend
from haizea.resourcemanager.frontends.opennebula import OpenNebulaFrontend
from haizea.resourcemanager.frontends.rpc import RPCFrontend
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease, ImmediateLease 
from haizea.resourcemanager.resourcepool import ResourcePool
from haizea.resourcemanager.scheduler import Scheduler
from haizea.resourcemanager.rpcserver import RPCServer
from haizea.resourcemanager.log import Logger
from haizea.common.utils import abstract, roundDateTime

import operator
import signal
import sys, os
from time import sleep
from math import ceil
from mx.DateTime import now, TimeDelta

DAEMON_STDOUT = DAEMON_STDIN = "/dev/null"
DAEMON_STDERR = "/var/tmp/haizea.err"
DEFAULT_LOGFILE = "/var/tmp/haizea.log"

class ResourceManager(object):
    """The resource manager
    
    This class is the root of Haizea. Pretty much everything else (scheduler,
    enactment modules, etc.) is contained in this class. The ResourceManager
    class is meant to be a singleton.
    
    """
    
    def __init__(self, config, daemon=False, pidfile=None):
        """Initializes the resource manager.
        
        Argument:
        config -- a populated instance of haizea.common.config.RMConfig
        daemon -- True if Haizea must run as a daemon, False if it must
                  run in the foreground
        pidfile -- When running as a daemon, file to save pid to
        """
        self.config = config
        
        # Create the RM components
        
        mode = config.getMode()
        clock = config.getClock()

        if mode == "simulated" and clock == constants.CLOCK_SIMULATED:
            # Simulations always run in the foreground
            self.daemon = False
            # Logger
            self.logger = Logger(self)
            # The clock
            starttime = config.getInitialTime()
            self.clock = SimulatedClock(self, starttime)
            self.rpc_server = None
        elif mode == "opennebula" or (mode == "simulated" and clock == constants.CLOCK_REAL):
            self.daemon = daemon
            self.pidfile = pidfile
            # Logger
            if daemon:
                self.logger = Logger(self, DEFAULT_LOGFILE)
            else:
                self.logger = Logger(self)

            # The clock
            wakeup_interval = config.get_wakeup_interval()
            non_sched = config.get_non_schedulable_interval()
            self.clock = RealClock(self, wakeup_interval, non_sched)
            
            # RPC server
            self.rpc_server = RPCServer(self)

        # Resource pool
        self.resourcepool = ResourcePool(self)
    
        # Scheduler
        self.scheduler = Scheduler(self)
    
        # Lease request frontends
        if mode == "simulated" and clock == constants.CLOCK_SIMULATED:
            # In pure simulation, we can only use the tracefile frontend
            self.frontends = [TracefileFrontend(self, self.clock.get_start_time())]
        elif mode == "simulated" and clock == constants.CLOCK_REAL:
            # In simulation with a real clock, only the RPC frontend can be used
            self.frontends = [RPCFrontend(self)]
        elif mode == "opennebula":
            self.frontends = [OpenNebulaFrontend(self)]
            
        # Statistics collection 
        self.stats = stats.StatsCollection(self, self.config.getDataFile())

    def daemonize(self):
        """Daemonizes the Haizea process.
        
        Based on code in:  http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/66012
        
        """
        # First fork
        try:
            pid = os.fork()
            if pid > 0: 
                # Exit first parent
                sys.exit(0) 
        except OSError, e:
            sys.stderr.write("Failed to daemonize Haizea: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        # Decouple from parent environment.
        os.chdir(".")
        os.umask(0)
        os.setsid()
    
        # Second fork
        try:
            pid = os.fork()
            if pid > 0: 
                # Exit second parent.
                sys.exit(0) 
        except OSError, e:
            sys.stderr.write("Failed to daemonize Haizea: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(2)
            
        # Open file descriptors and print start message
        si = file(DAEMON_STDIN, 'r')
        so = file(DAEMON_STDOUT, 'a+')
        se = file(DAEMON_STDERR, 'a+', 0)
        pid = os.getpid()
        sys.stderr.write("\nStarted Haizea daemon with pid %i\n\n" % pid)
        sys.stderr.flush()
        file(self.pidfile,'w+').write("%i\n" % pid)
        
        # Redirect standard file descriptors.
        os.close(sys.stdin.fileno())
        os.close(sys.stdout.fileno())
        os.close(sys.stderr.fileno())
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    def start(self):
        """Starts the resource manager"""
        self.logger.status("Starting resource manager", constants.RM)

        # Create counters to keep track of interesting data.
        self.stats.create_counter(constants.COUNTER_ARACCEPTED, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_ARREJECTED, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_IMACCEPTED, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_IMREJECTED, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_BESTEFFORTCOMPLETED, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_QUEUESIZE, constants.AVERAGE_TIMEWEIGHTED)
        self.stats.create_counter(constants.COUNTER_DISKUSAGE, constants.AVERAGE_NONE)
        self.stats.create_counter(constants.COUNTER_CPUUTILIZATION, constants.AVERAGE_TIMEWEIGHTED)
        
        if self.daemon:
            self.daemonize()
        if self.rpc_server:
            self.rpc_server.start()
        # Start the clock
        self.clock.run()
        
    def stop(self):
        """Stops the resource manager"""
        
        self.logger.status("Stopping resource manager", constants.RM)
        
        # Stop collecting data (this finalizes counters)
        self.stats.stop()

        # TODO: When gracefully stopping mid-scheduling, we need to figure out what to
        #       do with leases that are still running.

        self.logger.status("  Completed best-effort leases: %i" % self.stats.data.counters[constants.COUNTER_BESTEFFORTCOMPLETED], constants.RM)
        self.logger.status("  Accepted AR leases: %i" % self.stats.data.counters[constants.COUNTER_ARACCEPTED], constants.RM)
        self.logger.status("  Rejected AR leases: %i" % self.stats.data.counters[constants.COUNTER_ARREJECTED], constants.RM)
        
        # In debug mode, dump the lease descriptors.
        for lease in self.scheduler.completedleases.entries.values():
            lease.print_contents()
            
        # Write all collected data to disk
        self.stats.save_to_disk()
        
    def process_requests(self, nexttime):
        """Process any new requests in the request frontend
        
        Checks the request frontend to see if there are any new requests that
        have to be processed. AR leases are sent directly to the schedule.
        Best-effort leases are queued.
        
        Arguments:
        nexttime -- The next time at which the scheduler can allocate resources.
                    This is meant to be provided by the clock simply as a sanity
                    measure when running in real time (to avoid scheduling something
                    "now" to actually have "now" be in the past once the scheduling
                    function returns. i.e., nexttime has nothing to do with whether 
                    there are resources available at that time or not.
        
        """        
        
        # Get requests from frontend
        requests = []
        for frontend in self.frontends:
            requests += frontend.getAccumulatedRequests()
        requests.sort(key=operator.attrgetter("submit_time"))
                
        ar_leases = [req for req in requests if isinstance(req, ARLease)]
        be_leases = [req for req in requests if isinstance(req, BestEffortLease)]
        im_leases = [req for req in requests if isinstance(req, ImmediateLease)]
        
        # Queue best-effort
        for req in be_leases:
            self.scheduler.enqueue(req)
            
        # Add AR leases and immediate leases
        for req in ar_leases + im_leases:
            self.scheduler.add_pending_lease(req)
        
        # Run the scheduling function.
        try:
            self.scheduler.schedule(nexttime)
        except Exception, msg:
            # Exit if something goes horribly wrong
            self.logger.error("Exception in scheduling function. Dumping state..." , constants.RM)
            self.print_stats("ERROR", verbose=True)
            raise      

    def process_reservations(self, time):
        """Process reservations starting/stopping at specified time"""
        
        # The scheduler takes care of this.
        try:
            self.scheduler.process_reservations(time)
        except Exception, msg:
            # Exit if something goes horribly wrong
            self.logger.error("Exception when processing reservations. Dumping state..." , constants.RM)
            self.print_stats("ERROR", verbose=True)
            raise      

        
    def print_stats(self, loglevel, verbose=False):
        """Print some basic statistics in the log
        
        Arguments:
        loglevel -- log level at which to print stats
        verbose -- if True, will print the lease descriptor of all the scheduled
                   and queued leases.
        """
        
        # Print clock stats and the next changepoint in slot table
        self.clock.print_stats(loglevel)
        self.logger.log(loglevel, "Next change point (in slot table): %s" % self.get_next_changepoint(), constants.RM)

        # Print descriptors of scheduled leases
        scheduled = self.scheduler.scheduledleases.entries.keys()
        self.logger.log(loglevel, "Scheduled requests: %i" % len(scheduled), constants.RM)
        if verbose and len(scheduled)>0:
            self.logger.log(loglevel, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv", constants.RM)
            for k in scheduled:
                lease = self.scheduler.scheduledleases.get_lease(k)
                lease.print_contents(loglevel=loglevel)
            self.logger.log(loglevel, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^", constants.RM)

        # Print queue size and descriptors of queued leases
        self.logger.log(loglevel, "Queue size: %i" % self.scheduler.queue.length(), constants.RM)
        if verbose and self.scheduler.queue.length()>0:
            self.logger.log(loglevel, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv", constants.RM)
            for lease in self.scheduler.queue:
                lease.print_contents(loglevel=loglevel)
            self.logger.log(loglevel, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^", constants.RM)

    def get_next_changepoint(self):
        """Return next changepoint in the slot table"""
        return self.scheduler.slottable.peekNextChangePoint(self.clock.get_time())
   
    def exists_leases_in_rm(self):
        """Return True if there are any leases still "in the system" """
        return self.scheduler.exists_scheduled_leases() or not self.scheduler.is_queue_empty()
    
    # TODO: Add more events. This is pending on actually getting interesting
    # events in OpenNebula 1.2. For now, the only event is a prematurely
    # ending VM.
    def notify_event(self, lease_id, event):
        self.scheduler.notify_event(lease_id, event)

    def cancel_lease(self, lease_id):
        """Cancels a lease.
        
        Arguments:
        lease -- Lease to cancel
        """
        self.scheduler.cancel_lease(lease_id)

            
class Clock(object):
    """Base class for the resource manager's clock.
    
    The clock is in charge of periodically waking the resource manager so it
    will process new requests and handle existing reservations. This is a
    base class defining abstract methods.
    
    """
    def __init__(self, rm):
        self.rm = rm
    
    def get_time(self): 
        """Return the current time"""
        return abstract()

    def get_start_time(self): 
        """Return the time at which the clock started ticking"""
        return abstract()
    
    def get_next_schedulable_time(self): 
        """Return the next time at which resources could be scheduled.
        
        The "next schedulable time" server sanity measure when running 
        in real time (to avoid scheduling something "now" to actually 
        have "now" be in the past once the scheduling function returns. 
        i.e., the "next schedulable time" has nothing to do with whether 
        there are resources available at that time or not.
        """
        return abstract()
    
    def run(self):
        """Start and run the clock. This function is, in effect,
        the main loop of the resource manager."""
        return abstract()     
    
    def print_stats(self, loglevel):
        """Print some basic statistics about the clock on the log
        
        Arguments:
        loglevel -- log level at which statistics should be printed.
        """
        return abstract()
    
        
class SimulatedClock(Clock):
    """Simulates the passage of time... really fast.
    
    The simulated clock steps through time to produce an ideal schedule.
    See the run() function for a description of how time is incremented
    exactly in the simulated clock.
    
    """
    
    def __init__(self, rm, starttime):
        """Initialize the simulated clock, starting at the provided starttime"""
        Clock.__init__(self, rm)
        self.starttime = starttime
        self.time = starttime
        self.logger = self.rm.logger
        self.statusinterval = self.rm.config.getStatusMessageInterval()
       
    def get_time(self):
        """See docstring in base Clock class."""
        return self.time
    
    def get_start_time(self):
        """See docstring in base Clock class."""
        return self.starttime

    def get_next_schedulable_time(self):
        """See docstring in base Clock class."""
        return self.time    
    
    def run(self):
        """Runs the simulated clock through time.
        
        The clock starts at the provided start time. At each point in time,
        it wakes up the resource manager and then skips to the next time
        where "something" is happening (see __get_next_time for a more
        rigorous description of this).
        
        The clock stops when there is nothing left to do (no pending or 
        queue requests, and no future reservations)
        
        The simulated clock can only work in conjunction with the
        tracefile request frontend.
        """
        self.rm.logger.status("Starting simulated clock", constants.CLOCK)
        self.rm.stats.start(self.get_start_time())
        prevstatustime = self.time
        done = False
        # Main loop
        while not done:
            # Check to see if there are any leases which are ending prematurely.
            # Note that this is unique to simulation.
            prematureends = self.rm.scheduler.slottable.getPrematurelyEndingRes(self.time)
            
            # Notify the resource manager about the premature ends
            for rr in prematureends:
                self.rm.notify_event(rr.lease.id, constants.EVENT_END_VM)
                
            # Process reservations starting/stopping at the current time and
            # check if there are any new requests.
            self.rm.process_reservations(self.time)
            self.rm.process_requests(self.time)
            
            # Since processing requests may have resulted in new reservations
            # starting now, we process reservations again.
            self.rm.process_reservations(self.time)
            
            # Print a status message
            if self.statusinterval != None and (self.time - prevstatustime).minutes >= self.statusinterval:
                self.__print_status()
                prevstatustime = self.time
                
            # Skip to next point in time.
            self.time, done = self.__get_next_time()
                    
        # Stop the resource manager
        self.rm.logger.status("Stopping simulated clock", constants.CLOCK)
        self.rm.stop()
        
    def print_stats(self, loglevel):
        """See docstring in base Clock class."""
        pass
        
    def __print_status(self):
        """Prints status summary."""
        self.logger.status("STATUS ---Begin---", constants.RM)
        self.logger.status("STATUS Completed best-effort leases: %i" % self.rm.stats.data.counters[constants.COUNTER_BESTEFFORTCOMPLETED], constants.RM)
        self.logger.status("STATUS Queue size: %i" % self.rm.stats.data.counters[constants.COUNTER_QUEUESIZE], constants.RM)
        self.logger.status("STATUS Best-effort reservations: %i" % self.rm.scheduler.numbesteffortres, constants.RM)
        self.logger.status("STATUS Accepted AR leases: %i" % self.rm.stats.data.counters[constants.COUNTER_ARACCEPTED], constants.RM)
        self.logger.status("STATUS Rejected AR leases: %i" % self.rm.stats.data.counters[constants.COUNTER_ARREJECTED], constants.RM)
        self.logger.status("STATUS ----End----", constants.RM)        
    
    def __get_next_time(self):
        """Determines what is the next point in time to skip to.
        
        At a given point in time, the next time is the earliest of the following:
        * The arrival of the next lease request
        * The start or end of a reservation (a "changepoint" in the slot table)
        * A premature end of a lease
        """
        done = False
        
        # Determine candidate next times
        tracefrontend = self.__get_trace_frontend()
        nextchangepoint = self.rm.get_next_changepoint()
        nextprematureend = self.rm.scheduler.slottable.getNextPrematureEnd(self.time)
        nextreqtime = tracefrontend.getNextReqTime()
        self.rm.logger.debug("Next change point (in slot table): %s" % nextchangepoint, constants.CLOCK)
        self.rm.logger.debug("Next request time: %s" % nextreqtime, constants.CLOCK)
        self.rm.logger.debug("Next premature end: %s" % nextprematureend, constants.CLOCK)
        
        # The previous time is now
        prevtime = self.time
        
        # We initialize the next time to now too, to detect if
        # we've been unable to determine what the next time is.
        newtime = self.time
        
        # Find the earliest of the three, accounting for None values
        if nextchangepoint != None and nextreqtime == None:
            newtime = nextchangepoint
        elif nextchangepoint == None and nextreqtime != None:
            newtime = nextreqtime
        elif nextchangepoint != None and nextreqtime != None:
            newtime = min(nextchangepoint, nextreqtime)
            
        if nextprematureend != None:
            newtime = min(nextprematureend, newtime)
            
        if nextchangepoint == newtime:
            # Note that, above, we just "peeked" the next changepoint in the slottable.
            # If it turns out we're skipping to that point in time, then we need to
            # "get" it (this is because changepoints in the slottable are cached to
            # minimize access to the slottable. This optimization turned out to
            # be more trouble than it's worth and will probably be removed sometime
            # soon.
            newtime = self.rm.scheduler.slottable.getNextChangePoint(newtime)
            
        # If there's no more leases in the system, and no more pending requests,
        # then we're done.
        if not self.rm.exists_leases_in_rm() and not tracefrontend.existsPendingReq():
            done = True
        
        # We can also be done if we've specified that we want to stop when
        # the best-effort requests are all done or when they've all been submitted.
        stopwhen = self.rm.config.stopWhen()
        scheduledbesteffort = self.rm.scheduler.scheduledleases.get_leases(type = BestEffortLease)
        pendingbesteffort = [r for r in tracefrontend.requests if isinstance(r, BestEffortLease)]
        if stopwhen == constants.STOPWHEN_BEDONE:
            if self.rm.scheduler.isQueueEmpty() and len(scheduledbesteffort) + len(pendingbesteffort) == 0:
                done = True
        elif stopwhen == constants.STOPWHEN_BESUBMITTED:
            if len(pendingbesteffort) == 0:
                done = True
                
        # If we didn't arrive at a new time, and we're not done, we've fallen into
        # an infinite loop. This is A Bad Thing(tm).
        if newtime == prevtime and done != True:
            self.rm.logger.error("Simulated clock has fallen into an infinite loop. Dumping state..." , constants.CLOCK)
            self.rm.printStats("ERROR", verbose=True)
            raise Exception, "Simulated clock has fallen into an infinite loop."
        
        return newtime, done

    def __get_trace_frontend(self):
        """Gets the tracefile frontend from the resource manager"""
        frontends = self.rm.frontends
        tracef = [f for f in frontends if isinstance(f, TracefileFrontend)]
        if len(tracef) != 1:
            raise Exception, "The simulated clock can only work with a tracefile request frontend."
        else:
            return tracef[0] 
        
        
class RealClock(Clock):
    """A realtime clock.
    
    The real clock wakes up periodically to, in turn, tell the resource manager
    to wake up. The real clock can also be run in a "fastforward" mode for
    debugging purposes (however, unlike the simulated clock, the clock will
    always skip a fixed amount of time into the future).
    """
    def __init__(self, rm, quantum, non_sched, fastforward = False):
        """Initializes the real clock.
        
        Arguments:
        rm -- the resource manager
        quantum -- interval between clock wakeups
        fastforward -- if True, the clock won't actually sleep
                       for the duration of the quantum."""
        Clock.__init__(self, rm)
        self.fastforward = fastforward
        if not self.fastforward:
            self.lastwakeup = None
        else:
            self.lastwakeup = roundDateTime(now())
        self.starttime = self.get_time()
        self.nextschedulable = None
        self.nextperiodicwakeup = None
        self.quantum = TimeDelta(seconds=quantum)
        self.non_sched = TimeDelta(seconds=non_sched)
               
    def get_time(self):
        """See docstring in base Clock class."""
        if not self.fastforward:
            return now()
        else:
            return self.lastwakeup
    
    def get_start_time(self):
        """See docstring in base Clock class."""
        return self.starttime

    def get_next_schedulable_time(self):
        """See docstring in base Clock class."""
        return self.nextschedulable    
    
    def run(self):
        """Runs the real clock through time.
        
        The clock starts when run() is called. In each iteration of the main loop
        it will do the following:
        - Wake up the resource manager
        - Determine if there will be anything to do before the next
          time the clock will wake up (after the quantum has passed). Note
          that this information is readily available on the slot table.
          If so, set next-wakeup-time to (now + time until slot table
          event). Otherwise, set it to (now + quantum)
        - Sleep until next-wake-up-time
        
        The clock keeps on tickin' until a SIGINT signal (Ctrl-C if running in the
        foreground) or a SIGTERM signal is received.
        """
        self.rm.logger.status("Starting clock", constants.CLOCK)
        self.rm.stats.start(self.get_start_time())
        
        signal.signal(signal.SIGINT, self.signalhandler_gracefulstop)
        signal.signal(signal.SIGTERM, self.signalhandler_gracefulstop)
        
        done = False
        # Main loop
        while not done:
            self.rm.logger.status("Waking up to manage resources", constants.CLOCK)
            
            # Save the waking time. We want to use a consistent time in the 
            # resource manager operations (if we use now(), we'll get a different
            # time every time)
            if not self.fastforward:
                self.lastwakeup = roundDateTime(self.get_time())
            self.rm.logger.status("Wake-up time recorded as %s" % self.lastwakeup, constants.CLOCK)
                
            # Next schedulable time
            self.nextschedulable = roundDateTime(self.lastwakeup + self.non_sched)
            
            # Wake up the resource manager
            self.rm.process_reservations(self.lastwakeup)
            # TODO: Compute nextschedulable here, before processing requests
            self.rm.process_requests(self.nextschedulable)
            
            # Next wakeup time
            time_now = now()
            if self.lastwakeup + self.quantum <= time_now:
                quantums = (time_now - self.lastwakeup) / self.quantum
                quantums = int(ceil(quantums)) * self.quantum
                self.nextperiodicwakeup = roundDateTime(self.lastwakeup + quantums)
            else:
                self.nextperiodicwakeup = roundDateTime(self.lastwakeup + self.quantum)
            
            # Determine if there's anything to do before the next wakeup time
            nextchangepoint = self.rm.get_next_changepoint()
            if nextchangepoint != None and nextchangepoint <= self.nextperiodicwakeup:
                # We need to wake up earlier to handle a slot table event
                nextwakeup = nextchangepoint
                self.rm.scheduler.slottable.getNextChangePoint(self.lastwakeup)
                self.rm.logger.status("Going back to sleep. Waking up at %s to handle slot table event." % nextwakeup, constants.CLOCK)
            else:
                # Nothing to do before waking up
                nextwakeup = self.nextperiodicwakeup
                self.rm.logger.status("Going back to sleep. Waking up at %s to see if something interesting has happened by then." % nextwakeup, constants.CLOCK)
            
            # Sleep
            if not self.fastforward:
                sleep((nextwakeup - now()).seconds)
            else:
                self.lastwakeup = nextwakeup
          
    def print_stats(self, loglevel):
        """See docstring in base Clock class."""
        pass
    
    def signalhandler_gracefulstop(self, signum, frame):
        """Handler for SIGTERM and SIGINT. Allows Haizea to stop gracefully."""
        sigstr = ""
        if signum == signal.SIGTERM:
            sigstr = " (SIGTERM)"
        elif signum == signal.SIGINT:
            sigstr = " (SIGINT)"
        self.rm.logger.status("Received signal %i%s" %(signum, sigstr), constants.CLOCK)
        self.rm.logger.status("Stopping gracefully...", constants.CLOCK)
        self.rm.stop()
        sys.exit()

if __name__ == "__main__":
    from haizea.common.config import RMConfig
    CONFIGFILE = "../../../etc/sample.conf"
    CONFIG = RMConfig.fromFile(CONFIGFILE)
    RM = ResourceManager(CONFIG)
    RM.start()