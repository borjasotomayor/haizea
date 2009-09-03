# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
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

"""The manager (resource manager) module is the root of Haizea. If you want to
see where the ball starts rolling, look at the following two functions:

* manager.Manager.__init__()
* manager.Manager.start()

This module provides the following classes:

* Manager: Haizea itself. Pretty much everything else
  is contained in this class.
* Clock: A base class for Haizea's clock.
* SimulatedClock: A clock for simulations.
* RealClock: A clock that advances in realtime.
"""
 
import haizea.common.constants as constants
from haizea.core.scheduler.preparation_schedulers.unmanaged import UnmanagedPreparationScheduler
from haizea.core.scheduler.preparation_schedulers.imagetransfer import ImageTransferPreparationScheduler
from haizea.core.enact.opennebula import OpenNebulaResourcePoolInfo, OpenNebulaVMEnactment, OpenNebulaDummyDeploymentEnactment
from haizea.core.enact.simulated import SimulatedResourcePoolInfo, SimulatedVMEnactment, SimulatedDeploymentEnactment
from haizea.core.frontends.tracefile import TracefileFrontend
from haizea.core.frontends.opennebula import OpenNebulaFrontend
from haizea.core.frontends.rpc import RPCFrontend
from haizea.core.accounting import AccountingDataCollection
from haizea.core.scheduler import UnrecoverableError
from haizea.core.scheduler.lease_scheduler import LeaseScheduler
from haizea.core.scheduler.vm_scheduler import VMScheduler
from haizea.core.scheduler.mapper import class_mappings as mapper_mappings
from haizea.core.scheduler.slottable import SlotTable, ResourceReservation
from haizea.core.scheduler.policy import PolicyManager
from haizea.core.scheduler.resourcepool import ResourcePool, ResourcePoolWithReusableImages
from haizea.core.leases import Lease, Site
from haizea.core.log import HaizeaLogger
from haizea.core.rpcserver import RPCServer
from haizea.common.utils import abstract, round_datetime, Singleton, import_class
from haizea.pluggable.policies import admission_class_mappings, preemption_class_mappings, host_class_mappings 
from haizea.pluggable.accounting import probe_class_mappings

import operator
import logging
import signal
import sys, os
import traceback
import shelve
from time import sleep
from math import ceil
from mx.DateTime import now, TimeDelta

DAEMON_STDOUT = DAEMON_STDIN = "/dev/null"
DAEMON_STDERR = "/var/tmp/haizea.err"
DEFAULT_LOGFILE = "/var/tmp/haizea.log"

class Manager(Singleton):
    """The root of Haizea
    
    This class is the root of Haizea. Pretty much everything else (scheduler,
    enactment modules, etc.) is contained in this class. The Manager
    class is meant to be a singleton.
    
    """
    
    def __init__(self, config, daemon=False, pidfile=None):
        """Initializes the manager.
        
        Argument:
        config -- a populated instance of haizea.common.config.RMConfig
        daemon -- True if Haizea must run as a daemon, False if it must
                  run in the foreground
        pidfile -- When running as a daemon, file to save pid to
        """
        self.config = config
        
        # Create the RM components
        
        mode = config.get("mode")
        
        self.daemon = daemon
        self.pidfile = pidfile

        if mode == "simulated":
            # Simulated-time simulations always run in the foreground
            clock = self.config.get("clock")
            if clock == constants.CLOCK_SIMULATED:
                self.daemon = False
        elif mode == "opennebula":
            clock = constants.CLOCK_REAL        
        
        self.init_logging()
                
        if clock == constants.CLOCK_SIMULATED:
            starttime = self.config.get("starttime")
            self.clock = SimulatedClock(self, starttime)
            self.rpc_server = None
        elif clock == constants.CLOCK_REAL:
            wakeup_interval = self.config.get("wakeup-interval")
            non_sched = self.config.get("non-schedulable-interval")
            if mode == "opennebula":
                fastforward = self.config.get("dry-run")
            else:
                fastforward = False
            self.clock = RealClock(self, wakeup_interval, non_sched, fastforward)
            if fastforward:
                # No need for an RPC server when doing a dry run
                self.rpc_server = None
            else:
                self.rpc_server = RPCServer(self)
                    
        # Enactment modules
        if mode == "simulated":
            resources = self.config.get("simul.resources")
            if resources == "in-tracefile":
                tracefile = self.config.get("tracefile")
                site = Site.from_lwf_file(tracefile)
            elif resources.startswith("file:"):
                sitefile = resources.split(":")
                site = Site.from_xml_file(sitefile)
            else:
                site = Site.from_resources_string(resources)
    
            info_enact = SimulatedResourcePoolInfo(site)
            vm_enact = SimulatedVMEnactment()
            deploy_enact = SimulatedDeploymentEnactment()
        elif mode == "opennebula":
            # Enactment modules
            info_enact = OpenNebulaResourcePoolInfo()
            vm_enact = OpenNebulaVMEnactment()
            # No deployment in OpenNebula. Using dummy one for now.
            deploy_enact = OpenNebulaDummyDeploymentEnactment()            

        if mode == "simulated":
            preparation_type = self.config.get("lease-preparation")
        elif mode == "opennebula":
            # No deployment in OpenNebula.
            preparation_type = constants.PREPARATION_UNMANAGED

        # Resource pool
        if preparation_type == constants.PREPARATION_TRANSFER:
            if self.config.get("diskimage-reuse") == constants.REUSE_IMAGECACHES:
                resourcepool = ResourcePoolWithReusableImages(info_enact, vm_enact, deploy_enact)
            else:
                resourcepool = ResourcePool(info_enact, vm_enact, deploy_enact)
        else:
            resourcepool = ResourcePool(info_enact, vm_enact, deploy_enact)
    
        # Slot table
        slottable = SlotTable(info_enact.get_resource_types())
        for n in resourcepool.get_nodes() + resourcepool.get_aux_nodes():
            rt = slottable.create_resource_tuple_from_capacity(n.capacity)
            slottable.add_node(n.id, rt)

        # Policy manager
        admission = self.config.get("policy.admission")
        admission = admission_class_mappings.get(admission, admission)
        admission = import_class(admission)
        admission = admission(slottable)
        
        preemption = self.config.get("policy.preemption")
        preemption = preemption_class_mappings.get(preemption, preemption)
        preemption = import_class(preemption)
        preemption = preemption(slottable)

        host_selection = self.config.get("policy.host-selection")
        host_selection = host_class_mappings.get(host_selection, host_selection)
        host_selection = import_class(host_selection)
        host_selection = host_selection(slottable)

        self.policy = PolicyManager(admission, preemption, host_selection)

        # Preparation scheduler
        if preparation_type == constants.PREPARATION_UNMANAGED:
            preparation_scheduler = UnmanagedPreparationScheduler(slottable, resourcepool, deploy_enact)
        elif preparation_type == constants.PREPARATION_TRANSFER:
            preparation_scheduler = ImageTransferPreparationScheduler(slottable, resourcepool, deploy_enact)    
    
        # VM mapper and scheduler
        mapper = self.config.get("mapper")
        mapper = mapper_mappings.get(mapper, mapper)
        mapper = import_class(mapper)
        mapper = mapper(slottable, self.policy)
        vm_scheduler = VMScheduler(slottable, resourcepool, mapper)
    
        # Statistics collection 
        self.accounting = AccountingDataCollection(self.config.get("datafile"))
        # Load probes
        probes = self.config.get("accounting-probes")
        probes = probes.split()
        for probe in probes:
            probe_class = probe_class_mappings.get(probe, probe)
            probe_class = import_class(probe_class)
            probe_obj = probe_class(self.accounting)
            self.accounting.add_probe(probe_obj)    
    
        # Lease Scheduler
        self.scheduler = LeaseScheduler(vm_scheduler, preparation_scheduler, slottable, self.accounting)
        
        # Lease request frontends
        if mode == "simulated":
            if clock == constants.CLOCK_SIMULATED:
                # In pure simulation, we can only use the tracefile frontend
                self.frontends = [TracefileFrontend(self, self.clock.get_start_time())]
            elif clock == constants.CLOCK_REAL:
                # In simulation with a real clock, only the RPC frontend can be used
                self.frontends = [RPCFrontend(self)]             
        elif mode == "opennebula":
            self.frontends = [OpenNebulaFrontend(self)]               

        persistence_file = self.config.get("persistence-file")
        if persistence_file == "none":
            persistence_file = None
        self.persistence = PersistenceManager(persistence_file)
        
        self.logger = logging.getLogger("RM")


    def init_logging(self):
        """Initializes logging
        
        """

        logger = logging.getLogger("")
        if self.daemon:
            handler = logging.FileHandler(self.config.get("logfile"))
        else:
            handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(haizeatime)s] %(name)-7s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        level = logging.getLevelName(self.config.get("loglevel"))
        logger.setLevel(level)
        logging.setLoggerClass(HaizeaLogger)

        
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
        self.logger.info("Starting resource manager")
        
        if self.daemon:
            self.daemonize()
        if self.rpc_server:
            self.rpc_server.start()
            
        self.__recover()            
            
        # Start the clock
        try:
            self.clock.run()
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)

    def stop(self):
        """Stops the resource manager by stopping the clock"""
        self.clock.stop()
        
    def graceful_stop(self):
        """Stops the resource manager gracefully and exits"""
        
        self.logger.status("Stopping resource manager gracefully...")
        
        # Stop collecting data (this finalizes counters)
        self.accounting.stop()
        
        self.persistence.close()
        
        # TODO: When gracefully stopping mid-scheduling, we need to figure out what to
        #       do with leases that are still running.

        self.print_status()
        
        # In debug mode, dump the lease descriptors.
        for lease in self.scheduler.completed_leases.entries.values():
            lease.print_contents()
            
        # Write all collected data to disk
        leases = self.scheduler.completed_leases.entries
        self.accounting.save_to_disk(leases)
        
        # Stop RPC server
        if self.rpc_server != None:
            self.rpc_server.stop()
                    
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
            requests += frontend.get_accumulated_requests()
        requests.sort(key=operator.attrgetter("submit_time"))
        
        # Request leases and run the scheduling function.
        try:
            self.logger.vdebug("Requesting leases")
            for req in requests:
                self.scheduler.request_lease(req)

            self.logger.vdebug("Running scheduling function")
            self.scheduler.schedule(nexttime)
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)

    def process_starting_reservations(self, time):
        """Process reservations starting/stopping at specified time"""
        
        # The lease scheduler takes care of this.
        try:
            self.scheduler.process_starting_reservations(time)
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)

    def process_ending_reservations(self, time):
        """Process reservations starting/stopping at specified time"""
        
        # The lease scheduler takes care of this.
        try:
            self.scheduler.process_ending_reservations(time)
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)             
             
    def notify_event(self, lease_id, event):
        """Notifies an asynchronous event to Haizea.
        
        Arguments:
        lease_id -- ID of lease that is affected by event
        event -- Event (currently, only the constants.EVENT_END_VM event is supported)
        """
        try:
            lease = self.scheduler.get_lease_by_id(lease_id)
            self.scheduler.notify_event(lease, event)
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)
        
    def cancel_lease(self, lease_id):
        """Cancels a lease.
        
        Arguments:
        lease_id -- ID of lease to cancel
        """    
        try:
            lease = self.scheduler.get_lease_by_id(lease_id)
            self.scheduler.cancel_lease(lease)
        except UnrecoverableError, exc:
            self.__unrecoverable_error(exc)
        except Exception, exc:
            self.__unexpected_exception(exc)
            
    def get_next_changepoint(self):
        """Return next changepoint in the slot table"""
        return self.scheduler.slottable.get_next_changepoint(self.clock.get_time())
   
    def exists_more_leases(self):
        """Return True if there are any leases still "in the system" """
        return self.scheduler.exists_scheduled_leases() or not self.scheduler.is_queue_empty()

    def print_status(self):
        """Prints status summary."""
        
        leases = self.scheduler.leases.get_leases()
        completed_leases = self.scheduler.completed_leases.get_leases()
        self.logger.status("--- Haizea status summary ---")
        self.logger.status("Number of leases (not including completed): %i" % len(leases))
        self.logger.status("Completed leases: %i" % len(completed_leases))
        self.logger.status("---- End summary ----")        

    def __recover(self):
        """Loads persisted leases and scheduling information
        
        This method does three things:
        1. Recover persisted leases. Note that not all persisted leases
           may be recoverable. For example, if a lease was scheduled
           to start at a certain time, but that time passed while
           Haizea was not running, the lease will simply be transitioned
           to a failed state.
        2. Recover the queue.
        3. Recover the list of "future leases" as determined by
           the backfilling algorithm.
        """
        
        # Load leases
        leases = self.persistence.get_leases()
        for lease in leases:
            # Create a list of RRs
            rrs = lease.preparation_rrs + lease.vm_rrs
            for vmrr in lease.vm_rrs:
                rrs += vmrr.pre_rrs + vmrr.post_rrs

            # Bind resource tuples in RRs to slot table
            for rr in rrs:
                for restuple in rr.resources_in_pnode.values():
                    restuple.slottable = self.scheduler.slottable

            self.logger.debug("Attempting to recover lease %i" % lease.id)
            lease.print_contents()
            
            # Check the lease's state and determine how to proceed.
            load_rrs = False
            lease_state = lease.get_state()
            if lease_state in (Lease.STATE_DONE, Lease.STATE_CANCELLED, Lease.STATE_REJECTED, Lease.STATE_FAIL):
                self.logger.info("Recovered lease %i (already done)" % lease.id)
                self.scheduler.completed_leases.add(lease)
            elif lease_state in (Lease.STATE_NEW, Lease.STATE_PENDING):
                self.scheduler.leases.add(lease)
            elif lease_state == Lease.STATE_QUEUED:
                load_rrs = True
                self.scheduler.leases.add(lease)
                self.logger.info("Recovered lease %i (queued)" % lease.id)
            elif lease_state in (Lease.STATE_SCHEDULED, Lease.STATE_READY):
                # Check if schedule is still valid.
                vmrr = lease.get_last_vmrr()
                if len(vmrr.pre_rrs) > 0:
                    start = vmrr.pre_rrs[0].start
                else:
                    start = vmrr.start
                if self.clock.get_time() < start:
                    load_rrs = True
                    self.scheduler.leases.add(lease)
                    self.logger.info("Recovered lease %i" % lease.id)
                else:
                    lease.set_state(Lease.STATE_FAIL)
                    self.scheduler.completed_leases.add(lease)
                    self.logger.info("Could not recover lease %i (scheduled starting time has passed)" % lease.id)
            elif lease_state == Lease.STATE_ACTIVE:
                vmrr = lease.get_last_vmrr()
                if self.clock.get_time() < self.clock.get_time():
                    # TODO: Check if VMs are actually running
                    load_rrs = True
                    self.scheduler.leases.add(lease)
                    self.logger.info("Recovered lease %i" % lease.id)
                else:
                    # TODO: May have to stop extant virtual machines
                    lease.set_state(Lease.STATE_FAIL)
                    self.scheduler.completed_leases.add(lease)                    
                    self.logger.info("Could not recover lease %i (scheduled ending time has passed)" % lease.id)
            else:
                # No support for recovering lease in the
                # remaining states
                lease.set_state(Lease.STATE_FAIL)
                self.scheduler.completed_leases.add(lease)                    
                self.logger.info("Could not recover lease %i (unsupported state %i for recovery)" % (lease.id, lease_state))
                
            # Load the lease's RRs into the slot table
            if load_rrs:
                for rr in rrs:
                    if rr.state in (ResourceReservation.STATE_ACTIVE, ResourceReservation.STATE_SCHEDULED):
                        self.scheduler.slottable.add_reservation(rr)
                
        # Rebuild the queue        
        queue = self.persistence.get_queue()
        for lease_id in queue:
            if self.scheduler.leases.has_lease(lease_id):
                lease = self.scheduler.leases.get_lease(lease_id)
                self.scheduler.queue.enqueue(lease)

        # Rebuild the "future leases"
        future = self.persistence.get_future_leases()
        for lease_id in future:
            if self.scheduler.leases.has_lease(lease_id):
                lease = self.scheduler.leases.get_lease(lease_id)
                self.scheduler.vm_scheduler.future_leases.add(lease)


    def __unrecoverable_error(self, exc):
        """Handles an unrecoverable error.
        
        This method prints information on the unrecoverable error and makes Haizea panic.
        """
        self.logger.error("An unrecoverable error has happened.")
        self.logger.error("Original exception:")
        self.__print_exception(exc.exc, exc.get_traceback())
        self.logger.error("Unrecoverable error traceback:")
        self.__print_exception(exc, sys.exc_info()[2])
        self.__panic()

    def __unexpected_exception(self, exc):
        """Handles an unrecoverable error.
        
        This method prints information on the unrecoverable error and makes Haizea panic.
        """
        self.logger.error("An unexpected exception has happened.")
        self.__print_exception(exc, sys.exc_info()[2])
        self.__panic()
            
    def __print_exception(self, exc, exc_traceback):
        """Prints an exception's traceback to the log."""
        tb = traceback.format_tb(exc_traceback)
        for line in tb:
            self.logger.error(line)
        self.logger.error("Message: %s" % exc)

    
    def __panic(self):
        """Makes Haizea crash and burn in a panicked frenzy"""
        
        self.logger.status("Panicking...")

        # Stop RPC server
        if self.rpc_server != None:
            self.rpc_server.stop()

        # Dump state
        self.print_status()
        self.logger.error("Next change point (in slot table): %s" % self.get_next_changepoint())

        # Print lease descriptors
        leases = self.scheduler.leases.get_leases()
        if len(leases)>0:
            self.logger.vdebug("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
            for lease in leases:
                lease.print_contents()
            self.logger.vdebug("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")        

        # Exit
        treatment = self.config.get("lease-failure-handling")
        if treatment == constants.ONFAILURE_EXIT_RAISE:
            raise
        else:
            exit(1)

            
class Clock(object):
    """Base class for the resource manager's clock.
    
    The clock is in charge of periodically waking the resource manager so it
    will process new requests and handle existing reservations. This is a
    base class defining abstract methods.
    
    """
    def __init__(self, manager):
        self.manager = manager
        self.done = False
    
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

    def stop(self):
        """Stop the clock.
        
        Stopping the clock makes Haizea exit.
        """
        self.done = True    
    
        
class SimulatedClock(Clock):
    """Simulates the passage of time... really fast.
    
    The simulated clock steps through time to produce an ideal schedule.
    See the run() function for a description of how time is incremented
    exactly in the simulated clock.
    
    """
    
    def __init__(self, manager, starttime):
        """Initialize the simulated clock, starting at the provided starttime"""
        Clock.__init__(self, manager)
        self.starttime = starttime
        self.time = starttime
        self.logger = logging.getLogger("CLOCK")
        self.statusinterval = self.manager.config.get("status-message-interval")
       
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
        self.logger.status("Starting simulated clock")
        self.manager.accounting.start(self.get_start_time())
        prevstatustime = self.time
        
        # Main loop
        while not self.done:
            # Check to see if there are any leases which are ending prematurely.
            # Note that this is unique to simulation.
            prematureends = self.manager.scheduler.slottable.get_prematurely_ending_res(self.time)
            
            # Notify the resource manager about the premature ends
            for rr in prematureends:
                self.manager.notify_event(rr.lease.id, constants.EVENT_END_VM)
                
            # Process reservations starting/stopping at the current time and
            # check if there are any new requests.
            self.manager.process_ending_reservations(self.time)
            self.manager.process_starting_reservations(self.time)
            self.manager.process_requests(self.time)
            
            # Since processing requests may have resulted in new reservations
            # starting now, we process reservations again.
            self.manager.process_starting_reservations(self.time)
            # And one final call to deal with nil-duration reservations
            self.manager.process_ending_reservations(self.time)
            
            self.manager.accounting.at_timestep(self.manager.scheduler)
            
            # Print a status message
            if self.statusinterval != None and (self.time - prevstatustime).minutes >= self.statusinterval:
                self.manager.print_status()
                prevstatustime = self.time
                
            # Skip to next point in time.
            self.time, self.done = self.__get_next_time()
                    
        self.logger.status("Simulated clock has stopped")

        # Stop the resource manager
        self.manager.graceful_stop()
        
    
    def __get_next_time(self):
        """Determines what is the next point in time to skip to.
        
        At a given point in time, the next time is the earliest of the following:
        * The arrival of the next lease request
        * The start or end of a reservation (a "changepoint" in the slot table)
        * A premature end of a lease
        """
        
        # Determine candidate next times
        tracefrontend = self.__get_trace_frontend()
        nextchangepoint = self.manager.get_next_changepoint()
        nextprematureend = self.manager.scheduler.slottable.get_next_premature_end(self.time)
        nextreqtime = tracefrontend.get_next_request_time()
        self.logger.debug("Next change point (in slot table): %s" % nextchangepoint)
        self.logger.debug("Next request time: %s" % nextreqtime)
        self.logger.debug("Next premature end: %s" % nextprematureend)
        
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
                        
        # If there's no more leases in the system, and no more pending requests,
        # then we're done.
        if not self.manager.exists_more_leases() and not tracefrontend.exists_more_requests():
            self.done = True
        
        # We can also be done if we've specified that we want to stop when
        # the best-effort requests are all done or when they've all been submitted.
        stopwhen = self.manager.config.get("stop-when")
        besteffort = self.manager.scheduler.leases.get_leases(type = Lease.BEST_EFFORT)
        pendingbesteffort = [r for r in tracefrontend.requests if r.get_type() == Lease.BEST_EFFORT]
        if stopwhen == constants.STOPWHEN_BEDONE:
            if self.manager.scheduler.is_queue_empty() and len(besteffort) + len(pendingbesteffort) == 0:
                self.done = True
        elif stopwhen == constants.STOPWHEN_BESUBMITTED:
            if len(pendingbesteffort) == 0:
                self.done = True
                
        # If we didn't arrive at a new time, and we're not done, we've fallen into
        # an infinite loop. This is A Bad Thing(tm).
        if newtime == prevtime and self.done != True:
            raise Exception, "Simulated clock has fallen into an infinite loop."
        
        return newtime, self.done

    def __get_trace_frontend(self):
        """Gets the tracefile frontend from the resource manager"""
        frontends = self.manager.frontends
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
    def __init__(self, manager, quantum, non_sched, fastforward = False):
        """Initializes the real clock.
        
        Arguments:
        manager -- the resource manager
        quantum -- interval between clock wakeups
        fastforward -- if True, the clock won't actually sleep
                       for the duration of the quantum."""
        Clock.__init__(self, manager)
        self.fastforward = fastforward
        if not self.fastforward:
            self.lastwakeup = None
        else:
            self.lastwakeup = round_datetime(now())
        self.logger = logging.getLogger("CLOCK")
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
        self.logger.status("Starting clock")
        self.manager.accounting.start(self.get_start_time())
        
        try:
            signal.signal(signal.SIGINT, self.signalhandler_gracefulstop)
            signal.signal(signal.SIGTERM, self.signalhandler_gracefulstop)
        except ValueError, exc:
            # This means Haizea is not the main thread, which will happen
            # when running it as part of a py.test. We simply ignore this
            # to allow the test to continue.
            pass
        
        # Main loop
        while not self.done:
            self.logger.status("Waking up to manage resources")
            
            # Save the waking time. We want to use a consistent time in the 
            # resource manager operations (if we use now(), we'll get a different
            # time every time)
            if not self.fastforward:
                self.lastwakeup = round_datetime(self.get_time())
            self.logger.status("Wake-up time recorded as %s" % self.lastwakeup)
                
            # Next schedulable time
            self.nextschedulable = round_datetime(self.lastwakeup + self.non_sched)
            
            # Wake up the resource manager
            self.manager.process_ending_reservations(self.lastwakeup)
            self.manager.process_starting_reservations(self.lastwakeup)
            # TODO: Compute nextschedulable here, before processing requests
            self.manager.process_requests(self.nextschedulable)

            self.manager.accounting.at_timestep(self.manager.scheduler)
            
            # Next wakeup time
            time_now = now()
            if self.lastwakeup + self.quantum <= time_now:
                quantums = (time_now - self.lastwakeup) / self.quantum
                quantums = int(ceil(quantums)) * self.quantum
                self.nextperiodicwakeup = round_datetime(self.lastwakeup + quantums)
            else:
                self.nextperiodicwakeup = round_datetime(self.lastwakeup + self.quantum)
            
            # Determine if there's anything to do before the next wakeup time
            nextchangepoint = self.manager.get_next_changepoint()
            if nextchangepoint != None and nextchangepoint <= self.nextperiodicwakeup:
                # We need to wake up earlier to handle a slot table event
                nextwakeup = nextchangepoint
                self.logger.status("Going back to sleep. Waking up at %s to handle slot table event." % nextwakeup)
            else:
                # Nothing to do before waking up
                nextwakeup = self.nextperiodicwakeup
                self.logger.status("Going back to sleep. Waking up at %s to see if something interesting has happened by then." % nextwakeup)
            
            # The only exit condition from the real clock is if the stop_when_no_more_leases
            # is set to True, and there's no more work left to do.
            # TODO: This first if is a kludge. Other options should only interact with
            # options through the configfile's get method. The "stop-when-no-more-leases"
            # option is currently OpenNebula-specific (while the real clock isn't; it can
            # be used by both the simulator and the OpenNebula mode). This has to be
            # fixed.            
            if self.manager.config._options.has_key("stop-when-no-more-leases"):
                stop_when_no_more_leases = self.manager.config.get("stop-when-no-more-leases")
                if stop_when_no_more_leases and not self.manager.exists_more_leases():
                    self.done = True
            
            # Sleep
            if not self.done:
                if not self.fastforward:
                    sleep((nextwakeup - now()).seconds)
                else:
                    self.lastwakeup = nextwakeup

        self.logger.status("Real clock has stopped")

        # Stop the resource manager
        self.manager.graceful_stop()
    
    def signalhandler_gracefulstop(self, signum, frame):
        """Handler for SIGTERM and SIGINT. Allows Haizea to stop gracefully."""
        
        sigstr = ""
        if signum == signal.SIGTERM:
            sigstr = " (SIGTERM)"
        elif signum == signal.SIGINT:
            sigstr = " (SIGINT)"
        self.logger.status("Received signal %i%s" %(signum, sigstr))
        self.done = True


class PersistenceManager(object):    
    """Persistence manager.
    
    The persistence manager is in charge of persisting leases, and some
    scheduling data, to disk. This allows Haizea to recover from crashes.
    """    
    
    def __init__(self, file):
        """Constructor
        
        Initializes the persistence manager. If the specified file
        does not exist, it is created. If the file is created, it
        is opened but the information is not recovered (this is
        the responsibility of the Manager class)
        
        Arguments:
        file -- Persistence file. If None is specified, then
                persistence is disabled and Haizea will run entirely
                in-memory.
        """
        if file == None:
            self.disabled = True
            self.shelf = {}
        else:
            self.disabled = False
            file = os.path.expanduser(file)
            d = os.path.dirname(file)
            if not os.path.exists(d):
                os.makedirs(d)
            self.shelf = shelve.open(file, flag='c', protocol = -1)
        
    def persist_lease(self, lease):
        """Persists a single lease to disk
                
        Arguments:
        lease -- Lease to persist
        """        
        if not self.disabled:
            self.shelf["lease-%i" % lease.id] = lease
            self.shelf.sync()

    def persist_queue(self, queue):
        """Persists the queue to disk
                
        Arguments:
        queue -- The queue
        """        
        if not self.disabled:
            self.shelf["queue"] = [l.id for l in queue]
            self.shelf.sync()
        
    def persist_future_leases(self, leases):
        """Persists the set of future leases
                
        Arguments:
        leases -- "Future leases" (as determined by backfilling algorithm)
        """              
        if not self.disabled:
            self.shelf["future"] = [l.id for l in leases]        
            self.shelf.sync()
        
    def get_leases(self):
        """Returns the leases persisted to disk.
                
        """              
        return [v for k,v in self.shelf.items() if k.startswith("lease-")]
    
    def get_queue(self):
        """Returns the queue persisted to disk.
                
        """              
        if self.shelf.has_key("queue"):
            return self.shelf["queue"]
        else:
            return []
        
    def get_future_leases(self):
        """Returns the future leases persisted to disk.
                
        """              
        if self.shelf.has_key("future"):
            return self.shelf["future"]
        else:
            return []        
    
    def close(self):
        """Closes the persistence manager.
        
        Closing the persistence manager saves any remaining
        data to disk.
        """              
        if not self.disabled:
            self.shelf.close()
        
    