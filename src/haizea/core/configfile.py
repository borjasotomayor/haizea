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

from haizea.common.config import ConfigException, Section, Option, Config, OPTTYPE_INT, OPTTYPE_FLOAT, OPTTYPE_STRING, OPTTYPE_BOOLEAN, OPTTYPE_DATETIME, OPTTYPE_TIMEDELTA 
from haizea.common.utils import generate_config_name
import haizea.common.constants as constants
import haizea.common.defaults as defaults
import sys
from mx.DateTime import TimeDelta
import ConfigParser

try:
    import xml.etree.ElementTree as ET
except ImportError:
    # Compatibility with Python <=2.4
    import elementtree.ElementTree as ET 

class HaizeaConfig(Config):

    sections = []
    
    # ============================= #
    #                               #
    #        GENERAL OPTIONS        #
    #                               #
    # ============================= #

    general = Section("general", required=True,
                      doc = "This section is used for general options affecting Haizea as a whole.")
    general.options = \
    [
     Option(name        = "loglevel",
            getter      = "loglevel",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "INFO",
            valid       = ["STATUS","INFO","DEBUG","VDEBUG"],
            doc         = """
            Controls the level (and amount) of 
            log messages. Valid values are:
            
             - STATUS: Only print status messages
             - INFO: Slightly more verbose that STATUS
             - DEBUG: Prints information useful for debugging the scheduler.
             - VDEBUG: Prints very verbose information
               on the scheduler's internal data structures. Use only
               for short runs.        
            """),

     Option(name        = "logfile",
            getter      = "logfile",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "/var/tmp/haizea.log",
            doc         = """
            When running Haizea as a daemon, this option specifies the file
            that log messages should be written to.        
            """),
     
     Option(name        = "mode",
            getter      = "mode",
            type        = OPTTYPE_STRING,
            required    = True,
            valid       = ["simulated","opennebula"],
            doc         = """
            Sets the mode the scheduler will run in.
            Currently the only valid values are "simulated" and
            "opennebula". The "simulated" mode expects lease
            requests to be provided through a trace file, and
            all enactment is simulated. The "opennebula" mode
            interacts with the OpenNebula virtual infrastructure
            manager (http://www.opennebula.org/) to obtain lease
            requests and to do enactment on physical resources.                
            """),

     Option(name        = "lease-preparation",
            getter      = "lease-preparation",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.PREPARATION_UNMANAGED,
            valid       = [constants.PREPARATION_UNMANAGED,
                           constants.PREPARATION_TRANSFER],
            doc         = """
            Sets how the scheduler will handle the
            preparation overhead of leases. Valid values are:
            
             - unmanaged: The scheduler can assume that there
               is no deployment overhead, or that some
               other entity is taking care of it (e.g., one
               of the enactment backends)
             - imagetransfer: A disk image has to be transferred
               from a repository node before the lease can start.
            """),

     Option(name        = "lease-failure-handling",
            getter      = "lease-failure-handling",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.ONFAILURE_CANCEL,
            valid       = [constants.ONFAILURE_CANCEL,
                           constants.ONFAILURE_EXIT,
                           constants.ONFAILURE_EXIT_RAISE],
            doc         = """
            Sets how the scheduler will handle a failure in
            a lease. Valid values are:
            
             - cancel: The lease is cancelled and marked as "FAILED"
             - exit: Haizea will exit cleanly, printing relevant debugging
               information to its log.
             - exit-raise: Haizea will exit by raising an exception. This is
               useful for debugging, as IDEs will recognize this as an exception
               and will facilitate debugging it.
            """),

     Option(name        = "persistence-file",
            getter      = "persistence-file",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = defaults.PERSISTENCE_LOCATION,
            doc         = """
            This is the file where lease information, along with some
            additional scheduling information, is persisted to. If set
            to "none", no information will be persisted to disk, and
            Haizea will run entirely in-memory (this is advisable
            when running in simulation, as persisting to disk adds
            considerable overhead, compared to running in-memory).
            """)

    ]

    sections.append(general)

    # ============================= #
    #                               #
    #      SCHEDULING OPTIONS       #
    #                               #
    # ============================= #

    scheduling = Section("scheduling", required=True,
                         doc = "The options in this section control how Haizea schedules leases.")
    scheduling.options = \
    [
     Option(name        = "mapper",
            getter      = "mapper",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "greedy",
            doc         = """
            VM-to-physical node mapping algorithm used by Haizea. There is currently
            only one mapper available (the greedy mapper).
            """),

     Option(name        = "policy-admission",
            getter      = "policy.admission",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "accept-all",
            doc         = """
            Lease admission policy. This controls what leases are accepted by Haizea. 
            Take into account that this decision takes place before Haizea even 
            attempts to schedule the lease (so, you can think of lease admission as 
            "eligibility to be scheduled"). 
            
            There are two built-in policies:
            
             - accept-all: Accept all leases.
             - no-ARs: Accept all leases except advance reservations.
             
            See the Haizea documentation for details on how to write your own
            policies.
            """),

     Option(name        = "policy-preemption",
            getter      = "policy.preemption",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "no-preemption",
            doc         = """
            Lease preemption policy. Determines what leases can be preempted. There
            are two built-in policies:
            
             - no-preemption: Do not allow any preemptions
             - ar-preempts-everything: Allow all ARs to preempt other leases.
            
            See the Haizea documentation for details on how to write your own
            policies.
            """),
            
     Option(name        = "policy-host-selection",
            getter      = "policy.host-selection",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "greedy",
            doc         = """
            Physical host selection policy. controls how Haizea chooses what physical hosts 
            to map VMs to. This option is closely related to the mapper options 
            (if the greedy mapper is used, then the greedy host selection policy
            should be used, or unexpected results will happen). 
            
            The two built-in policies are:
             - no-policy: Choose nodes arbitrarily
             - greedy: Apply a greedy policy that tries to minimize the number
               of preemptions.
            
            See the Haizea documentation for details on how to write your own
            policies.
            """),

     Option(name        = "policy-pricing",
            getter      = "policy.pricing",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = "free",
            doc         = """
            ...
            
            See the Haizea documentation for details on how to write your own
            policies.
            """),
                        
     Option(name        = "wakeup-interval",
            getter      = "wakeup-interval",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            default     = TimeDelta(seconds=60),
            doc         = """
            Interval at which Haizea will wake up
            to manage resources and process pending requests.
            This option is not used when using a simulated clock,
            since the clock will skip directly to the time where an
            event is happening.
            """),

     Option(name        = "backfilling",
            getter      = "backfilling",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = None,
            valid       = [constants.BACKFILLING_OFF,
                           constants.BACKFILLING_AGGRESSIVE,
                           constants.BACKFILLING_CONSERVATIVE,
                           constants.BACKFILLING_INTERMEDIATE],
            doc         = """
            Backfilling algorithm to use. Valid values are:
            
             - off: don't do backfilling
             - aggressive: at most 1 reservation in the future
             - conservative: unlimited reservations in the future
             - intermediate: N reservations in the future (N is specified
               in the backfilling-reservations option)
            """),

     Option(name        = "backfilling-reservations",
            getter      = "backfilling-reservations",
            type        = OPTTYPE_INT,
            required    = False,
            required_if = [(("scheduling","backfilling"),constants.BACKFILLING_INTERMEDIATE)],
            doc         = """
            Number of future reservations to allow when
            using the "intermediate" backfilling option.
            """),

     Option(name        = "suspension",
            getter      = "suspension",
            type        = OPTTYPE_STRING,
            required    = True,
            valid       = [constants.SUSPENSION_NONE,
                           constants.SUSPENSION_SERIAL,
                           constants.SUSPENSION_ALL],
            doc         = """
            Specifies what can be suspended. Valid values are:
            
             - none: suspension is never allowed
             - serial-only: only 1-node leases can be suspended
             - all: any lease can be suspended                
            """),

     Option(name        = "suspend-rate",
            getter      = "suspend-rate",
            type        = OPTTYPE_FLOAT,
            required    = True,
            doc         = """
            Rate at which VMs are assumed to suspend (in MB of
            memory per second)                
            """),

     Option(name        = "resume-rate",
            getter      = "resume-rate",
            type        = OPTTYPE_FLOAT,
            required    = True,
            doc         = """
            Rate at which VMs are assumed to resume (in MB of
            memory per second)                
            """),

     Option(name        = "suspendresume-exclusion",
            getter      = "suspendresume-exclusion",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.SUSPRES_EXCLUSION_LOCAL,
            valid       = [constants.SUSPRES_EXCLUSION_LOCAL,
                           constants.SUSPRES_EXCLUSION_GLOBAL],
            doc         = """
            When suspending or resuming a VM, the VM's memory is dumped to a
            file on disk. To correctly estimate the time required to suspend
            a lease with multiple VMs, Haizea makes sure that no two 
            suspensions/resumptions happen at the same time (e.g., if eight
            memory files were being saved at the same time to disk, the disk's
            performance would be reduced in a way that is not as easy to estimate
            as if only one file were being saved at a time).
            
            Depending on whether the files are being saved to/read from a global
            or local filesystem, this exclusion can be either global or local.                        
            """),

     Option(name        = "scheduling-threshold-factor",
            getter      = "scheduling-threshold-factor",
            type        = OPTTYPE_INT,
            required    = False,
            default     = 1,
            doc         = """
            To avoid thrashing, Haizea will not schedule a lease unless all overheads
            can be correctly scheduled (which includes image transfers, suspensions, etc.).
            However, this can still result in situations where a lease is prepared,
            and then immediately suspended because of a blocking lease in the future.
            The scheduling threshold factor can be used to specify that a lease must
            not be scheduled unless it is guaranteed to run for a minimum amount of
            time (the rationale behind this is that you ideally don't want leases
            to be scheduled if they're not going to be active for at least as much time
            as was spent in overheads).
            
            The default value is 1, meaning that the lease will be active for at least
            as much time T as was spent on overheads (e.g., if preparing the lease requires
            60 seconds, and we know that it will have to be suspended, requiring 30 seconds,
            Haizea won't schedule the lease unless it can run for at least 90 minutes).
            In other words, a scheduling factor of F required a minimum duration of 
            F*T. A value of 0 could lead to thrashing, since Haizea could end up with
            situations where a lease starts and immediately gets suspended.               
            """),

     Option(name        = "override-suspend-time",
            getter      = "override-suspend-time",
            type        = OPTTYPE_INT,
            required    = False,
            default     = None,
            doc         = """
            Overrides the time it takes to suspend a VM to a fixed value
            (i.e., not computed based on amount of memory, enactment overhead, etc.)
            """),

     Option(name        = "override-resume-time",
            getter      = "override-resume-time",
            type        = OPTTYPE_INT,
            required    = False,
            default     = None,
            doc         = """
            Overrides the time it takes to suspend a VM to a fixed value
            (i.e., not computed based on amount of memory, enactment overhead, etc.)
            """),

     Option(name        = "force-scheduling-threshold",
            getter      = "force-scheduling-threshold",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            doc         = """
            This option can be used to force a specific scheduling threshold time
            to be used, instead of calculating one based on overheads.                
            """),

     Option(name        = "migration",
            getter      = "migration",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.MIGRATE_NO,          
            valid       = [constants.MIGRATE_NO,
                           constants.MIGRATE_YES,
                           constants.MIGRATE_YES_NOTRANSFER],              
            doc         = """
            Specifies whether leases can be migrated from one
            physical node to another. Valid values are: 
            
             - no
             - yes
             - yes-notransfer: migration is performed without
               transferring any files. 
            """),

     Option(name        = "non-schedulable-interval",
            getter      = "non-schedulable-interval",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            default     = TimeDelta(seconds=10),
            doc         = """
            The minimum amount of time that must pass between
            when a request is scheduled to when it can actually start.
            The default should be good for most configurations, but
            may need to be increased if you're dealing with exceptionally
            high loads.                
            """),

     Option(name        = "shutdown-time",
            getter      = "shutdown-time",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            default     = TimeDelta(seconds=0),
            doc         = """
            The amount of time that will be allocated for a VM to shutdown.
            When running in OpenNebula mode, it is advisable to set this to
            a few seconds, so no operation gets scheduled right when a
            VM is shutting down. The most common scenario is that a VM
            will start resuming right when another VM shuts down. However,
            since both these activities involve I/O, it can delay the resume
            operation and affect Haizea's estimation of how long the resume
            will take.
            """),

     Option(name        = "enactment-overhead",
            getter      = "enactment-overhead",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            default     = TimeDelta(seconds=0),
            doc         = """
            The amount of time that is required to send
            an enactment command. This value will affect suspend/resume
            estimations and, in OpenNebula mode, will force a pause
            of this much time between suspend/resume enactment
            commands. When suspending/resuming many VMs at the same time
            (which is likely to happen if suspendresume-exclusion is set
            to "local"), it will take OpenNebula 1-2 seconds to process
            each command (this is a small amount of time, but if 32 VMs
            are being suspended at the same time, on in each physical node,
            this time can compound up to 32-64 seconds, which has to be
            taken into account when estimating when to start a suspend
            operation that must be completed before another lease starts).
            """)

    ]
    sections.append(scheduling)
    
    # ============================= #
    #                               #
    #      SIMULATION OPTIONS       #
    #                               #
    # ============================= #
    
    simulation = Section("simulation", required=False,
                         required_if = [(("general","mode"),"simulated")],
                         doc = "This section is used to specify options when Haizea runs in simulation" )
    simulation.options = \
    [
     Option(name        = "clock",
            getter      = "clock",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.CLOCK_REAL,
            valid       = [constants.CLOCK_REAL,
                           constants.CLOCK_SIMULATED],
            doc         = """
            Type of clock to use in simulation:
            
             - simulated: A simulated clock that fastforwards through
               time. Can only use the tracefile request
               frontend
             - real: A real clock is used, but simulated resources and
               enactment actions are used. Can only use the RPC
               request frontend.                
            """),

     Option(name        = "starttime",
            getter      = "starttime",
            type        = OPTTYPE_DATETIME,
            required    = False,
            required_if = [(("simulation","clock"),constants.CLOCK_SIMULATED)],
            doc         = """
            Time at which simulated clock will start.                
            """),             

     Option(name        = "resources",
            getter      = "simul.resources",
            type        = OPTTYPE_STRING,
            required    = True,
            doc         = """
            Simulated resources. This option can take two values,
            "in-tracefile" (which means that the description of
            the simulated site is in the tracefile) or a string 
            specifying a site with homogeneous resources. 
            The format is:
        
            <numnodes> [ <resource_type>:<resource_quantity>]+
        
            For example, "4  CPU:100 Memory:1024" describes a site
            with four nodes, each with one CPU and 1024 MB of memory.
            """),

     Option(name        = "imagetransfer-bandwidth",
            getter      = "imagetransfer-bandwidth",
            type        = OPTTYPE_INT,
            required    = True,
            doc         = """
            Bandwidth (in Mbps) available for image transfers.
            This would correspond to the outbound network bandwidth of the
            node where the images are stored.                
            """),

     Option(name        = "stop-when",
            getter      = "stop-when",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.STOPWHEN_ALLDONE,
            valid       = [constants.STOPWHEN_ALLDONE,
                           constants.STOPWHEN_BESUBMITTED,
                           constants.STOPWHEN_BEDONE,
                           constants.STOPWHEN_EXACT],
            doc         = """
            When using the simulated clock, this specifies when the
            simulation must end. Valid options are:
            
             - all-leases-done: All requested leases have been completed
               and there are no queued/pending requests.
             - besteffort-submitted: When all best-effort leases have been
               submitted.
             - besteffort-done: When all best-effort leases have been
               completed.    
             - exact: Stop at a specific time (use option stop-when-time)            
            """),

     Option(name        = "stop-when-time",
            getter      = "stop-when-time",
            type        = OPTTYPE_STRING,
            required    = False,
            required_if = [(("simulation","stop-when"),constants.STOPWHEN_EXACT)],
            doc         = """
            A time in format DD:HH:MM:SS at which the simulation should be
            stopped (useful for debugging)            
            """),

     Option(name        = "status-message-interval",
            getter      = "status-message-interval",
            type        = OPTTYPE_INT,
            required    = False,
            default     = None,
            doc         = """
            If specified, the simulated clock will print a status
            message with some basic statistics. This is useful to keep track
            of long simulations. The interval is specified in minutes.                
            """),

     Option(name        = "sanity-check",
            getter      = "sanity-check",
            type        = OPTTYPE_BOOLEAN,
            required    = False,
            default     = False,
            doc         = """
            Perform a sanity check at every timestep (only for debugging)
            """),     

    ]
    sections.append(simulation)
    

    # ============================= #
    #                               #
    #      ACCOUNTING OPTIONS       #
    #                               #
    # ============================= #

    accounting = Section("accounting", required=True,
                      doc = "Haizea can collect information while running, and save that information to a file for off-line processing. This section includes options controlling this feature.")

    accounting.options = \
    [
     Option(name        = "datafile",
            getter      = "datafile",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = None,
            doc         = """
            This is the file where statistics on
            the scheduler's run will be saved to (waiting time of leases,
            utilization data, etc.). If omitted, no data will be saved.
            """),

     Option(name        = "probes",
            getter      = "accounting-probes",
            type        = OPTTYPE_STRING,
            required    = False,
            doc         = """
            Accounting probes.
            
            There are four built-in probes:
            
             - AR: Collects information on AR leases.
             - best-effort: Collects information on best effort leases.
             - immediate: Collects information immediate leases.
             - utilization: Collects information on resource utilization.
             
            See the Haizea documentation for details on how to write your
            own accounting probes.
      
            """),

     Option(name        = "attributes",
            getter      = "attributes",
            type        = OPTTYPE_STRING,
            required    = False,
            doc         = """
            This option is used internally by Haizea when using
            multiconfiguration files. See the multiconfiguration
            documentation for more details.        
            """)
    ]
    
    sections.append(accounting)

    # ============================= #
    #                               #
    #      DEPLOYMENT OPTIONS       #
    #     (w/ image transfers)      #
    #                               #
    # ============================= #

    imgtransfer = Section("deploy-imagetransfer", required=False,
                         required_if = [(("general","lease-deployment"),"imagetransfer")],
                         doc = """
                         When lease deployment with disk image transfers is selected,
                         this section is used to control image deployment parameters.""")
    imgtransfer.options = \
    [
     Option(name        = "transfer-mechanism",
            getter      = "transfer-mechanism",
            type        = OPTTYPE_STRING,
            required    = True,
            valid       = [constants.TRANSFER_UNICAST,
                           constants.TRANSFER_MULTICAST],
            doc         = """
            Specifies how disk images are transferred. Valid values are:
            
             - unicast: A disk image can be transferred to just one node at a time
             - multicast: A disk image can be multicast to multiple nodes at 
               the same time.                
            """),

     Option(name        = "avoid-redundant-transfers",
            getter      = "avoid-redundant-transfers",
            type        = OPTTYPE_BOOLEAN,
            required    = False,
            default     = True,
            doc         = """
            Specifies whether the scheduler should take steps to
            detect and avoid redundant transfers (e.g., if two leases are
            scheduled on the same node, and they both require the same disk
            image, don't transfer the image twice; allow one to "piggyback"
            on the other). There is generally no reason to set this option
            to False.
            """),

     Option(name        = "force-imagetransfer-time",
            getter      = "force-imagetransfer-time",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            doc         = """
            Forces the image transfer time to a specific amount.
            This options is intended for testing purposes.                
            """),
            
     Option(name        = "diskimage-reuse",
            getter      = "diskimage-reuse",
            type        = OPTTYPE_STRING,
            required    = False,
            required_if = None,
            default     = constants.REUSE_NONE,
            valid       = [constants.REUSE_NONE,
                           constants.REUSE_IMAGECACHES],
            doc         = """
            Specifies whether disk image caches should be created
            on the nodes, so the scheduler can reduce the number of transfers
            by reusing images. Valid values are:
            
             - none: No image reuse
             - image-caches: Use image caching algorithm described in Haizea
               publications
            """),

     Option(name        = "diskimage-cache-size",
            getter      = "diskimage-cache-size",
            type        = OPTTYPE_INT,
            required    = False,
            required_if = [(("deploy-imagetransfer","diskimage-reuse"),True)],
            doc         = """
            Specifies the size (in MB) of the disk image cache on
            each physical node.                
            """)
    ]
    sections.append(imgtransfer)

    # ============================= #
    #                               #
    #      TRACEFILE OPTIONS        #
    #                               #
    # ============================= #

    tracefile = Section("tracefile", required=False, 
                        doc="""
                        When reading in requests from a tracefile, this section is used
                        to specify the tracefile and other parameters.""")
    tracefile.options = \
    [
     Option(name        = "tracefile",
            getter      = "tracefile",
            type        = OPTTYPE_STRING,
            required    = True,
            doc         = """
            Path to tracefile to use.                
            """),

     Option(name        = "annotationfile",
            getter      = "annotationfile",
            type        = OPTTYPE_STRING,
            required    = False,
            doc         = """
            Path to lease annotation file.                
            """),

     Option(name        = "injectionfile",
            getter      = "injectionfile",
            type        = OPTTYPE_STRING,
            required    = False,
            doc         = """
            Path to file with leases to "inject" into the tracefile.                
            """),      
               
     Option(name        = "runtime-slowdown-overhead",
            getter      = "runtime-slowdown-overhead",
            type        = OPTTYPE_FLOAT,
            required    = False,
            default     = 0,
            doc         = """
            Adds a runtime overhead (in %) to the lease duration.                
            """),

     Option(name        = "add-overhead",
            getter      = "add-overhead",
            type        = OPTTYPE_STRING,
            required    = False,
            default     = constants.RUNTIMEOVERHEAD_NONE,
            valid       = [constants.RUNTIMEOVERHEAD_NONE,
                           constants.RUNTIMEOVERHEAD_ALL,
                           constants.RUNTIMEOVERHEAD_BE],
            doc         = """
            Specifies what leases will have a runtime overhead added:
            
             - none: No runtime overhead must be added.
             - besteffort: Add only to best-effort leases
             - all: Add runtime overhead to all leases                
            """),

     Option(name        = "bootshutdown-overhead",
            getter      = "bootshutdown-overhead",
            type        = OPTTYPE_TIMEDELTA,
            required    = False,
            default     = TimeDelta(seconds=0),
            doc         = """
            Specifies how many seconds will be alloted to
            boot and shutdown of the lease.                
            """),
                  
     Option(name        = "override-memory",
            getter      = "override-memory",
            type        = OPTTYPE_INT,
            required    = False,
            default     = constants.NO_MEMORY_OVERRIDE,
            doc         = """
            Overrides memory requirements specified in tracefile.
            """),
    ]
    sections.append(tracefile)
    
    # ============================= #
    #                               #
    #      OPENNEBULA OPTIONS       #
    #                               #
    # ============================= #

    opennebula = Section("opennebula", required=False,
                         required_if = [(("general","mode"),"opennebula")],
                         doc = """
                         This section is used to specify OpenNebula parameters,
                         necessary when using Haizea as an OpenNebula scheduling backend.""")
    opennebula.options = \
    [
     Option(name        = "host",
            getter      = "one.host",
            type        = OPTTYPE_STRING,
            required    = True,
            doc         = """
            Host where OpenNebula is running.
            Typically, OpenNebula and Haizea will be installed
            on the same host, so the following option should be
            set to 'localhost'. If they're on different hosts,
            make sure you modify this option accordingly.             
            """),

     Option(name        = "port",
            getter      = "one.port",
            type        = OPTTYPE_INT,
            required    = False,
            default     = defaults.OPENNEBULA_RPC_PORT,
            doc         = """
            TCP port of OpenNebula's XML RPC server             
            """),
            
     Option(name        = "stop-when-no-more-leases",
            getter      = "stop-when-no-more-leases",
            type        = OPTTYPE_BOOLEAN,
            required    = False,
            default     = False,
            doc         = """
            This option is useful for testing and running experiments.
            If set to True, Haizea will stop when there are no more leases
            to process (which allows you to tun Haizea and OpenNebula unattended,
            and count on it stopping when there are no more leases to process).
            For now, this only makes sense if you're seeding Haizea with requests from
            the start (otherwise, it will start and immediately stop).
            """),            

     Option(name        = "dry-run",
            getter      = "dry-run",
            type        = OPTTYPE_BOOLEAN,
            required    = False,
            default     = False,
            doc         = """
            This option is useful for testing.
            If set to True, Haizea will fast-forward through time (note that this is
            different that using the simulated clock, which has to be used with a tracefile;
            with an Haizea/OpenNebula dry run, you will have to seed OpenNebula with requests
            before starting Haizea). You will generally want to set stop-when-no-more-leases
            when doing a dry-run.
            
            IMPORTANT: Haizea will still send out enactment commands to OpenNebula. Make
            sure you replace onevm with a dummy command that does nothing (or that reacts
            in some way you want to test; e.g., by emulating a deployment failure, etc.)
            """),            

    ]
    sections.append(opennebula)
    
    def __init__(self, config):
        Config.__init__(self, config, self.sections)

        self.attrs = {}
        if self._options["attributes"] != None:
            attrs = self._options["attributes"].split(",")
            for attr in attrs:
                (k,v) = attr.split("=")
                self.attrs[k] = v

        
    def get_attr(self, attr):
        return self.attrs[attr]
        
    def get_attrs(self):
        return self.attrs.keys()


class HaizeaMultiConfig(Config):
    
    MULTI_SEC = "multi"
    COMMON_SEC = "common"
    TRACEDIR_OPT = "tracedir"
    TRACEFILES_OPT = "tracefiles"
    ANNOTATIONDIR_OPT = "annotationdir"
    ANNOTATIONFILES_OPT = "annotationfiles"
    SKIP_NO_ANNOTATION_OPT = "skip-no-annotation"
    INJDIR_OPT = "injectiondir"
    INJFILES_OPT = "injectionfiles"
    SKIP_NO_INJECTION_OPT = "skip-no-injection"
    DATADIR_OPT = "datadir"
    
    def __init__(self, config):
        # TODO: Define "multi" section as a Section object
        Config.__init__(self, config, [])
        
    def get_profiles(self):
        sections = set([s.split(":")[0] for s in self.config.sections()])
        # Remove multi and common sections
        sections.difference_update([self.COMMON_SEC, self.MULTI_SEC])
        return list(sections)

    def get_trace_files(self):
        dir = self.config.get(self.MULTI_SEC, self.TRACEDIR_OPT)
        traces = self.config.get(self.MULTI_SEC, self.TRACEFILES_OPT).split()
        return [dir + "/" + t for t in traces]

    def get_annotation_files(self):
        if not self.config.has_option(self.MULTI_SEC, self.ANNOTATIONDIR_OPT):
            return [None]
        else:
            dir = self.config.get(self.MULTI_SEC, self.ANNOTATIONDIR_OPT)
            annot = self.config.get(self.MULTI_SEC, self.ANNOTATIONFILES_OPT).split()
            annot = [dir + "/" + t for t in annot]
            annot.append(None)
            return annot

    def get_inject_files(self):
        if not self.config.has_option(self.MULTI_SEC, self.INJDIR_OPT):
            return [None]
        else:
            dir = self.config.get(self.MULTI_SEC, self.INJDIR_OPT)
            inj = self.config.get(self.MULTI_SEC, self.INJFILES_OPT).split()
            inj = [dir + "/" + i for i in inj]
            inj.append(None)
            return inj
    
    def get_configs(self):
        profiles = self.get_profiles()
        tracefiles = self.get_trace_files()
        annotationfiles = self.get_annotation_files()
        injectfiles = self.get_inject_files()

        if not self.config.has_option(self.MULTI_SEC, self.SKIP_NO_INJECTION_OPT):
            skip_no_injection = False
        else:
            skip_no_injection = self.config.getboolean(self.MULTI_SEC, self.SKIP_NO_INJECTION_OPT)
            
        if not self.config.has_option(self.MULTI_SEC, self.SKIP_NO_ANNOTATION_OPT):
            skip_no_annotation = False
        else:
            skip_no_annotation = self.config.getboolean(self.MULTI_SEC, self.SKIP_NO_ANNOTATION_OPT)
        
        no_annotations = (annotationfiles == [None])
        no_injections = (injectfiles == [None])

        configs = []
        for profile in profiles:
            for tracefile in tracefiles:
                for annotationfile in annotationfiles:
                    for injectfile in injectfiles:
                        if annotationfile == None and skip_no_annotation:
                            continue
                        if injectfile == None and skip_no_injection:
                            continue
                        profileconfig = ConfigParser.ConfigParser()
                        commonsections = [s for s in self.config.sections() if s.startswith("common:")]
                        profilesections = [s for s in self.config.sections() if s.startswith(profile +":")]
                        sections = commonsections + profilesections
                        for s in sections:
                            s_noprefix = s.split(":")[1]
                            items = self.config.items(s)
                            if not profileconfig.has_section(s_noprefix):
                                profileconfig.add_section(s_noprefix)
                            for item in items:
                                profileconfig.set(s_noprefix, item[0], item[1])
                                
                        # The tracefile section may have not been created
                        if not profileconfig.has_section("tracefile"):
                            profileconfig.add_section("tracefile")
    
                        # Add tracefile option
                        profileconfig.set("tracefile", "tracefile", tracefile)
                        
                        # Add injected file option
                        if injectfile != None:
                            profileconfig.set("tracefile", "injectionfile", injectfile)
                            
                        # Add annotations file option
                        if annotationfile != None:
                            profileconfig.set("tracefile", "annotationfile", annotationfile)
    
                        # Add datafile option
                        datadir = self.config.get(self.MULTI_SEC, self.DATADIR_OPT)
                        datafilename = generate_config_name(profile, tracefile, annotationfile, injectfile)
                        datafile = datadir + "/" + datafilename + ".dat"
                        # The accounting section may have not been created
                        if not profileconfig.has_section("accounting"):
                            profileconfig.add_section("accounting")
                        profileconfig.set("accounting", "datafile", datafile)
                        
                        # Set "attributes" option (only used internally)
                        attrs = {"profile":profile,"tracefile":tracefile,"injectfile":injectfile,"annotationfile":annotationfile}
                        
                        trace_attrs = self.__load_attributes_from_file(tracefile)
                        attrs.update(trace_attrs)
                        
                        if injectfile != None:
                            inj_attrs = self.__load_attributes_from_file(injectfile)
                            attrs.update(inj_attrs)

                        if annotationfile != None:
                            annot_attrs = self.__load_attributes_from_file(annotationfile)
                            attrs.update(annot_attrs)
                        
                        attrs_str = ",".join(["%s=%s" % (k,v) for (k,v) in attrs.items()])
                        if profileconfig.has_option("accounting", "attributes"):
                            attrs_str += ",%s" % profileconfig.get("accounting", "attributes")
                        profileconfig.set("accounting", "attributes", attrs_str)
    
                        try:
                            c = HaizeaConfig(profileconfig)
                        except ConfigException, msg:
                            print >> sys.stderr, "Error in configuration file:"
                            print >> sys.stderr, msg
                            exit(1)
                        configs.append(c)
        
        return configs
    
    def __load_attributes_from_file(self, file):
        attrs = {}
        context = ET.iterparse(file, events=("start", "end"))
        for event, elem in context:
            if event == "start" and elem.tag in ("lease-annotation", "lease-request"):
                break                
            if event == "end" and elem.tag == "attributes":
                for attr_elem in elem:
                    attrs[attr_elem.get("name")] = attr_elem.get("value")
                break
        return attrs
        