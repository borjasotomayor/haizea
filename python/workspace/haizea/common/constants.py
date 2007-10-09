# Types of tracefiles
TRACE_CSV=0
TRACE_SWF=1
TRACE_GWF=2

# Types of leases
LEASE_BESTEFFORT = 0
LEASE_EXACT = 1

# Types of resources
RES_CPU = 1
RES_MEM = 2
RES_NETIN = 3
RES_NETOUT = 4
RES_DISK = 5

def res_str(s):
    if s == RES_CPU:
        return "CPU"
    elif s == RES_MEM:
        return "Mem"
    elif s == RES_NETIN:
        return "Net (in)"
    elif s == RES_NETOUT:
        return "Net (out)"
    elif s == RES_DISK:
        return "Disk"


# Types of file transfers
TRANSFER_DEPLOY = 0
TRANSFER_MIGRATE = 1

# On complete
ONCOMPLETE_ENDLEASE = 0
ONCOMPLETE_SUSPEND = 1

# Lease states
LEASE_STATE_PENDING = 0
LEASE_STATE_SCHEDULED = 1
LEASE_STATE_DEPLOYING = 2
LEASE_STATE_DEPLOYED = 3
LEASE_STATE_ACTIVE = 4
LEASE_STATE_SUSPENDED = 5
LEASE_STATE_DONE = 6

def state_str(s):
    if s == LEASE_STATE_PENDING:
        return "Pending"
    elif s == LEASE_STATE_SCHEDULED:
        return "Scheduled"
    elif s == LEASE_STATE_DEPLOYING:
        return "Deploying"
    elif s == LEASE_STATE_DEPLOYED:
        return "Deployed"
    elif s == LEASE_STATE_ACTIVE:
        return "Active"
    elif s == LEASE_STATE_SUSPENDED:
        return "Suspended"
    elif s == LEASE_STATE_DONE:
        return "Done"

# Resource reservation states
RES_STATE_SCHEDULED = 0
RES_STATE_ACTIVE = 1
RES_STATE_DONE = 2

def rstate_str(s):
    if s == RES_STATE_SCHEDULED:
        return "Scheduled"
    elif s == RES_STATE_ACTIVE:
        return "Active"
    elif s == RES_STATE_DONE:
        return "Done"

# Configfile sections and options
PROFILES_SEC="profiles"
NAMES_OPT="names"

TRACES_SEC="traces"
TRACEDIR_OPT="tracedir"
TRACEFILES_OPT="tracefiles"

INJECTIONS_SEC="leaseinjections"
INJDIR_OPT="injectiondir"
INJFILES_OPT="injectionfiles"

RUN_SEC="run"
PROFILES_OPT="profiles"
TRACES_OPT="traces"
INJS_OPT="injections"

GENERAL_SEC="general"
LOGLEVEL_OPT="loglevel"
PROFILE_OPT="profile"
SUSPENSION_OPT="suspension"
BACKFILLING_OPT="backfilling"
RESERVATIONS_OPT="backfilling-reservations"
TRACEFILE_OPT="tracefile"
INJFILE_OPT="injectionfile"

REPORTING_SEC="reporting"
CSS_OPT="css"
REPORTDIR_OPT="reportdir"
CLIPSTART_OPT="clip-start"
CLIPEND_OPT="clip-end"

SIMULATION_SEC="simulation"
STARTTIME_OPT="starttime"
TEMPLATEDB_OPT="templatedb"
TARGETDB_OPT="targetdb"
NODES_OPT="nodes"
BANDWIDTH_OPT="bandwidth"
RESOURCES_OPT="resources"


BACKFILLING_OFF="off"
BACKFILLING_AGGRESSIVE="aggressive"
BACKFILLING_CONSERVATIVE="conservative"
BACKFILLING_INTERMEDIATE="intermediate"

# Component names
RM="RM"
SCHED="SCHED"
ST="SLOT"
DB="DB"
DS="STRUCT"

# Transfer required in deployment
TRANSFER_NO = 0

# Misc
BETTER = -1
EQUAL = 0
WORSE = 1


# Data filenames
CPUUTILFILE="cpuutil.dat"
MEMUTILFILE="memutil.dat"
ACCEPTEDFILE="accepted.dat"
REJECTEDFILE="rejected.dat"
COMPLETEDFILE="besteffort-completed.dat"
QUEUESIZEFILE="queuesize.dat"
QUEUEWAITFILE="queuewait.dat"
EXECWAITFILE="execwait.dat"
CLIPTIMESFILE="cliptimes.dat"


# Types of final tables in report generation
TABLE_FINALVALUE=0
TABLE_FINALTIME=1
TABLE_FINALAVG=2

# Types of graphs
GRAPH_LINE_VALUE=0
GRAPH_LINE_AVG=1
GRAPH_STEP_VALUE=2
GRAPH_POINT_VALUE=3
GRAPH_POINTLINE_VALUEAVG=4


# Trace config file secs and opts
INTERVAL_SEC = "interval"
NUMNODES_SEC = "numnodes"
DURATION_SEC = "duration"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"

BANDWIDTH_OPT = "bandwidth"
DURATION_OPT = "duration"

DISTRIBUTION_OPT = "distribution"
MIN_OPT = "min"
MAX_OPT = "max"
ITEMS_OPT = "items"
ITEMSPROBS_OPT = "itemswithprobs"
MEAN_OPT = "mean"
STDEV_OPT = "stdev"

DIST_UNIFORM = "uniform"
DIST_EXPLICIT = "explicit"

CLIP_BYTIME = 0
CLIP_BYLEASE = 1