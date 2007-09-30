# Types of tracefiles
TRACE_CSV=0
TRACE_GWF=1

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
GENERAL_SEC="general"
LOGLEVEL_OPT="loglevel"
PROFILE_OPT="profile"

SIMULATION_SEC="simulation"

STARTTIME_OPT="starttime"
TEMPLATEDB_OPT="templatedb"
TARGETDB_OPT="targetdb"
NODES_OPT="nodes"
BANDWIDTH_OPT="bandwidth"
RESOURCES_OPT="resources"

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

