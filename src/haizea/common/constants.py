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

# Types of tracefiles
TRACE_CSV=0
TRACE_SWF=1
TRACE_GWF=2

# Types of leases
LEASE_BESTEFFORT = 0
LEASE_AR = 1

# Types of resources
RES_CPU = 0
RES_MEM = 1
RES_NETIN = 2
RES_NETOUT = 3
RES_DISK = 4

# Types of types of resources
RESTYPE_FLOAT = 0
RESTYPE_INT = 1

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
MODE_OPT="mode"
LEASE_DEPLOYMENT_OPT="lease-deployment"
PROFILE_OPT="profile"
SUSPENSION_OPT="suspension"
MIGRATION_OPT="migration"
MIGRATE_OPT="what-to-migrate"
TRANSFER_OPT="diskimage-transfer"
BACKFILLING_OPT="backfilling"
RESERVATIONS_OPT="backfilling-reservations"
TRACEFILE_OPT="tracefile"
INJFILE_OPT="injectionfile"
IMGFILE_OPT="imagefile"
REUSE_OPT="reuse"
CACHESIZE_OPT="diskimage-cache-size"
NODESELECTION_OPT="nodeselection"
DATADIR_OPT="datadir"

REPORTING_SEC="reporting"
CSS_OPT="css"
REPORTDIR_OPT="reportdir"
TABLE_OPT="table"
SLIDESHOW_OPT="slideshow"
CLIPSTART_OPT="clip-start"
CLIPEND_OPT="clip-end"

SIMULATION_SEC="simulation"
STARTTIME_OPT="starttime"
TEMPLATEDB_OPT="templatedb"
TARGETDB_OPT="targetdb"
NODES_OPT="nodes"
BANDWIDTH_OPT="imagetransfer-bandwidth"
SUSPENDRATE_OPT="suspendresume-rate"
SUSPENDTHRESHOLD_OPT="suspend-threshold"
SUSPENDTHRESHOLDFACTOR_OPT="suspend-threshold-factor"
RESOURCES_OPT="resources"
STOPWHEN_OPT="stop-when"
RUNOVERHEAD_OPT="runtime-overhead"
BOOTOVERHEAD_OPT="bootshutdown-overhead"
RUNOVERHEADBE_OPT="runtime-overhead-onlybesteffort"
FORCETRANSFERT_OPT="force-transfer-time"
REUSE_OPT="diskimage-reuse"
AVOIDREDUNDANT_OPT="avoid-redundant-transfers"
STATUS_INTERVAL_OPT="status-message-interval"

OPENNEBULA_SEC = "opennebula"
DB_OPT = "db"
ONEVM_OPT = "onevm"
ESTIMATESUSPENDRATE_OPT = "suspendresume-rate-estimate"
WAKEUPINTERVAL_OPT = "wakeup-interval"
NONSCHEDULABLE_OPT = "non-schedulable-interval"

DEPLOY_IMAGETRANSFER_SEC = "deploy-imagetransfer"
TRANSFER_MECHANISM_OPT = "transfer-mechanism"

SCHEDULING_SEC="scheduling"
TRACEFILE_SEC="tracefile"

COMMON_SEC="common"
MULTI_SEC="multi"
BASEDATADIR_OPT="basedatadir"

MODE_SIMULATION="simulation"
MODE_OPENNEBULA="opennebula"

BACKFILLING_OFF="off"
BACKFILLING_AGGRESSIVE="aggressive"
BACKFILLING_CONSERVATIVE="conservative"
BACKFILLING_INTERMEDIATE="intermediate"

SUSPENSION_NONE="none"
SUSPENSION_SERIAL="serial-only"
SUSPENSION_ALL="all"

MIGRATE_NONE="nothing"
MIGRATE_MEM="mem"
MIGRATE_MEMVM="mem+vm"

TRANSFER_NONE="none"
TRANSFER_UNICAST="unicast"
TRANSFER_MULTICAST="multicast"

STOPWHEN_BESUBMITTED="best-effort-submitted"
STOPWHEN_BEDONE="best-effort-done"

REUSE_NONE="none"
REUSE_IMAGECACHES="image-caches"

NODESELECTION_AVOIDPREEMPT="avoid-preemption"
NODESELECTION_PREFERREUSE="prefer-imagereuse"

DEPLOYMENT_UNMANAGED = "unmanaged"
DEPLOYMENT_PREDEPLOY = "predeployed-images"
DEPLOYMENT_TRANSFER = "imagetransfer"

# Graph configfile sections and options
TITLE_OPT="title"
DATAFILE_OPT="datafile"
TITLEX_OPT="title-x"
TITLEY_OPT="title-y"
GRAPHTYPE_OPT="graphtype"
PROFILE_OPT="profile"
TRACE_OPT="trace"
INJ_OPT="injection"

GRAPH_LINE_VALUE="line-value"
GRAPH_LINE_AVG="line-average"
GRAPH_STEP_VALUE="step-value"
GRAPH_POINT_VALUE="point-value"
GRAPH_POINTLINE_VALUEAVG="point-value+line-avg"
GRAPH_CUMULATIVE="cumulative"
GRAPH_NUMNODE_LENGTH_CORRELATION_SIZE="numnode-length-correlation-insize"
GRAPH_NUMNODE_LENGTH_CORRELATION_Y="numnode-length-correlation-iny"
GRAPH_NUMNODE_REQLENGTH_CORRELATION_SIZE="numnode-reqlength-correlation-insize"
GRAPH_NUMNODE_REQLENGTH_CORRELATION_Y="numnode-reqlength-correlation-iny"

# Component names
RM="RM"
SCHED="SCHED"
ST="SLOT"
DS="STRUCT"
ENACT="ENACT"
CLOCK="CLOCK"
ONE="ONE"
TRACE="TRACE"

# Transfer required in deployment
REQTRANSFER_NO = 0
REQTRANSFER_YES = 1
REQTRANSFER_COWPOOL = 2
REQTRANSFER_PIGGYBACK = 3

# Misc
BETTER = -1
EQUAL = 0
WORSE = 1


# Types of final tables in report generation
TABLE_FINALVALUE="final-value"
TABLE_FINALTIME="final-time"
TABLE_FINALAVG="final-avg"

# Trace config file secs and opts
INTERVAL_SEC = "interval"
NUMNODES_SEC = "numnodes"
DURATION_SEC = "duration"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"
WORKLOAD_SEC = "workload"

DURATION_OPT = "duration"

DISTRIBUTION_OPT = "distribution"
MIN_OPT = "min"
MAX_OPT = "max"
ITEMS_OPT = "items"
ITEMSPROBS_OPT = "itemswithprobs"
MEAN_OPT = "mean"
STDEV_OPT = "stdev"

PERCENT_OPT = "percent"
NUMNODES_OPT = "numnodes"


DIST_UNIFORM = "uniform"
DIST_EXPLICIT = "explicit"

# Image config file secs and opts
IMAGES_OPT="images"
DISTRIBUTION_OPT="distribution"
LENGTH_OPT="filelength"
SIZE_SEC="size"

CLIP_PERCENTSUBMITTED = "percent"
CLIP_TIMESTAMP = "timestamp"
CLIP_LASTSUBMISSION = "last-submission"
CLIP_NOCLIP = "noclip"

CACHESIZE_UNLIMITED = -1

REPORT_ALL="all"
REPORT_BASH="bash"
REPORT_CONDOR="condor"
REPORT_SINGLE_PROFILE="singletrace"
REPORT_SINGLE_TRACE="singleprofile"

DOING_IDLE=0
DOING_TRANSFER=1
DOING_VM_SUSPEND=99
DOING_VM_RUN=100
DOING_VM_RESUME=101
DOING_TRANSFER_NOVM=666

ENACT_PACKAGE="haizea.resourcemanager.enact"

COUNTER_ARACCEPTED="Accepted AR"
COUNTER_ARREJECTED="Rejected AR"
COUNTER_IMACCEPTED="Accepted Immediate"
COUNTER_IMREJECTED="Rejected Immediate"
COUNTER_BESTEFFORTCOMPLETED="Best-effort completed"
COUNTER_QUEUESIZE="Queue size"
COUNTER_DISKUSAGE="Disk usage"
COUNTER_CPUUTILIZATION="CPU utilization"

AVERAGE_NONE=0
AVERAGE_NORMAL=1
AVERAGE_TIMEWEIGHTED=2
