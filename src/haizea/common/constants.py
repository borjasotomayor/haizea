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

# Types of resources
RES_CPU = 0
RES_MEM = 1
RES_NETIN = 2
RES_NETOUT = 3
RES_DISK = 4

# Types of types of resources
RESTYPE_FLOAT = 0
RESTYPE_INT = 1


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

SUSPRES_EXCLUSION_LOCAL="local"
SUSPRES_EXCLUSION_GLOBAL="global"

MIGRATE_NONE="nothing"
MIGRATE_MEM="mem"
MIGRATE_MEMDISK="mem+disk"

TRANSFER_UNICAST="unicast"
TRANSFER_MULTICAST="multicast"

STOPWHEN_ALLDONE = "all-leases-done"
STOPWHEN_BESUBMITTED="besteffort-submitted"
STOPWHEN_BEDONE="besteffort-done"

REUSE_NONE="none"
REUSE_IMAGECACHES="image-caches"

RUNTIMEOVERHEAD_NONE="none"
RUNTIMEOVERHEAD_ALL="all"
RUNTIMEOVERHEAD_BE="besteffort"

DEPLOYMENT_UNMANAGED = "unmanaged"
DEPLOYMENT_PREDEPLOY = "predeployed-images"
DEPLOYMENT_TRANSFER = "imagetransfer"

CLOCK_SIMULATED = "simulated"
CLOCK_REAL = "real"

# Transfer required in deployment
REQTRANSFER_NO = 0
REQTRANSFER_YES = 1
REQTRANSFER_COWPOOL = 2
REQTRANSFER_PIGGYBACK = 3

# Misc
BETTER = -1
EQUAL = 0
WORSE = 1

DIRECTION_FORWARD = 0
DIRECTION_BACKWARD = 1
        
CACHESIZE_UNLIMITED = -1



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

EVENT_END_VM=0

LOGLEVEL_VDEBUG = 5
LOGLEVEL_STATUS = 25

