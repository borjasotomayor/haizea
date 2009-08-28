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

# Types of resources
RES_CPU = "CPU"
RES_MEM = "Memory"
RES_NETIN = "Net-in"
RES_NETOUT = "Net-out"
RES_DISK = "Disk"

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

MIGRATE_NO="no"
MIGRATE_YES="yes"
MIGRATE_YES_NOTRANSFER="yes-notransfer"

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

PREPARATION_UNMANAGED = "unmanaged"
PREPARATION_PREDEPLOY = "predeployed-images"
PREPARATION_TRANSFER = "imagetransfer"

CLOCK_SIMULATED = "simulated"
CLOCK_REAL = "real"

# Misc
BETTER = -1
EQUAL = 0
WORSE = 1

DIRECTION_FORWARD = 0
DIRECTION_BACKWARD = 1
        
CACHESIZE_UNLIMITED = -1



ENACT_PACKAGE="haizea.core.enact"

EVENT_END_VM=0

LOGLEVEL_VDEBUG = 5
LOGLEVEL_STATUS = 25

NO_MEMORY_OVERRIDE = -1

ONFAILURE_CANCEL = "cancel"
ONFAILURE_EXIT = "exit"
ONFAILURE_EXIT_RAISE = "exit-raise"