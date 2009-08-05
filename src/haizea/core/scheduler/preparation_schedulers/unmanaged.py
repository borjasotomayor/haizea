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

from haizea.core.leases import Lease
from haizea.core.scheduler import EarliestStartingTime
from haizea.core.scheduler.preparation_schedulers import PreparationScheduler
import haizea.common.constants as constants
from mx.DateTime import TimeDelta

class UnmanagedPreparationScheduler(PreparationScheduler):
    def __init__(self, slottable, resourcepool, deployment_enact):
        PreparationScheduler.__init__(self, slottable, resourcepool, deployment_enact)
        self.handlers = {}
    
    def schedule(self, lease, vmrr, nexttime):
        # Nothing to do
        return [], True
    
    def find_earliest_starting_times(self, lease, nexttime):
        # The earliest starting time is "nexttime" on all nodes.
        node_ids = [node.id for node in self.resourcepool.get_nodes()]
        earliest = {}
        for node in node_ids:
            earliest[node] = EarliestStartingTime(nexttime, EarliestStartingTime.EARLIEST_NOPREPARATION)
        return earliest
            
    def estimate_migration_time(self, lease):
        return TimeDelta(seconds=0)     
            
    def schedule_migration(self, lease, vmrr, nexttime):
        return []
                
    def cancel_preparation(self, lease):
        self.cleanup(lease)

    def cleanup(self, lease):
        # Nothing to clean up.
        pass