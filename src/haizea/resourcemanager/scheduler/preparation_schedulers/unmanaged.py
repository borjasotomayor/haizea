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

from haizea.resourcemanager.leases import Lease
from haizea.resourcemanager.scheduler.preparation_schedulers import PreparationScheduler
import haizea.common.constants as constants

class UnmanagedPreparationScheduler(PreparationScheduler):
    def __init__(self, slottable, resourcepool, deployment_enact):
        PreparationScheduler.__init__(self, slottable, resourcepool, deployment_enact)
        self.handlers = {}
    
    # Add dummy disk images
    def schedule(self, lease, vmrr, nexttime):
        for (vnode, pnode) in vmrr.nodes.items():
            self.resourcepool.add_diskimage(pnode, lease.diskimage_id, lease.diskimage_size, lease.id, vnode)
        return [], True
    
    def find_earliest_starting_times(self, lease, nexttime):
        nod_ids = [n.nod_id for n in self.resourcepool.get_nodes()]
        earliest = dict([(node, [nexttime, constants.REQTRANSFER_NO, None]) for node in nod_ids])
        return earliest
            
    def cancel_preparation(self, lease):
        self.cleanup(lease)
        lease.diskimagemap = {}

    def cleanup(self, lease):
        for vnode, pnode in lease.diskimagemap.items():
            self.resourcepool.remove_diskimage(pnode, lease.id, vnode)