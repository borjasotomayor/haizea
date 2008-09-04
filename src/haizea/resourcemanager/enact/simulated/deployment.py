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

from haizea.resourcemanager.enact.base import DeploymentEnactmentBase
from haizea.resourcemanager.resourcepool import Node
import haizea.resourcemanager.datastruct as ds
import haizea.common.constants as constants
import logging

baseCachePath="/vm/cache"
baseWorkingPath="/vm/working"
stagingPath="/vm/staging"

class DeploymentEnactment(DeploymentEnactmentBase):
    def __init__(self, resourcepool):
        DeploymentEnactmentBase.__init__(self, resourcepool)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
        config = self.resourcepool.rm.config
                
        self.bandwidth = config.get("imagetransfer-bandwidth")
                
        # Image repository nodes
        numnodes = config.get("simul.nodes")
        
        imgcapacity = ds.ResourceTuple.create_empty()
        imgcapacity.set_by_type(constants.RES_NETOUT, self.bandwidth)

        self.fifo_node = Node(self.resourcepool, numnodes+1, "FIFOnode", imgcapacity)
        self.edf_node = Node(self.resourcepool, numnodes+2, "EDFnode", imgcapacity)
        
    def get_edf_node(self):
        return self.edf_node
    
    def get_fifo_node(self):
        return self.fifo_node       
    
    def get_aux_nodes(self):
        return [self.edf_node, self.fifo_node] 
    
    def get_bandwidth(self):
        return self.bandwidth
        
    def resolve_to_file(self, lease_id, vnode, diskimage_id):
        return "%s/%s-L%iV%i" % (baseWorkingPath, diskimage_id, lease_id, vnode)