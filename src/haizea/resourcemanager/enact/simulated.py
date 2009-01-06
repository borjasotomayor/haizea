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

from haizea.resourcemanager.scheduler.resourcepool import Node
from haizea.resourcemanager.scheduler.slottable import ResourceTuple
from haizea.resourcemanager.enact import ResourcePoolInfo, VMEnactment, DeploymentEnactment
import haizea.common.constants as constants
from haizea.common.utils import get_config
import logging

class SimulatedResourcePoolInfo(ResourcePoolInfo):
    def __init__(self):
        ResourcePoolInfo.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
        config = get_config()
                
        numnodes = config.get("simul.nodes")

        capacity = self.parse_resources_string(config.get("simul.resources"))
        
        self.nodes = [Node(i+1, "simul-%i" % (i+1), capacity) for i in range(numnodes)]
        for n in self.nodes:
            n.enactment_info = n.nod_id
        
    def get_nodes(self):
        return self.nodes
    
    def get_resource_types(self):
        return [(constants.RES_CPU, constants.RESTYPE_FLOAT, "CPU"),
                (constants.RES_MEM,  constants.RESTYPE_INT, "Mem"),
                (constants.RES_DISK, constants.RESTYPE_INT, "Disk"),
                (constants.RES_NETIN, constants.RESTYPE_INT, "Net (in)"),
                (constants.RES_NETOUT, constants.RESTYPE_INT, "Net (out)")]
        
    def parse_resources_string(self, resources):
        resources = resources.split(";")
        desc2type = dict([(x[2], x[0]) for x in self.get_resource_types()])
        capacity = ResourceTuple.create_empty()
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity.set_by_type(desc2type[resourcename], int(resourcecapacity))
        return capacity
    
    def get_migration_bandwidth(self):
        return 100 # TODO: Get from config file

class SimulatedVMEnactment(VMEnactment):
    def __init__(self):
        VMEnactment.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.VM")
        
    def start(self, action):
        for vnode in action.vnodes:
            # Unpack action
            pnode = action.vnodes[vnode].pnode
            image = action.vnodes[vnode].diskimage
            cpu = action.vnodes[vnode].resources.get_by_type(constants.RES_CPU)
            memory = action.vnodes[vnode].resources.get_by_type(constants.RES_MEM)
            self.logger.debug("Received request to start VM for L%iV%i on host %i, image=%s, cpu=%i, mem=%i"
                         % (action.lease_haizea_id, vnode, pnode, image, cpu, memory))
    
    def stop(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to stop VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def suspend(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to suspend VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def resume(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to resume VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def verify_suspend(self, action):
        return 0
    
    def verify_resume(self, action):
        return 0
    
class SimulatedDeploymentEnactment(DeploymentEnactment):    
    def __init__(self):
        DeploymentEnactment.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
        config = get_config()
                
        self.bandwidth = config.get("imagetransfer-bandwidth")
                
        # Image repository nodes
        numnodes = config.get("simul.nodes")
        
        imgcapacity = ResourceTuple.create_empty()
        imgcapacity.set_by_type(constants.RES_NETOUT, self.bandwidth)

        self.fifo_node = Node(numnodes+1, "FIFOnode", imgcapacity)
        self.edf_node = Node(numnodes+2, "EDFnode", imgcapacity)
        
    def get_edf_node(self):
        return self.edf_node
    
    def get_fifo_node(self):
        return self.fifo_node       
    
    def get_aux_nodes(self):
        return [self.edf_node, self.fifo_node] 
    
    def get_bandwidth(self):
        return self.bandwidth
        
    def resolve_to_file(self, lease_id, vnode, diskimage_id):
        return "/var/haizea/images/%s-L%iV%i" % (diskimage_id, lease_id, vnode)