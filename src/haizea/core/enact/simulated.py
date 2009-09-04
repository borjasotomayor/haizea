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

from haizea.core.leases import Capacity
from haizea.core.scheduler.resourcepool import ResourcePoolNode
from haizea.core.enact import ResourcePoolInfo, VMEnactment, DeploymentEnactment
import haizea.common.constants as constants
import logging

class SimulatedResourcePoolInfo(ResourcePoolInfo):
    def __init__(self, site):
        ResourcePoolInfo.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
                
        if not ("CPU" in site.resource_types and "Memory" in site.resource_types):
            # CPU and Memory must be specified
            # TODO: raise something more meaningful
            raise
        
        # Disk and network should be specified but, if not, we can
        # just add arbitrarily large values.
        if not "Disk" in site.resource_types:
            site.add_resource("Disk", [1000000])

        if not "Net-in" in site.resource_types:
            site.add_resource("Net-in", [1000000])

        if not "Net-out" in site.resource_types:
            site.add_resource("Net-out", [1000000])
        
        self.resource_types = site.get_resource_types()        
        
        nodes = site.nodes.get_all_nodes()
        
        self.nodes = dict([(id, ResourcePoolNode(id, "simul-%i" % id, capacity)) for (id, capacity) in nodes.items()])
        for node in self.nodes.values():
            node.enactment_info = node.id      
        
    def get_nodes(self):
        return self.nodes
    
    def get_resource_types(self):
        return self.resource_types

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
            cpu = 100 #action.vnodes[vnode].resources.get_by_type(constants.RES_CPU)
            memory = 1024 #action.vnodes[vnode].resources.get_by_type(constants.RES_MEM)
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
    def __init__(self, bandwidth):
        DeploymentEnactment.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
                
        self.bandwidth = bandwidth
        
        imgcapacity = Capacity([constants.RES_NETOUT])
        imgcapacity.set_quantity(constants.RES_NETOUT, self.bandwidth)

        # TODO: Determine node number based on site
        self.imagenode = ResourcePoolNode(1000, "image_node", imgcapacity)
        
    def get_imagenode(self):
        return self.imagenode
        
    def get_aux_nodes(self):
        return [self.imagenode] 
    
    def get_bandwidth(self):
        return self.bandwidth
        
    def resolve_to_file(self, lease_id, vnode, diskimage_id):
        return "/var/haizea/images/%s-L%iV%i" % (diskimage_id, lease_id, vnode)