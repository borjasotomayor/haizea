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

from haizea.resourcemanager.resourcepool import Node
from haizea.resourcemanager.enact import ResourcePoolInfo
import haizea.common.constants as constants
from haizea.common.utils import get_config
import haizea.resourcemanager.datastruct as ds
import logging

class SimulatedResourcePoolInfo(ResourcePoolInfo):
    def __init__(self):
        ResourcePoolInfo.__init__(self)
        self.logger = logging.getLogger("ENACT.SIMUL.INFO")
        config = get_config()
        self.suspendresumerate = config.get("simul.suspendresume-rate")
                
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
        capacity=ds.ResourceTuple.create_empty()
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity.set_by_type(desc2type[resourcename], int(resourcecapacity))
        return capacity

    def get_suspendresume_rate(self):
        return self.suspendresumerate
    
    def get_migration_bandwidth(self):
        return 100 # TODO: Get from config file
