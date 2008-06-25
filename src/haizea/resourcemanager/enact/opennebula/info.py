# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                       #
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
from haizea.resourcemanager.enact.base import ResourcePoolInfoBase
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from pysqlite2 import dbapi2 as sqlite

oneattr2haizea = { "TOTALCPU": constants.RES_CPU,
                   "TOTALMEMORY": constants.RES_MEM }

class ResourcePoolInfo(ResourcePoolInfoBase):
    def __init__(self, resourcepool):
        ResourcePoolInfoBase.__init__(self, resourcepool)
        config = self.resourcepool.rm.config
        self.logger = self.resourcepool.rm.logger
        self.suspendresumerate = config.getONESuspendResumeRate()

        # Get information about nodes from DB
        conn = sqlite.connect(config.getONEDB())
        conn.row_factory = sqlite.Row
        
        self.nodes = []
        cur = conn.cursor()
        cur.execute("select hid, host_name from hostpool where state != 4")
        hosts = cur.fetchall()
        for (i,host) in enumerate(hosts):
            nod_id = i+1
            enactID = int(host["hid"])
            hostname = host["host_name"]
            capacity = ds.ResourceTuple.createEmpty()
            capacity.setByType(constants.RES_DISK, 80000) # OpenNebula currently doesn't provide this
            capacity.setByType(constants.RES_NETIN, 100) # OpenNebula currently doesn't provide this
            capacity.setByType(constants.RES_NETOUT, 100) # OpenNebula currently doesn't provide this
            cur.execute("select name, value from host_attributes where id=%i" % enactID)
            attrs = cur.fetchall()
            for attr in attrs:
                name = attr["name"]
                if oneattr2haizea.has_key(name):
                    capacity.setByType(oneattr2haizea[name], int(attr["value"]))
            capacity.setByType(constants.RES_CPU, capacity.getByType(constants.RES_CPU) / 100.0)
            node = Node(self.resourcepool, nod_id, hostname, capacity)
            node.enactmentInfo = int(enactID)
            self.nodes.append(node)
            
        self.logger.info("Fetched %i nodes from ONE db" % len(self.nodes), constants.ONE)
        
        # Image repository nodes
        # TODO: No image transfers in OpenNebula yet
        self.FIFOnode = None
        self.EDFnode = None
        
    def getNodes(self):
        return self.nodes
    
    def getEDFNode(self):
        return self.EDFnode
    
    def getFIFONode(self):
        return self.FIFOnode
    
    def getResourceTypes(self):
        return [(constants.RES_CPU, constants.RESTYPE_FLOAT, "CPU"),
                (constants.RES_MEM,  constants.RESTYPE_INT, "Mem"),
                (constants.RES_DISK, constants.RESTYPE_INT, "Disk"),
                (constants.RES_NETIN, constants.RESTYPE_INT, "Net (in)"),
                (constants.RES_NETOUT, constants.RESTYPE_INT, "Net (out)")]
        
    def getSuspendResumeRate(self):
        return self.suspendresumerate
