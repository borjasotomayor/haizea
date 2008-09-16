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
from haizea.common.utils import get_config
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from pysqlite2 import dbapi2 as sqlite
import logging

class OpenNebulaResourcePoolInfo(ResourcePoolInfo):
    ONEATTR2HAIZEA = { "TOTALCPU": constants.RES_CPU,
                   "TOTALMEMORY": constants.RES_MEM }
    
    def __init__(self):
        ResourcePoolInfo.__init__(self)
        config = get_config()
        self.logger = logging.getLogger("ENACT.ONE.INFO")
        self.suspendresumerate = config.get("one.suspendresume-rate-estimate")

        # Get information about nodes from DB
        conn = sqlite.connect(config.get("one.db"))
        conn.row_factory = sqlite.Row
        
        self.nodes = []
        cur = conn.cursor()
        cur.execute("select hid, host_name from hostpool where state != 4")
        hosts = cur.fetchall()
        for (i, host) in enumerate(hosts):
            nod_id = i+1
            enactID = int(host["hid"])
            hostname = host["host_name"]
            capacity = ds.ResourceTuple.create_empty()
            capacity.set_by_type(constants.RES_DISK, 80000) # OpenNebula currently doesn't provide this
            capacity.set_by_type(constants.RES_NETIN, 100) # OpenNebula currently doesn't provide this
            capacity.set_by_type(constants.RES_NETOUT, 100) # OpenNebula currently doesn't provide this
            cur.execute("select name, value from host_attributes where id=%i" % enactID)
            attrs = cur.fetchall()
            for attr in attrs:
                name = attr["name"]
                if OpenNebulaResourcePoolInfo.ONEATTR2HAIZEA.has_key(name):
                    capacity.set_by_type(OpenNebulaResourcePoolInfo.ONEATTR2HAIZEA[name], int(attr["value"]))
            capacity.set_by_type(constants.RES_CPU, capacity.get_by_type(constants.RES_CPU) / 100.0)
            capacity.set_by_type(constants.RES_MEM, capacity.get_by_type(constants.RES_MEM) / 1024.0)
            node = Node(nod_id, hostname, capacity)
            node.enactment_info = int(enactID)
            self.nodes.append(node)
            
        self.logger.info("Fetched %i nodes from ONE db" % len(self.nodes))
        for n in self.nodes:
            self.logger.debug("%i %s %s" % (n.nod_id, n.hostname, n.capacity))
        
    def get_nodes(self):
        return self.nodes
    
    def get_resource_types(self):
        return [(constants.RES_CPU, constants.RESTYPE_FLOAT, "CPU"),
                (constants.RES_MEM,  constants.RESTYPE_INT, "Mem"),
                (constants.RES_DISK, constants.RESTYPE_INT, "Disk"),
                (constants.RES_NETIN, constants.RESTYPE_INT, "Net (in)"),
                (constants.RES_NETOUT, constants.RESTYPE_INT, "Net (out)")]
        
    def get_suspendresume_rate(self):
        return self.suspendresumerate

    def get_bandwidth(self):
        return 0
