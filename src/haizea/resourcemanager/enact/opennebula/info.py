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


        # Get information about nodes from DB
        conn = sqlite.connect(config.getONEDB())
        conn.row_factory = sqlite.Row
        
        self.nodes = []
        cur = conn.cursor()
        cur.execute("select hid, host_name from hostpool")
        hosts = cur.fetchall()
        for (i,host) in enumerate(hosts):
            nod_id = i+1
            enactID = int(host["hid"])
            hostname = host["host_name"]
            capacity = [None, None, None, None, None] # TODO: Hardcoding == bad
            capacity[constants.RES_DISK] = 20000 # OpenNebula currently doesn't provide this
            capacity[constants.RES_NETIN] = 100 # OpenNebula currently doesn't provide this
            capacity[constants.RES_NETOUT] = 100 # OpenNebula currently doesn't provide this
            cur.execute("select name, value from host_attributes where id=%i" % enactID)
            attrs = cur.fetchall()
            for attr in attrs:
                name = attr["name"]
                if oneattr2haizea.has_key(name):
                    capacity[oneattr2haizea[name]] = int(attr["value"])
            capacity = ds.ResourceTuple.fromList(capacity)
            node = Node(self.resourcepool, nod_id, hostname, capacity)
            node.enactID = enactID
            self.nodes.append(node)
            
        self.logger.info("Fetched %i nodes from ONE db" % len(self.nodes), constants.ONE)
#        capacity = [None, None, None, None, None] # TODO: Hardcoding == bad
#        
#        for r in resources:
#            resourcename = r.split(",")[0]
#            resourcecapacity = r.split(",")[1]
#            capacity[constants.str_res(resourcename)] = int(resourcecapacity)
#        capacity = ds.ResourceTuple.fromList(capacity)
#        
#        self.nodes = [Node(self.resourcepool, i+1, "simul-%i" % (i+1), capacity) for i in range(numnodes)]
        
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