from haizea.resourcemanager.resourcepool import Node
from haizea.resourcemanager.enact.base import ResourcePoolInfoBase
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds

class ResourcePoolInfo(ResourcePoolInfoBase):
    def __init__(self, resourcepool):
        ResourcePoolInfoBase.__init__(self, resourcepool)
        config = self.resourcepool.rm.config
        
        numnodes = config.getNumPhysicalNodes()
        resources = config.getResourcesPerPhysNode()
        bandwidth = config.getBandwidth()        

        capacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity[constants.str_res(resourcename)] = int(resourcecapacity)
        capacity = ds.ResourceTuple.fromList(capacity)
        
        self.nodes = [Node(self.resourcepool, i+1, "simul-%i" % (i+1), capacity) for i in range(numnodes)]
        
        # Image repository nodes
        imgcapacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        imgcapacity[constants.RES_CPU]=0
        imgcapacity[constants.RES_MEM]=0
        imgcapacity[constants.RES_NETIN]=0
        imgcapacity[constants.RES_NETOUT]=bandwidth
        imgcapacity[constants.RES_DISK]=0
        imgcapacity = ds.ResourceTuple.fromList(imgcapacity)

        self.FIFOnode = Node(self.resourcepool, numnodes+1, "FIFOnode", imgcapacity)
        self.EDFnode = Node(self.resourcepool, numnodes+2, "EDFnode", imgcapacity)
        
    def getNodes(self):
        return self.nodes
    
    def getEDFNode(self):
        return self.EDFnode
    
    def getFIFONode(self):
        return self.FIFOnode