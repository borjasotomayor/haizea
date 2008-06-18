from haizea.resourcemanager.resourcepool import Node
from haizea.resourcemanager.enact.base import ResourcePoolInfoBase
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds

class ResourcePoolInfo(ResourcePoolInfoBase):
    def __init__(self, resourcepool):
        ResourcePoolInfoBase.__init__(self, resourcepool)
        config = self.resourcepool.rm.config
        
        numnodes = config.getNumPhysicalNodes()
        bandwidth = config.getBandwidth()        

        capacity = self.parseResourcesString(config.getResourcesPerPhysNode())
        
        self.nodes = [Node(self.resourcepool, i+1, "simul-%i" % (i+1), capacity) for i in range(numnodes)]
        
        # Image repository nodes
        imgcapacity = ds.ResourceTuple.createEmpty()
        imgcapacity.setByType(constants.RES_NETOUT, bandwidth)

        self.FIFOnode = Node(self.resourcepool, numnodes+1, "FIFOnode", imgcapacity)
        self.EDFnode = Node(self.resourcepool, numnodes+2, "EDFnode", imgcapacity)
        
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
        
    def parseResourcesString(self, resources):
        desc2type = dict([(x[2],x[0]) for x in self.getResourceTypes()])
        capacity=ds.ResourceTuple.createEmpty()
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity.setByType(desc2type[resourcename], int(resourcecapacity))
        return capacity

