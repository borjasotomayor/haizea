from haizea.common.utils import abstract
import haizea.resourcemanager.datastruct as ds

class ResourcePoolInfoBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
        self.logger = resourcepool.rm.logger
        
        resourcetypes = self.getResourceTypes()
        ds.ResourceTuple.setResourceTypes(resourcetypes)

        
    def getNodes(self): 
        """ Returns the nodes in the resource pool. """
        abstract()
        
    def getFIFOnode(self):
        """ Returns the image node for FIFO transfers
        
        Note that this function will disappear as soon
        as we merge the EDF and FIFO image nodes (and
        their respective algorithms)
        """
        abstract()

    def getEDFnode(self):
        """ Returns the image node for EDF transfers
        
        Note that this function will disappear as soon
        as we merge the EDF and FIFO image nodes (and
        their respective algorithms)
        """
        abstract()
        
    def getResourceTypes(self):
        abstract()
        
class StorageEnactmentBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
        self.logger = resourcepool.rm.logger
    
class VMEnactmentBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
        self.logger = resourcepool.rm.logger
        
    def start(self, vms): abstract()
    
    def stop(self, vms): abstract()
    
    def suspend(self, vms): abstract()
    
    def resume(self, vms): abstract()