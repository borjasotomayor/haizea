from haizea.resourcemanager.enact.base import StorageEnactmentBase

class StorageEnactment(StorageEnactmentBase):
    def __init__(self, resourcepool):
        StorageEnactmentBase.__init__(self, resourcepool)
        self.imagepath="/images/playground/borja"
        
    def resolveToFile(self, leaseID, vnode, diskImageID):
        return "%s/%s/%s.img" % (self.imagepath, diskImageID, diskImageID)