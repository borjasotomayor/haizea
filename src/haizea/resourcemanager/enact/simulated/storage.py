from haizea.resourcemanager.enact.base import StorageEnactmentBase

baseCachePath="/vm/cache"
baseWorkingPath="/vm/working"
stagingPath="/vm/staging"

class StorageEnactment(StorageEnactmentBase):
    def __init__(self, resourcepool):
        StorageEnactmentBase.__init__(self, resourcepool)
        
    def resolveToFile(self, leaseID, vnode, diskImageID):
        return "%s/%s-L%iV%i" % (baseWorkingPath, diskImageID, leaseID, vnode)