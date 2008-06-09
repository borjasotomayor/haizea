from haizea.common.log import info, debug, edebug, warning, error
import haizea.resourcemanager.datastruct as ds
import haizea.common.constants as constants
from haizea.resourcemanager.resourcepool.base import BaseResourcePool, BaseNode, VMImageFile, RAMImageFile

class SimulatedResourcePool(BaseResourcePool):
    def __init__(self, rm):
        self.rm = rm

        numnodes = self.rm.config.getNumPhysicalNodes()
        resources = self.rm.config.getResourcesPerPhysNode()
        bandwidth = self.rm.config.getBandwidth()        

        capacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        for r in resources:
            resourcename = r.split(",")[0]
            resourcecapacity = r.split(",")[1]
            capacity[constants.str_res(resourcename)] = int(resourcecapacity)
        capacity = ds.ResourceTuple.fromList(capacity)
        
        nodes = [SimulatedNode(self, i+1, capacity) for i in range(numnodes)]
        
        # Image repository nodes
        imgcapacity = [None, None, None, None, None] # TODO: Hardcoding == bad
        imgcapacity[constants.RES_CPU]=0
        imgcapacity[constants.RES_MEM]=0
        imgcapacity[constants.RES_NETIN]=0
        imgcapacity[constants.RES_NETOUT]=bandwidth
        imgcapacity[constants.RES_DISK]=0
        imgcapacity = ds.ResourceTuple.fromList(imgcapacity)
        # FIFO node
        self.FIFOnode = SimulatedNode(self, numnodes+1, imgcapacity)
        self.EDFnode = SimulatedNode(self, numnodes+2, imgcapacity)
        
        BaseResourcePool.__init__(self, rm, nodes)

    def getFIFORepositoryNode(self):
        return self.FIFOnode
    
    def getEDFRepositoryNode(self):
        return self.EDFnode
        
    def addImageToNode(self,nod_id,imagefile,imagesize,vnodes,timeout=None):
        info("Adding image for leases=%s in nod_id=%i" % (vnodes,nod_id), constants.ENACT, None)
        self.getNode(nod_id).printFiles()

        if self.reusealg == constants.REUSE_NONE:
            for (leaseID, vnode) in vnodes:
                img = VMImageFile(imagefile, imagesize, masterimg=False)
                img.addMapping(leaseID,vnode)
                self.getNode(nod_id).addFile(img)
        elif self.reusealg == constants.REUSE_COWPOOL:
            # Sometimes we might find that the image is already deployed
            # (although unused). In that case, don't add another copy to
            # the pool. Just "reactivate" it.
            if self.getNode(nod_id).isInPool(imagefile):
                for (leaseID, vnode) in vnodes:
                    self.getNode(nod_id).addToPool(imagefile, leaseID, vnode, timeout)
            else:
                img = VMImageFile(imagefile, imagesize, masterimg=True)
                img.timeout = timeout
                for (leaseID, vnode) in vnodes:
                    img.addMapping(leaseID,vnode)
                if self.maxpoolsize != constants.POOL_UNLIMITED:
                    poolsize = self.getNode(nod_id).getPoolSize()
                    reqsize = poolsize + imagesize
                    if reqsize > self.maxpoolsize:
                        desiredsize = self.maxpoolsize - imagesize
                        info("Adding the image would make the size of pool in node %i = %iMB. Will try to bring it down to %i" % (nod_id, reqsize, desiredsize), constants.ENACT, None)
                        self.getNode(nod_id).printFiles()
                        success = self.getNode(nod_id).purgePoolDownTo(self.maxpoolsize)
                        if not success:
                            info("Unable to add to pool. Creating tainted image instead.", constants.ENACT, None)
                            # If unsuccessful, this just means we couldn't add the image
                            # to the pool. We will have to create tainted images to be used
                            # only by these leases
                            for (leaseID, vnode) in vnodes:
                                img = VMImageFile(imagefile, imagesize, masterimg=False)
                                img.addMapping(leaseID,vnode)
                                self.getNode(nod_id).addFile(img)
                        else:
                            self.getNode(nod_id).addFile(img)
                    else:
                        self.getNode(nod_id).addFile(img)
            
        self.getNode(nod_id).printFiles()
        
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
        
    def addTaintedImageToNode(self,pnode,imagefile,imagesize,lease,vnode):
        info("Adding tainted image for L%iV%i in pnode=%i" % (lease,vnode,pnode), constants.ENACT, None)
        self.getNode(pnode).printFiles()
        img = VMImageFile(imagefile, imagesize, masterimg=False)
        img.addMapping(lease,vnode)
        self.getNode(pnode).addFile(img)
        self.getNode(pnode).printFiles()
        
    def checkImage(self, pnode, leaseID, vnode, imagefile):
        node = self.getNode(pnode)
        if self.reusealg == constants.REUSE_NONE:
            if not node.hasTaintedImage(leaseID, vnode, imagefile):
                error("ERROR: Image for L%iV%i is not deployed on node %i" % (leaseID, vnode, pnode), constants.ENACT, None)
        elif self.reusealg == constants.REUSE_COWPOOL:
            poolentry = node.getPoolEntry(imagefile, leaseID=leaseID, vnode=vnode)
            if poolentry == None:
                # Not necessarily an error. Maybe the pool was full, and
                # we had to fall back on creating a tainted image right
                # when the image was transferred. We have to check this.
                if not node.hasTaintedImage(leaseID, vnode, imagefile):
                    error("ERROR: Image for L%iV%i is not in pool on node %i, and there is no tainted image" % (leaseID, vnode, pnode), constants.ENACT, None)
            else:
                # Create tainted image
                info("Adding tainted image for L%iV%i in node %i" % (leaseID, vnode, pnode), constants.ENACT, None)
                node.printFiles()
                img = VMImageFile(imagefile, poolentry.filesize, masterimg=False)
                img.addMapping(leaseID,vnode)
                node.addFile(img)
                node.printFiles()
                self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
    
    def isInPool(self,pnode,imagefile,time):
        return self.getNode(pnode).isInPool(imagefile, after=time)
    
    def getNodesWithImgInPool(self, imagefile, after = None):
        return [n.nod_id for n in self.nodes if n.isInPool(imagefile, after=after)]
    
    def addToPool(self,pnode, imagefile, leaseID, vnode, timeout):
        return self.getNode(pnode).addToPool(imagefile, leaseID, vnode, timeout)
    
    def removeImage(self,pnode,lease,vnode):
        node = self.getNode(pnode)
        node.printFiles()
        if self.reusealg == constants.REUSE_COWPOOL:
            info("Removing pooled images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT, None)
            toremove = []
            for img in node.getPoolImages():
                if (lease,vnode) in img.mappings:
                    img.mappings.remove((lease,vnode))
                node.printFiles()
                # Keep image around, even if it isn't going to be used
                # by any VMs. It might be reused later on.
                # It will be purged if space has to be made available
                # for other images
            for img in toremove:
                node.files.remove(img)
            node.printFiles()

        info("Removing tainted images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT, None)
        node.removeTainted(lease,vnode)

        node.printFiles()
        
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
        
    def addRAMFileToNode(self, pnode, leaseID, vnode, size):
        node = self.getNode(pnode)
        info("Adding RAM file for L%iV%i in node %i" % (leaseID,vnode,pnode), constants.ENACT, None)
        node.printFiles()
        f = RAMImageFile("RAM_L%iV%i" % (leaseID,vnode), size, leaseID,vnode)
        node.addFile(f)        
        node.printFiles()
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())

    def removeRAMFileFromNode(self, pnode, leaseID, vnode):
        node = self.getNode(pnode)
        info("Removing RAM file for L%iV%i in node %i" % (leaseID,vnode,pnode), constants.ENACT, None)
        node.printFiles()
        node.removeRAMFile(leaseID, vnode)
        node.printFiles()
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
        
    def getMaxDiskUsage(self):
        return max([n.getTotalFileSize() for n in self.nodes])
                                

class SimulatedNode(BaseNode):
    def __init__(self, resourcepool, nod_id, capacity):
        self.capacity = capacity
        BaseNode.__init__(self, resourcepool, nod_id)
        
    def getCapacity(self):
        return self.capacity
