from haizea.common.utils import vnodemapstr
import haizea.common.constants as constants
from haizea.common.utils import abstract
import haizea.resourcemanager.datastruct as ds


class ResourcePool(object):
    def __init__(self, rm):
        self.rm = rm
        
        self.loadEnactmentModules()
        
        self.nodes = self.info.getNodes()
        # TODO: The following two should be merged into
        # something like this:
        #    self.imageNode = self.info.getImageNode()
        self.FIFOnode = self.info.getFIFONode()
        self.EDFnode = self.info.getEDFNode()
        
        self.reusealg = self.rm.config.getReuseAlg()
        if self.reusealg == constants.REUSE_COWPOOL:
            self.maxpoolsize = self.rm.config.getMaxPoolSize()
        else:
            self.maxpoolsize = None
            
    def loadEnactmentModules(self):
        mode = self.rm.config.getMode()
        try:
            exec "import %s.%s as enact" % (constants.ENACT_PACKAGE, mode)
            self.info = enact.info(self)
            self.vm = enact.vm(self)
            self.storage = enact.storage(self)
        except Exception, msg:
            self.rm.logger.error("Unable to load enactment modules for mode '%s'" % mode ,constants.RM)
            raise                
        
    def getNodes(self):
        return self.nodes
        
    def getNode(self,nod_id):
        return self.nodes[nod_id-1]

    def getNodesWithImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgDeployed(imgURI)]
    
    def getNodesWithImgLater(self,imgURI,time):
        return [n.nod_id for n in self.nodes if n.isImgDeployedLater(imgURI,time)]

    def getFIFORepositoryNode(self):
        return self.FIFOnode
    
    def getEDFRepositoryNode(self):
        return self.EDFnode
        
    def addImageToNode(self,nod_id,imagefile,imagesize,vnodes,timeout=None):
        self.rm.logger.info("Adding image for leases=%s in nod_id=%i" % (vnodes,nod_id), constants.ENACT)
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
                        self.rm.logger.info("Adding the image would make the size of pool in node %i = %iMB. Will try to bring it down to %i" % (nod_id, reqsize, desiredsize), constants.ENACT)
                        self.getNode(nod_id).printFiles()
                        success = self.getNode(nod_id).purgePoolDownTo(self.maxpoolsize)
                        if not success:
                            self.rm.logger.info("Unable to add to pool. Creating tainted image instead.", constants.ENACT)
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
        self.rm.logger.info("Adding tainted image for L%iV%i in pnode=%i" % (lease,vnode,pnode), constants.ENACT)
        self.getNode(pnode).printFiles()
        img = VMImageFile(imagefile, imagesize, masterimg=False)
        img.addMapping(lease,vnode)
        self.getNode(pnode).addFile(img)
        self.getNode(pnode).printFiles()
        
    def checkImage(self, pnode, leaseID, vnode, imagefile):
        node = self.getNode(pnode)
        if self.reusealg == constants.REUSE_NONE:
            if not node.hasTaintedImage(leaseID, vnode, imagefile):
                self.rm.logger.error("ERROR: Image for L%iV%i is not deployed on node %i" % (leaseID, vnode, pnode), constants.ENACT)
        elif self.reusealg == constants.REUSE_COWPOOL:
            poolentry = node.getPoolEntry(imagefile, leaseID=leaseID, vnode=vnode)
            if poolentry == None:
                # Not necessarily an error. Maybe the pool was full, and
                # we had to fall back on creating a tainted image right
                # when the image was transferred. We have to check this.
                if not node.hasTaintedImage(leaseID, vnode, imagefile):
                    self.rm.logger.error("ERROR: Image for L%iV%i is not in pool on node %i, and there is no tainted image" % (leaseID, vnode, pnode), constants.ENACT)
            else:
                # Create tainted image
                self.rm.logger.info("Adding tainted image for L%iV%i in node %i" % (leaseID, vnode, pnode), constants.ENACT)
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
            self.rm.logger.info("Removing pooled images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT)
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

        self.rm.logger.info("Removing tainted images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT)
        node.removeTainted(lease,vnode)

        node.printFiles()
        
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
        
    def addRAMFileToNode(self, pnode, leaseID, vnode, size):
        node = self.getNode(pnode)
        self.rm.logger.info("Adding RAM file for L%iV%i in node %i" % (leaseID,vnode,pnode), constants.ENACT)
        node.printFiles()
        f = RAMImageFile("RAM_L%iV%i" % (leaseID,vnode), size, leaseID,vnode)
        node.addFile(f)        
        node.printFiles()
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())

    def removeRAMFileFromNode(self, pnode, leaseID, vnode):
        node = self.getNode(pnode)
        self.rm.logger.info("Removing RAM file for L%iV%i in node %i" % (leaseID,vnode,pnode), constants.ENACT)
        node.printFiles()
        node.removeRAMFile(leaseID, vnode)
        node.printFiles()
        self.rm.stats.addDiskUsage(self.getMaxDiskUsage())
        
    def getMaxDiskUsage(self):
        return max([n.getTotalFileSize() for n in self.nodes])
    
class Node(object):
    def __init__(self, resourcepool, nod_id, hostname, capacity):
        self.resourcepool = resourcepool
        self.logger = resourcepool.rm.logger
        self.nod_id = nod_id
        self.hostname = hostname
        self.files = []
        self.workingspacesize = 0
        self.capacity = capacity
        # Kludgy way of keeping track of utilization
        self.transfer_doing = constants.DOING_IDLE
        self.vm_doing = constants.DOING_IDLE
        
    def getCapacity(self):
        return self.capacity
           
    def addFile(self, f):
        self.files.append(f)
        if not (isinstance(f, VMImageFile) and f.masterimg==True):
            self.workingspacesize += f.filesize
        
    def removeTainted(self, lease, vnode):
        img = [f for f in self.files if isinstance(f, VMImageFile) and f.masterimg==False and (lease,vnode) in f.mappings]
        if len(img) > 0:
            img = img[0]
            self.files.remove(img)
            self.workingspacesize -= img.filesize
            
    def removeRAMFile(self, lease, vnode):
        img = [f for f in self.files if isinstance(f, RAMImageFile) and f.leaseID==lease and f.vnode==vnode]
        if len(img) > 0:
            img = img[0]
            self.files.remove(img)
            self.workingspacesize -= img.filesize
        
    def hasTaintedImage(self, leaseID, vnode, imagefile):
        images = self.getTaintedImages()
        image = [i for i in images if i.filename == imagefile and i.hasMapping(leaseID, vnode)]
        if len(image) == 0:
            return False
        elif len(image) == 1:
            return True
        elif len(image) > 1:
            self.rm.logger.warning("More than one tainted image for L%iV%i on node %i" % (leaseID, vnode, self.nod_id), constants.ENACT)
            return True
        
    def addToPool(self, imagefile, leaseID, vnode, timeout):
        for f in self.files:
            if f.filename == imagefile:
                f.addMapping(leaseID, vnode)
                f.updateTimeout(timeout)
                break  # Ugh
        self.printFiles()
            
    def getPoolEntry(self, imagefile, after = None, leaseID=None, vnode=None):
        images = self.getPoolImages()
        images = [i for i in images if i.filename == imagefile]
        if after != None:
            images = [i for i in images if i.timeout >= after]
        if leaseID != None and vnode != None:
            images = [i for i in images if i.hasMapping(leaseID, vnode)]
        if len(images)>0:
            return images[0]
        else:
            return None
        
    def isInPool(self, imagefile, after = None, leaseID=None, vnode=None):
        entry = self.getPoolEntry(imagefile, after = after, leaseID=leaseID, vnode=vnode)
        if entry == None:
            return False
        else:
            return True
        
    def getTotalFileSize(self):
        return self.workingspacesize

    def getPoolImages(self):
        return [f for f in self.files if isinstance(f, VMImageFile) and f.masterimg==True]

    def getTaintedImages(self):
        return [f for f in self.files if isinstance(f, VMImageFile) and f.masterimg==False]

    def getPoolSize(self):
        return sum([f.filesize for f in self.getPoolImages()])
    
    def purgeOldestUnusedImage(self):
        pool = self.getPoolImages()
        unused = [img for img in pool if not img.hasMappings()]
        if len(unused) == 0:
            return 0
        else:
            i = iter(unused)
            oldest = i.next()
            for img in i:
                if img.timeout < oldest.timeout:
                    oldest = img
            self.files.remove(oldest)
            return 1
    
    def purgePoolDownTo(self, target):
        done = False
        while not done:
            removed = self.purgeOldestUnusedImage()
            if removed==0:
                done = True
                success = False
            elif removed == 1:
                if self.getPoolSize() <= target:
                    done = True
                    success = True
        return success
        
    def printFiles(self):
        images = ""
        if len(self.files) > 0:
            images = ", ".join([str(img) for img in self.files])
        self.logger.info("Node %i has %iMB %s" % (self.nod_id,self.getTotalFileSize(),images), constants.ENACT)

    def getState(self):
        if self.vm_doing == constants.DOING_IDLE and self.transfer_doing == constants.DOING_TRANSFER:
            return constants.DOING_TRANSFER_NOVM
        else:
            return self.vm_doing

        
class File(object):
    def __init__(self, filename, filesize):
        self.filename = filename
        self.filesize = filesize
        
class VMImageFile(File):
    def __init__(self, filename, filesize, masterimg = False):
        File.__init__(self, filename, filesize)
        self.mappings = set([])
        self.masterimg = masterimg
        self.timeout = None
        
    def addMapping(self, leaseID, vnode):
        self.mappings.add((leaseID,vnode))
        
    def hasMapping(self, leaseID, vnode):
        return (leaseID,vnode) in self.mappings
    
    def hasMappings(self):
        return len(self.mappings) > 0
        
    def updateTimeout(self, timeout):
        if timeout > self.timeout:
            self.timeout = timeout
        
    def isExpired(self, curTime):
        if self.timeout == None:
            return False
        elif self.timeout > curTime:
            return True
        else:
            return False
        
    def __str__(self):
        if self.masterimg == True:
            master="POOL"
        else:
            master="TAINTED"
        if self.timeout == None:
            timeout = "NOTIMEOUT"
        else:
            timeout = self.timeout
        return "(DISK " + self.filename + " " + vnodemapstr(self.mappings) + " " + master + " " + str(timeout) + ")"

class RAMImageFile(File):
    def __init__(self, filename, filesize, leaseID, vnode):
        File.__init__(self, filename, filesize)
        self.leaseID = leaseID
        self.vnode = vnode
                
    def __str__(self):
        mappings = [(self.leaseID, self.vnode)]
        return "(RAM " + self.filename + " " + vnodemapstr(mappings)+ ")"
