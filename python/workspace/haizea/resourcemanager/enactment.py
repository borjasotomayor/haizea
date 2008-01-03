from workspace.haizea.common.log import info, debug, edebug
from workspace.haizea.common.utils import vnodemapstr
import workspace.haizea.common.constants as constants

class BaseEnactment(object):
    def __init__(self, rm, nodes):
        self.rm = rm
        self.nodes = nodes
        self.reusealg = self.rm.config.getReuseAlg()
        if self.reusealg == constants.REUSE_COWPOOL:
            self.maxpoolsize = self.rm.config.getMaxPoolSize()
        else:
            self.maxpoolsize = None
        
    def getNode(self,nod_id):
        return self.nodes[nod_id-1]

    def getNodesWithImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgDeployed(imgURI)]
    
    def getNodesWithImgLater(self,imgURI,time):
        return [n.nod_id for n in self.nodes if n.isImgDeployedLater(imgURI,time)]
    
                
class BaseNode(object):
    def __init__(self, enactment, nod_id):
        self.enactment = enactment
        self.nod_id = nod_id
        self.files = []
    
    def isImgDeployed(self, imagefile):
        return imagefile in [f.filename for f in self.files]

    def isImgDeployedLater(self, imagefile, time):
        return imagefile in [f.filename for f in self.files if f.timeout >= time]
            
    def addFile(self, f):
        self.files.append(f)
        
    def removeTainted(self, lease, vnode):
        img = [f for f in self.files if isinstance(f, VMImageFile) and f.masterimg==False and (lease,vnode) in f.mappings]
        img = img[0]
        self.files.remove(img)
        
        
    def addToPool(self, imagefile, leaseID, vnode, timeout):
        for f in self.files:
            if f.filename == imagefile:
                f.addMapping(leaseID, vnode)
                f.updateTimeout(timeout)
                break  # Ugh
        
    def getTotalFileSize(self):
        if len(self.files) == 0:
            return 0
        else:
            return sum([v.filesize for v in self.files])

    def getPoolImages(self):
        return [f for f in self.files if isinstance(f, VMImageFile) and f.masterimg==True]

        
    def getPoolSize(self):
        return sum([f.filesize for f in self.files if isinstance(f, VMImageFile) and f.masterimg==True])

    def getNumDeployedImg(self):
        return len(self.deployedimages)
    
    def purgeOldestUnusedImage(self):
        unused = [img for img in self.deployedimages if len(img.rsp_ids)==0]
        if len(unused) == 0:
            return 0
        else:
            i = iter(unused)
            oldest = i.next()
            for img in i:
                if img.timeout < oldest.timeout:
                    oldest = img
            self.deployedimages.remove(oldest)
            return 1
    
    def purgeImagesDownTo(self, target):
        done = False
        while not done:
            removed = self.purgeOldestUnusedImage()
            if removed==0:
                done = True
            elif removed == 1:
                if len(self.deployedimages) == target:
                    done = True
        
    def printFiles(self):
        images = ""
        if len(self.files) > 0:
            images = ", ".join([str(img) for img in self.files])
        info("Node %i has %iMB %s" % (self.nod_id,self.getTotalFileSize(),images), constants.ENACT, None)

                
class SimulatedEnactment(BaseEnactment):
    def __init__(self, rm):
        self.rm = rm
        numnodes = self.rm.config.getNumPhysicalNodes()
        nodes = [SimulatedNode(self, i+1) for i in range(numnodes)]
        BaseEnactment.__init__(self, rm, nodes)
        
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
            if self.getNode(nod_id).isImgDeployed(imagefile):
                for (leaseID, vnode) in vnodes:
                    self.getNode(nod_id).addToPool(imagefile, leaseID, vnode, timeout)
            else:
                img = VMImageFile(imagefile, imagesize, masterimg=True)
                img.timeout = timeout
                for (leaseID, vnode) in vnodes:
                    img.addMapping(leaseID,vnode)
                if self.maxpoolsize != constants.POOL_UNLIMITED:
                    poolsize = self.getNode(nod_id).getPoolSize()
                    if poolsize >= self.maxpoolsize:
                        info("Node %i has %i deployed images. Will try to bring it down to %i" % (nod_id, numDeployed, self.maxDeployImg -1), constants.ENACT, None)
                        self.getNode(nod_id).printDeployedImages()
                        self.getNode(nod_id).purgeImagesDownTo(self.maxDeployImg - 1)
                        self.getNode(nod_id).printDeployedImages()
                self.getNode(nod_id).addFile(img)
            
        self.getNode(nod_id).printFiles()
        
    def addVMtoCOWImg(self,nod_id,imgURI,rsp_id,timeout):
        info("Adding additional rsp_id=%s in nod_id=%i" % (rsp_id,nod_id), constants.ENACT, None)
        self.rspnode[rsp_id]=nod_id
        self.getNode(nod_id).printDeployedImages()
        self.getNode(nod_id).addVMtoCOWImg(imgURI, rsp_id, timeout)
        self.getNode(nod_id).printDeployedImages()
    
    def isImgDeployedLater(self,nod_id,imgURI, time):
        return self.getNode(nod_id).isImgDeployedLater(imgURI, time)
    
    def removeImage(self,pnode,lease,vnode):
        node = self.getNode(pnode)
        node.printFiles()
        if self.reusealg == constants.REUSE_COWPOOL:
            info("Removing pooled images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT, None)
            toremove = []
            print node.getPoolImages()
            for img in node.getPoolImages():
                if (lease,vnode) in img.mappings:
                    img.mappings.remove((lease,vnode))
                node.printFiles()
                if img.timeout >= self.rm.time and len(img.mappings) == 0:
                    info("Removing image %s" % img.filename, constants.ENACT, None)
                    toremove.append(img)
            for img in toremove:
                node.files.remove(img)
            node.printFiles()

        info("Removing tainted images for L%iV%i in node %i" % (lease,vnode,pnode), constants.ENACT, None)
        node.removeTainted(lease,vnode)

        node.printFiles()
        
        # TODO: Remove tainted image
                                

class SimulatedNode(BaseNode):
    def __init__(self, backend, nod_id):
        BaseNode.__init__(self, backend, nod_id)


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

class RAMImageFile(object):
    def __init__(self, imagefile, imagesize, leaseID, vnode):
        File.__init__(self, filename, filesize)
        self.leaseID = leaseID
        self.vnode = vnode
                
    def __str__(self):
        mappings = [(self.leaseID, self.vnode)]
        return "(RAM " + self.imagefile + " " + vnodemapstr(mappings)+ ")"
