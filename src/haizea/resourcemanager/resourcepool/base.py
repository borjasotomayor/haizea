from haizea.common.log import info, debug, edebug, warning, error
from haizea.common.utils import vnodemapstr
import haizea.common.constants as constants
from haizea.common.utils import abstract

class BaseResourcePool(object):
    def __init__(self, rm, nodes):
        self.rm = rm
        self.nodes = nodes
        self.reusealg = self.rm.config.getReuseAlg()
        if self.reusealg == constants.REUSE_COWPOOL:
            self.maxpoolsize = self.rm.config.getMaxPoolSize()
        else:
            self.maxpoolsize = None
        
    def getNodes(self):
        return self.nodes
        
    def getNode(self,nod_id):
        return self.nodes[nod_id-1]

    def getNodesWithImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgDeployed(imgURI)]
    
    def getNodesWithImgLater(self,imgURI,time):
        return [n.nod_id for n in self.nodes if n.isImgDeployedLater(imgURI,time)]
    
                
class BaseNode(object):
    def __init__(self, resourcepool, nod_id):
        self.resourcepool = resourcepool
        self.nod_id = nod_id
        self.files = []
        self.workingspacesize = 0
        # Kludgy way of keeping track of utilization
        self.transfer_doing = constants.DOING_IDLE
        self.vm_doing = constants.DOING_IDLE
        
    def getCapacity(self): abstract()
           
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
            warning("More than one tainted image for L%iV%i on node %i" % (leaseID, vnode, self.nod_id), constants.ENACT, None)
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
        info("Node %i has %iMB %s" % (self.nod_id,self.getTotalFileSize(),images), constants.ENACT, None)

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