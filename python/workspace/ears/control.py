from workspace.ears import srvlog

# "cache" is a list. Each item is a list with the following data:
# 1. Image URI (should be Image Identifier... for now, the URI will work)
# 2. Image Size
# 3. Number of times used

# "deployedimages" is a list. Each item is a list with the following data:
# 1. Image URI
# 2. Image size
# 3. Reservation ID (i.e. "What reservation does this image belong to?")

REUSE_NONE=0
REUSE_CACHE=1
REUSE_COWPOOL=2


class VMImage(object):
    def __init__(self, imgURI, imgSize):
        self.imgURI = imgURI
        self.imgSize = imgSize
        self.rsp_ids = set([])
        self.timeout = None
        
    def add_rspid(self, rsp_id):
        self.rsp_ids.add(rsp_id)
        
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

class BaseNode(object):
    def __init__(self, backend, nod_id):
        self.backend = backend
        self.nod_id = nod_id
        self.cache = []
        self.deployedimages = []
        
    def isImgCached(self, imgURI):
        return imgURI in [v[0] for v in self.cache]
    
    def isImgDeployed(self, imgURI):
        return imgURI in [v.imgURI for v in self.deployedimages]

    def isImgDeployedLater(self, imgURI, time):
        return imgURI in [v.imgURI for v in self.deployedimages if v.timeout >= time]
    
    def cacheSize(self):
        if len(self.cache) == 0:
            return 0
        else:
            return reduce(int.__add__, [v[1] for v in self.cache])
    
    def addToCache(self, imgURI, imgSize):
        if not self.isImgCached(imgURI):
            # Remove least used image
            while self.cacheSize() + imgSize > self.backend.maxCacheSize:
                indexLFU = None
                countLFU = None
                for i,entry in enumerate(self.cache):
                    if indexLFU == None: 
                        indexLFU = i
                        countLFU = entry[2]
                    elif entry[2] < countLFU: 
                        indexLFU = i
                        countLFU = entry[2]
                self.cache.pop(indexLFU)
            self.cache.append([imgURI, imgSize, 1])
        else:
            i = [v[0] for v in self.cache].index(imgURI)
            self.cache[i][2] += 1
            
    def addDeployedImage(self, img):
        self.deployedimages.append(img)
        #print self.nod_id
        #print rsp_id
        #print "XXXXXXXXX %i, %i" % (self.nod_id, rsp_id)
        
    def addVMtoCOWImg(self, imgURI, rsp_id, timeout):
        for img in self.deployedimages:
            if img.imgURI == imgURI:
                img.add_rspid(rsp_id)
                img.updateTimeout(timeout)
                break  # Ugh
        
    def totalDeployedImageSize(self):
        if len(self.deployedimages) == 0:
            return 0
        else:
            return sum([v.imgSize for v in self.deployedimages])
        
    def printDeployedImages(self):
        images = ""
        if len(self.deployedimages) > 0:
            images += "[ "
            for img in self.deployedimages:
                imgname=img.imgURI.split("/")[-1]
                images += imgname
                images += "("
                images += ",".join([`rsp_id` for rsp_id in img.rsp_ids])
                images += ")(%s) " % img.timeout
            images += "]"
        srvlog.info("Node %i has %iMB %s" % (self.nod_id,self.totalDeployedImageSize(),images))

class SimulationNode(BaseNode):
    def __init__(self, backend, nod_id):
        BaseNode.__init__(self, backend, nod_id)

class BaseControlBackend(object):
    def __init__(self, server, nodes, reusealg, maxCacheSize=None, maxDeployImg=None):
        self.server = server
        self.nodes = nodes
        self.reusealg = reusealg
        self.maxCacheSize = maxCacheSize
        self.maxDeployImg = maxDeployImg
        self.rspnode = {}
        
    def getNode(self,nod_id):
        return self.nodes[nod_id-1]
    
    def getNodesWithCachedImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgCached(imgURI)]

    def getNodesWithImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgDeployed(imgURI)]
    
    def getNodesWithImgLater(self,imgURI,time):
        return [n.nod_id for n in self.nodes if n.isImgDeployedLater(imgURI,time)]
    
        
    def printNodes(self):
        for node in self.nodes:
            srvlog.info("Node %i" % node.nod_id)
            srvlog.info("\tCache")
            for entry in node.cache:
                srvlog.info("\t%s" % entry)

class SimulationControlBackend(BaseControlBackend):
    def __init__(self, server, numnodes, reusealg, maxCacheSize=None, maxDeployImg=None):
        nodes = [SimulationNode(self, i+1) for i in range(numnodes)]
        BaseControlBackend.__init__(self, server, nodes, reusealg, maxCacheSize, maxDeployImg)
        
    def completedImgTransferToNode(self,nod_id,imgURI,imgSize,rsp_ids,timeout=None):
        srvlog.info("Adding image for rsp_ids=%s in nod_id=%i" % (rsp_ids,nod_id))
        self.getNode(nod_id).printDeployedImages()
        if self.reusealg == REUSE_CACHE:
            self.getNode(nod_id).addToCache(imgURI,imgSize)

        if self.reusealg in [REUSE_NONE,REUSE_CACHE]:
            for rsp_id in rsp_ids:
                img = VMImage(imgURI, imgSize)
                img.add_rspid(rsp_id)
                self.getNode(nod_id).addDeployedImage(img)
        elif self.reusealg == REUSE_COWPOOL:
            img = VMImage(imgURI, imgSize)
            img.timeout = timeout
            for rsp_id in rsp_ids:
                img.add_rspid(rsp_id)
            self.getNode(nod_id).addDeployedImage(img)

        for rsp_id in rsp_ids:
            self.rspnode[rsp_id]=nod_id
            
        self.getNode(nod_id).printDeployedImages()
        
    def addVMtoCOWImg(self,nod_id,imgURI,rsp_id,timeout):
        srvlog.info("Adding additional rsp_id=%s in nod_id=%i" % (rsp_id,nod_id))
        self.rspnode[rsp_id]=nod_id
        self.getNode(nod_id).printDeployedImages()
        self.getNode(nod_id).addVMtoCOWImg(imgURI, rsp_id, timeout)
        self.getNode(nod_id).printDeployedImages()
        

        
    def isImgCachedInNode(self,nod_id,imgURI):
        return self.getNode(nod_id).isImgCached(imgURI)
    
    def isImgDeployedLater(self,nod_id,imgURI, time):
        return self.getNode(nod_id).isImgDeployedLater(imgURI, time)
    
    
    def removeImage(self,rsp_id):
        srvlog.info("Removing images for rsp_id=%i" % rsp_id)
        nod_id = self.rspnode[rsp_id]
        node = self.getNode(nod_id)
        node.printDeployedImages()
        newimages = []
        for img in node.deployedimages:
            if rsp_id in img.rsp_ids:
                img.rsp_ids.remove(rsp_id)
                node.printDeployedImages()
                # Might have to keep the image if we're using a cowpool
                if self.reusealg == REUSE_COWPOOL:
                    if img.timeout >= self.server.getTime() and len(img.rsp_ids) == 0:
                        srvlog.info("Removing image %s" % img.imgURI)
                    else:
                        newimages.append(img)
            else:
                newimages.append(img)
                

        node.deployedimages = newimages
        node.printDeployedImages()
                            
        return nod_id
        
