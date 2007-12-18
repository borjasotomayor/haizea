from workspace.haizea.common.log import info, debug
import workspace.haizea.common.constants as constants

class BaseEnactment(object):
    def __init__(self, rm, nodes):
        self.rm = rm
        self.nodes = nodes
        self.reusealg = self.rm.config.getReuseAlgorithm()
        if self.reusealg == constants.REUSE_COWPOOL:
            self.maxDeployImg = self.rm.config.getMaxDeployImg()
        else:
            self.maxDeployImg = None
        
        self.rspnode = {}
        
    def getNode(self,nod_id):
        return self.nodes[nod_id-1]
    
    def getNodesWithCachedImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgCached(imgURI)]

    def getNodesWithImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgDeployed(imgURI)]
    
    def getNodesWithImgLater(self,imgURI,time):
        return [n.nod_id for n in self.nodes if n.isImgDeployedLater(imgURI,time)]
    
                
class BaseNode(object):
    def __init__(self, enactment, nod_id):
        self.enactment = enactment
        self.nod_id = nod_id
        self.deployedimages = []
    
    def isImgDeployed(self, imgURI):
        return imgURI in [v.imgURI for v in self.deployedimages]

    def isImgDeployedLater(self, imgURI, time):
        return imgURI in [v.imgURI for v in self.deployedimages if v.timeout >= time]
            
    def addDeployedImage(self, img):
        self.deployedimages.append(img)
        
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
        info("Node %i has %iMB %s" % (self.nod_id,self.totalDeployedImageSize(),images), constants.ENACT, None)

                
class SimulatedEnactment(BaseEnactment):
    def __init__(self, rm):
        self.rm = rm
        numnodes = self.rm.config.getNumPhysicalNodes()
        nodes = [SimulatedNode(self, i+1) for i in range(numnodes)]
        BaseEnactment.__init__(self, rm, nodes)
        
    def completedImgTransferToNode(self,nod_id,imgURI,imgSize,rsp_ids,timeout=None):
        info("Adding image for rsp_ids=%s in nod_id=%i" % (rsp_ids,nod_id), constants.ENACT, None)
        self.getNode(nod_id).printDeployedImages()
        if self.reusealg == REUSE_CACHE:
            self.getNode(nod_id).addToCache(imgURI,imgSize)

        if self.reusealg in [REUSE_NONE,REUSE_CACHE]:
            for rsp_id in rsp_ids:
                img = VMImage(imgURI, imgSize)
                img.add_rspid(rsp_id)
                self.getNode(nod_id).addDeployedImage(img)
        elif self.reusealg == REUSE_COWPOOL:
            # Sometimes we might find that the image is already deployed
            # (although unused). In that case, don't add another copy to
            # the pool. Just "reactivate" it.
            if self.getNode(nod_id).isImgDeployed(imgURI):
                for rsp_id in rsp_ids:
                    self.getNode(nod_id).addVMtoCOWImg(imgURI, rsp_id, timeout)
            else:
                img = VMImage(imgURI, imgSize)
                img.timeout = timeout
                for rsp_id in rsp_ids:
                    img.add_rspid(rsp_id)
                if self.maxDeployImg != None:
                    numDeployed = self.getNode(nod_id).getNumDeployedImg()
                    if numDeployed >= self.maxDeployImg:
                        info("Node %i has %i deployed images. Will try to bring it down to %i" % (nod_id, numDeployed, self.maxDeployImg -1), constants.ENACT, None)
                        self.getNode(nod_id).printDeployedImages()
                        self.getNode(nod_id).purgeImagesDownTo(self.maxDeployImg - 1)
                        self.getNode(nod_id).printDeployedImages()
                self.getNode(nod_id).addDeployedImage(img)

        for rsp_id in rsp_ids:
            self.rspnode[rsp_id]=nod_id
            
        self.getNode(nod_id).printDeployedImages()
        
    def addVMtoCOWImg(self,nod_id,imgURI,rsp_id,timeout):
        info("Adding additional rsp_id=%s in nod_id=%i" % (rsp_id,nod_id), constants.ENACT, None)
        self.rspnode[rsp_id]=nod_id
        self.getNode(nod_id).printDeployedImages()
        self.getNode(nod_id).addVMtoCOWImg(imgURI, rsp_id, timeout)
        self.getNode(nod_id).printDeployedImages()
    
    def isImgDeployedLater(self,nod_id,imgURI, time):
        return self.getNode(nod_id).isImgDeployedLater(imgURI, time)
    
    def removeImage(self,rsp_id):
        info("Removing images for rsp_id=%i" % rsp_id, constants.ENACT, None)
        nod_id = self.rspnode[rsp_id]
        node = self.getNode(nod_id)
        node.printDeployedImages()
        newimages = []
        for img in node.deployedimages:
            if rsp_id in img.rsp_ids:
                img.rsp_ids.remove(rsp_id)
                node.printDeployedImages()
                # Might have to keep the image if we're using a cowpool
                if self.reusealg == REUSE_COWPOOL and self.maxDeployImg==None:
                    if img.timeout >= self.server.getTime() and len(img.rsp_ids) == 0:
                        info("Removing image %s" % img.imgURI, constants.ENACT, None)
                    else:
                        newimages.append(img)
                elif self.reusealg == REUSE_NONE:
                    if len(img.rsp_ids) == 0:
                        info("Removing image %s" % img.imgURI, constants.ENACT, None)
                    else:
                        newimages.append(img)                    
                else:
                    newimages.append(img)
            else:
                newimages.append(img)

        node.deployedimages = newimages
        node.printDeployedImages()
                            
        return nod_id
        

class SimulatedNode(BaseNode):
    def __init__(self, backend, nod_id):
        BaseNode.__init__(self, backend, nod_id)


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
