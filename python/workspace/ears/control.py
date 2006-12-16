from workspace.ears import srvlog

# "cache" is a list. Each item is a list with the following data:
# 1. Image URI (should be Image Identifier... for now, the URI will work)
# 2. Image Size
# 3. Number of times used

# "deployedimages" is a list. Each item is a list with the following data:
# 1. Image URI
# 2. Image size
# 3. Reservation ID (i.e. "What reservation does this image belong to?")

class BaseNode(object):
    def __init__(self, backend, nod_id):
        self.backend = backend
        self.nod_id = nod_id
        self.cache = []
        self.deployedimages = []
        
    def isImgCached(self, imgURI):
        return imgURI in [v[0] for v in self.cache]
    
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
            
    def addDeployedImage(self, imgURI, imgSize, res_id):
        self.deployedimages.append([imgURI,imgSize,res_id])
        

class SimulationNode(BaseNode):
    def __init__(self, backend, nod_id):
        BaseNode.__init__(self, backend, nod_id)

class BaseControlBackend(object):
    def __init__(self, nodes, caching, maxCacheSize=None):
        self.nodes = nodes
        self.caching = caching
        self.maxCacheSize = maxCacheSize
        
    def getNodesWithCachedImg(self,imgURI):
        return [n.nod_id for n in self.nodes if n.isImgCached(imgURI)]
        
    def printNodes(self):
        for node in self.nodes:
            srvlog.info("Node %i" % node.nod_id)
            srvlog.info("\tCache")
            for entry in node.cache:
                srvlog.info("\t%s" % entry)

class SimulationControlBackend(BaseControlBackend):
    def __init__(self, numnodes, caching, maxCacheSize):
        nodes = [SimulationNode(self, i+1) for i in range(numnodes)]
        BaseControlBackend.__init__(self, nodes, caching, maxCacheSize)
        print self.maxCacheSize
        
    def completedImgTransferToNode(self,nod_id,imgURI,imgSize,rsp_id):
        if self.caching:
            self.nodes[nod_id-1].addToCache(imgURI,imgSize)
        self.nodes[nod_id-1].addDeployedImage(imgURI,imgSize, rsp_id)