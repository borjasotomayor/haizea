class Stats(object):
    def __init__(self, rm):
        self.rm = rm
        
        self.exactaccepted = []
        self.exactrejected = []
        self.besteffortcompleted = []
        self.queuesize = []
        self.queuewait = []

        self.exactacceptedcount = 0
        self.exactrejectedcount = 0
        self.besteffortcompletedcount = 0
        self.queuesizecount = 0
        
        self.utilization = {}
        self.utilizationavg = {}
 
        
    def addInitialMarker(self):
        time = self.rm.time
        self.exactaccepted.append((time,0))
        self.exactrejected.append((time,0))
        self.besteffortcompleted.append((time,0))
        self.queuesize.append((time,0))

    def addFinalMarker(self):
        time = self.rm.time
        self.exactaccepted.append((time,self.exactacceptedcount))
        self.exactrejected.append((time,self.exactrejectedcount))
        self.queuesize.append((time,self.queuesizecount))

    def incrAccepted(self):
        time = self.rm.time
        self.exactacceptedcount += 1
        self.exactaccepted.append((time,self.exactacceptedcount))

    def incrRejected(self):
        time = self.rm.time
        self.exactrejectedcount += 1
        self.exactrejected.append((time,self.exactrejectedcount))

    def incrBestEffortCompleted(self):
        time = self.rm.time
        self.besteffortcompletedcount += 1
        self.besteffortcompleted.append((time,self.besteffortcompletedcount))

    def incrQueueSize(self):
        time = self.rm.time
        self.queuesizecount += 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()        
        self.queuesize.append((time,self.queuesizecount))

    def decrQueueSize(self):
        time = self.rm.time
        self.queuesizecount -= 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()
        self.queuesize.append((time,self.queuesizecount))
        
    def addQueueWait(self, wait):
        time = self.rm.time
        self.queuewait.append((time,wait))
        
    def normalizeTimes(self, data):
        return [((v[0] - self.rm.starttime).seconds,v[1]) for v in data]
        
    def getExactAccepted(self):
        return self.normalizeTimes(self.exactaccepted)
    
    def getExactRejected(self):
        return self.normalizeTimes(self.exactrejected)
    
    def getBestEffortCompleted(self):
        return self.normalizeTimes(self.besteffortcompleted)
    
    def getQueueSize(self):
        return self.normalizeTimes(self.queuesize)
    
    def getQueueWait(self):
        return self.normalizeTimes(self.queuewait)
    
    def getUtilization(self, slottype):
        return self.utilization[slottype]

    def getUtilizationAvg(self, slottype):
        return self.utilizationavg[slottype]
