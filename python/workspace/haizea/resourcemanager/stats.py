class Stats(object):
    def __init__(self, rm):
        self.rm = rm
        
        self.exactaccepted = []
        self.exactrejected = []
        self.besteffortcompleted = []
        self.queuesize = []
        self.queuewait = []
        self.execwait = []

        self.exactacceptedcount = 0
        self.exactrejectedcount = 0
        self.besteffortcompletedcount = 0
        self.queuesizecount = 0
        
        self.utilization = {}
        self.utilizationavg = {}

        self.queuewaittimes = {}
 
        
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
        
    def startQueueWait(self, leaseID):
        time = self.rm.time
        self.queuewaittimes[leaseID] = [time, None, None]

    def stopQueueWait(self, leaseID):
        time = self.rm.time
        self.queuewaittimes[leaseID][1] = time
        start = self.queuewaittimes[leaseID][0]
        end = self.queuewaittimes[leaseID][1]
        wait = (end - start).seconds
        self.queuewait.append((time,wait))

    def startExec(self, leaseID):
        time = self.rm.time
        self.queuewaittimes[leaseID][2] = time
        start = self.queuewaittimes[leaseID][0]
        end = self.queuewaittimes[leaseID][2]
        wait = (end - start).seconds
        self.execwait.append((time,wait))

        
    def normalizeTimes(self, data):
        return [((v[0] - self.rm.starttime).seconds,v[1]) for v in data]
        
    def addNoAverage(self, data):
        return [(v[0],v[1],None) for v in data]
    
    def addTimeWeightedAverage(self, data):
        accum=0
        prevTime = None
        startVM = None
        stats = []
        for v in data:
            time = v[0]
            value = v[1]
            if prevTime != None:
                timediff = time - prevTime
                weightedValue = prevValue*timediff
                accum += weightedValue
                average = accum/time
            else:
                avg = value
            stats.append((time, value, avg))
            prevTime = time
            prevValue = value
        
        return stats        
    
    def addAverage(self, data):
        accum=0
        count=0
        startVM = None
        stats = []
        for v in data:
            value = v[1]
            accum += value
            count += 1
            avg = accum/count
            stats.append((v[0], value, avg))
        
        return stats          
        
    def getExactAccepted(self):
        l = self.normalizeTimes(self.exactaccepted)
        return self.addNoAverage(l)
    
    def getExactRejected(self):
        l = self.normalizeTimes(self.exactrejected)
        return self.addNoAverage(l)
    
    def getBestEffortCompleted(self):
        l = self.normalizeTimes(self.besteffortcompleted)
        return self.addNoAverage(l)
    
    def getQueueSize(self):
        l = self.normalizeTimes(self.queuesize)
        return self.addTimeWeightedAverage(l)
    
    def getQueueWait(self):
        l = self.normalizeTimes(self.queuewait)
        return self.addAverage(l)

    def getExecWait(self):
        l = self.normalizeTimes(self.execwait)
        return self.addAverage(l)
    
    def getUtilization(self, slottype):
        return self.utilization[slottype]

    def getUtilizationAvg(self, slottype):
        return self.utilizationavg[slottype]
