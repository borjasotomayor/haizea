class Stats(object):
    def __init__(self, rm):
        self.rm = rm
        
        self.exactaccepted = []
        self.exactrejected = []
        self.besteffortcompleted = []
        self.queuesize = []
        self.queuewait = []
        self.execwait = []
        self.boundedslowdown = []
        self.utilratio = []
        self.diskusage = []

        self.besteffortstartID = []
        self.besteffortendID = []

        self.exactacceptedcount = 0
        self.exactrejectedcount = 0
        self.besteffortcompletedcount = 0
        self.queuesizecount = 0
        
        
        self.utilization = {}
        self.utilizationavg = {}

        self.queuewaittimes = {}
        self.startendtimes = {}
 
        
    def addInitialMarker(self):
        time = self.rm.time
        self.exactaccepted.append((time,None,0))
        self.exactrejected.append((time,None,0))
        self.queuesize.append((time,None,0))
        self.diskusage.append((time,None,0))

    def addFinalMarker(self):
        time = self.rm.time
        self.exactaccepted.append((time,None,self.exactacceptedcount))
        self.exactrejected.append((time,None,self.exactrejectedcount))
        self.queuesize.append((time,None,self.queuesizecount))

    def incrAccepted(self, leaseID):
        time = self.rm.time
        self.exactacceptedcount += 1
        self.exactaccepted.append((time, leaseID, self.exactacceptedcount))

    def incrRejected(self, leaseID):
        time = self.rm.time
        self.exactrejectedcount += 1
        self.exactrejected.append((time, leaseID, self.exactrejectedcount))

    def incrBestEffortCompleted(self, leaseID):
        time = self.rm.time
        self.besteffortcompletedcount += 1
        self.besteffortcompleted.append((time,leaseID,self.besteffortcompletedcount))
        
        self.startendtimes[leaseID][1] = time
        start = self.startendtimes[leaseID][0]
        end = self.startendtimes[leaseID][1]
        dur = self.rm.scheduler.completedleases.getLease(leaseID).realdur
        
        ratio = dur / (end - start)
        self.utilratio.append((time, leaseID, ratio))
        

    def incrQueueSize(self, leaseID):
        time = self.rm.time
        self.queuesizecount += 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()        
        self.queuesize.append((time,leaseID,self.queuesizecount))

    def decrQueueSize(self, leaseID):
        time = self.rm.time
        self.queuesizecount -= 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()
        self.queuesize.append((time, leaseID, self.queuesizecount))
        
    def startQueueWait(self, leaseID):
        time = self.rm.time
        if not self.queuewaittimes.has_key(leaseID):
            self.queuewaittimes[leaseID] = [time, None, None]

    def stopQueueWait(self, leaseID):
        time = self.rm.time
        if self.queuewaittimes[leaseID][1] == None:
            self.queuewaittimes[leaseID][1] = time
            start = self.queuewaittimes[leaseID][0]
            end = self.queuewaittimes[leaseID][1]
            wait = (end - start).seconds
            self.queuewait.append((time,leaseID,wait))

    def startExec(self, leaseID):
        time = self.rm.time
        if not self.startendtimes.has_key(leaseID):
            self.startendtimes[leaseID] = [time, None]
        if self.queuewaittimes[leaseID][2] == None:
            self.queuewaittimes[leaseID][2] = time
            start = self.queuewaittimes[leaseID][0]
            end = self.queuewaittimes[leaseID][2]
            wait = (end - start).seconds
            self.execwait.append((time,leaseID,wait))
            
    def addBoundedSlowdown(self, leaseID, slowdown):
        time = self.rm.time
        self.boundedslowdown.append((time,leaseID,slowdown))
        
    def addDiskUsage(self, usage):
        time = self.rm.time
        self.diskusage.append((time,None,usage))
        
    def normalizeTimes(self, data):
        return [((v[0] - self.rm.starttime).seconds,v[1],v[2]) for v in data]
        
    def addNoAverage(self, data):
        return [(v[0],v[1],v[2],None) for v in data]
    
    def addTimeWeightedAverage(self, data):
        accum=0
        prevTime = None
        startVM = None
        stats = []
        for v in data:
            time = v[0]
            leaseID = v[1]
            value = v[2]
            if prevTime != None:
                timediff = time - prevTime
                weightedValue = prevValue*timediff
                accum += weightedValue
                average = accum/time
            else:
                avg = value
            stats.append((time, leaseID, value, avg))
            prevTime = time
            prevValue = value
        
        return stats        
    
    def addAverage(self, data):
        accum=0
        count=0
        startVM = None
        stats = []
        for v in data:
            value = v[2]
            accum += value
            count += 1
            avg = accum/count
            stats.append((v[0], v[1], value, avg))
        
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
    
    def getUtilizationRatio(self):
        l = self.normalizeTimes(self.utilratio)
        return self.addAverage(l)
    
    def getDiskUsage(self):
        l = self.normalizeTimes(self.diskusage)
        l = self.addNoAverage(l)
        return l
        
    def getBoundedSlowdown(self):
        l = self.normalizeTimes(self.boundedslowdown)
        l = self.addAverage(l)
        return l
