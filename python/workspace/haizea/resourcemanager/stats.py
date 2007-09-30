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
 
        
    def addMarker(self):
        time = self.rm.time
        self.exactaccepted.append((time,self.exactacceptedcount))
        self.exactrejected.append((time,self.exactrejectedcount))
        self.besteffortcompleted.append((time,self.besteffortcompletedcount))
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
        self.queuesize.append((time,self.queuesizecount))

    def decrQueueSize(self):
        time = self.rm.time
        self.queuesizecount -= 1
        self.queuesize.append((time,self.queuesizecount))
        
    def addQueueWait(self, wait):
        time = self.rm.time
        self.queuewait.append((time,wait))
