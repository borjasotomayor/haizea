import os
import os.path
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from haizea.common.utils import pickle

class Stats(object):
    def __init__(self, rm):
        self.rm = rm
        
        # TODO: Datadir
        # Write data to disk
        #profile = config.getProfile()
        #dir = statsdir + "/" + utils.genDataDirName(profile, tracefile, injectfile)
    
        self.utilization = []
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
        
        self.queuewaittimes = {}
        self.startendtimes = {}
        
        self.nodes=dict([(i+1,[]) for i in range(self.rm.resourcepool.getNumNodes())])
 
    def addUtilization(self,util):
        self.utilization.append((self.rm.clock.getTime(),None,util))
        
    def addInitialMarker(self):
        time = self.rm.clock.getTime()
        self.exactaccepted.append((time,None,0))
        self.exactrejected.append((time,None,0))
        self.queuesize.append((time,None,0))
        self.diskusage.append((time,None,0))
        
        for node in self.nodes:
            self.nodes[node].append((time,constants.DOING_IDLE))

    def addFinalMarker(self):
        time = self.rm.clock.getTime()
        self.exactaccepted.append((time,None,self.exactacceptedcount))
        self.exactrejected.append((time,None,self.exactrejectedcount))
        self.queuesize.append((time,None,self.queuesizecount))
        
        for node in self.rm.resourcepool.nodes:
            nodenum = node.nod_id
            doing = node.vm_doing
            (lasttime,lastdoing) = self.nodes[nodenum][-1]
            if time != lasttime:
                self.nodes[nodenum].append((time, doing))

    def incrAccepted(self, leaseID):
        time = self.rm.clock.getTime()
        self.exactacceptedcount += 1
        self.exactaccepted.append((time, leaseID, self.exactacceptedcount))

    def incrRejected(self, leaseID):
        time = self.rm.clock.getTime()
        self.exactrejectedcount += 1
        self.exactrejected.append((time, leaseID, self.exactrejectedcount))

    def incrBestEffortCompleted(self, leaseID):
        time = self.rm.clock.getTime()
        self.besteffortcompletedcount += 1
        self.besteffortcompleted.append((time,leaseID,self.besteffortcompletedcount))
        
        self.startendtimes[leaseID][1] = time
        start = self.startendtimes[leaseID][0]
        end = self.startendtimes[leaseID][1]
        #dur = self.rm.scheduler.completedleases.getLease(leaseID).duration.
        
        #ratio = dur / (end - start)
        #self.utilratio.append((time, leaseID, ratio))
        

    def incrQueueSize(self, leaseID):
        time = self.rm.clock.getTime()
        self.queuesizecount += 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()        
        self.queuesize.append((time,leaseID,self.queuesizecount))

    def decrQueueSize(self, leaseID):
        time = self.rm.clock.getTime()
        self.queuesizecount -= 1
        if self.queuesize[-1][0] == time:
            self.queuesize.pop()
        self.queuesize.append((time, leaseID, self.queuesizecount))
        
    def startQueueWait(self, leaseID):
        time = self.rm.clock.getTime()
        if not self.queuewaittimes.has_key(leaseID):
            self.queuewaittimes[leaseID] = [time, None, None]

    def stopQueueWait(self, leaseID):
        time = self.rm.clock.getTime()
        if self.queuewaittimes[leaseID][1] == None:
            self.queuewaittimes[leaseID][1] = time
            start = self.queuewaittimes[leaseID][0]
            end = self.queuewaittimes[leaseID][1]
            wait = (end - start).seconds
            self.queuewait.append((time,leaseID,wait))

    def startExec(self, leaseID):
        time = self.rm.clock.getTime()
        if not self.startendtimes.has_key(leaseID):
            self.startendtimes[leaseID] = [time, None]
        if self.queuewaittimes[leaseID][2] == None:
            self.queuewaittimes[leaseID][2] = time
            start = self.queuewaittimes[leaseID][0]
            end = self.queuewaittimes[leaseID][2]
            wait = (end - start).seconds
            self.execwait.append((time,leaseID,wait))
            
    def addBoundedSlowdown(self, leaseID, slowdown):
        time = self.rm.clock.getTime()
        self.boundedslowdown.append((time,leaseID,slowdown))
        
    def addDiskUsage(self, usage):
        time = self.rm.clock.getTime()
        self.diskusage.append((time,None,usage))
        
    def addNodeStats(self):
        time = self.rm.clock.getTime()
        for node in self.rm.resourcepool.nodes:
            nodenum = node.nod_id
            doing = node.getState()
            (lasttime,lastdoing) = self.nodes[nodenum][-1]
            if doing == lastdoing:
                # No need to update
                pass
            else:
                if lasttime == time:
                        self.nodes[nodenum][-1] = (time,doing)
                else:
                    self.nodes[nodenum].append((time,doing))
        
    def normalizeTimes(self, data):
        return [((v[0] - self.rm.clock.getStartTime()).seconds,v[1],v[2]) for v in data]
        
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
                avg = accum/time
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
    
    def getUtilization(self):
        l = self.normalizeTimes(self.utilization)
        return self.addTimeWeightedAverage(l)
    
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
    
    def getNodesDoing(self):
        starttime = self.rm.clock.getStartTime()
        nodes=dict([(i+1,[]) for i in range(self.rm.config.getNumPhysicalNodes())])
        for n in self.nodes:
            nodes[n] = []
            prevtime = None
            prevdoing = None
            for (time,doing) in self.nodes[n]:
                if prevtime != None:
                    difftime = (time-prevtime).seconds
                    nodes[n].append((difftime,prevdoing))
                prevtime = time
                prevdoing = doing
        return nodes
    
    def dumpStatsToDisk(self, dir):
        try:
            if not os.path.exists(dir):
                os.makedirs(dir)
        except OSError, e:
            if e.errno != EEXIST:
                raise e
    
        cpuutilization = self.getUtilization()
        exactaccepted = self.getExactAccepted()
        exactrejected = self.getExactRejected()
        besteffortcompleted = self.getBestEffortCompleted()
        queuesize = self.getQueueSize()
        queuewait = self.getQueueWait()
        execwait = self.getExecWait()
        utilratio = self.getUtilizationRatio()
        diskusage = self.getDiskUsage()
        boundedslowdown = self.getBoundedSlowdown()
        leases = ds.LeaseTable(None)
        leases.entries = self.rm.scheduler.completedleases.entries
        
        # Remove some data that won't be necessary in the reporting tools
        for l in leases.entries.values():
            l.removeRRs()
            l.scheduler = None
            l.logger = None
        
        doing = self.getNodesDoing()
    
        pickle(cpuutilization, dir, constants.CPUUTILFILE)
        pickle(exactaccepted, dir, constants.ACCEPTEDFILE)
        pickle(exactrejected, dir, constants.REJECTEDFILE)
        pickle(besteffortcompleted, dir, constants.COMPLETEDFILE)
        pickle(queuesize, dir, constants.QUEUESIZEFILE)
        pickle(queuewait, dir, constants.QUEUEWAITFILE)
        pickle(execwait, dir, constants.EXECWAITFILE)
        pickle(utilratio, dir, constants.UTILRATIOFILE)
        pickle(diskusage, dir, constants.DISKUSAGEFILE)
        pickle(boundedslowdown, dir, constants.SLOWDOWNFILE)
        pickle(leases, dir, constants.LEASESFILE)
        pickle(doing, dir, constants.DOINGFILE)

                
            
