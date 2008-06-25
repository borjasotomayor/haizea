# -------------------------------------------------------------------------- #
# Copyright 2006-2008, Borja Sotomayor                                       #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

import os
import os.path
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from haizea.common.utils import pickle

class Stats(object):
    def __init__(self, rm, datadir):
        self.rm = rm
        self.datadir = datadir
    
        # Counters
        self.counters={}
        self.counterLists={}
        self.counterAvgType={}
    
        # What are the nodes doing?
        self.doing = []        
        self.nodes=dict([(i+1,[]) for i in range(self.rm.resourcepool.getNumNodes())])
 
        
    def createCounter(self, counterID, avgtype, initial=0):
        self.counters[counterID] = initial
        self.counterLists[counterID] = []
        self.counterAvgType[counterID] = avgtype

    def incrCounter(self, counterID, leaseID = None):
        time = self.rm.clock.getTime()
        self.appendStat(counterID, self.counters[counterID] + 1, leaseID, time)

    def decrCounter(self, counterID, leaseID = None):
        time = self.rm.clock.getTime()
        self.appendStat(counterID, self.counters[counterID] - 1, leaseID, time)
        
    def appendStat(self, counterID, value, leaseID = None,time = None):
        if time == None:
            time = self.rm.clock.getTime()
        if len(self.counterLists[counterID]) > 0:
            prevtime = self.counterLists[counterID][-1][0]
        else:
            prevtime = None
        self.counters[counterID] = value
        if time == prevtime:
            self.counterLists[counterID][-1][2] = value
        else:
            self.counterLists[counterID].append([time, leaseID, value])
        
    def start(self, time):
        self.starttime = time
        
        # Start the counters
        for counterID in self.counters:
            initial = self.counters[counterID]
            self.appendStat(counterID, initial, time = time)
        
        # Start the doing
        for n in self.nodes:
            self.nodes[n].append((time,constants.DOING_IDLE))

    def tick(self):
        time = self.rm.clock.getTime()
        # Update the doing
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
        
    def stop(self):
        time = self.rm.clock.getTime()

        # Stop the counters
        for counterID in self.counters:
            self.appendStat(counterID, self.counters[counterID], time=time)
        
        # Add the averages
        for counterID in self.counters:
            l = self.normalizeTimes(self.counterLists[counterID])
            avgtype = self.counterAvgType[counterID]
            if avgtype == constants.AVERAGE_NONE:
                self.counterLists[counterID] = self.addNoAverage(l)
            elif avgtype == constants.AVERAGE_NORMAL:
                self.counterLists[counterID] = self.addAverage(l)
            elif avgtype == constants.AVERAGE_TIMEWEIGHTED:
                print counterID
                print l
                self.counterLists[counterID] = self.addTimeWeightedAverage(l)
        
        # Stop the doing
        for node in self.rm.resourcepool.nodes:
            nodenum = node.nod_id
            doing = node.vm_doing
            (lasttime,lastdoing) = self.nodes[nodenum][-1]
            if time != lasttime:
                self.nodes[nodenum].append((time, doing))
            
    def normalizeTimes(self, data):
        return [((v[0] - self.starttime).seconds,v[1],v[2]) for v in data]
        
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

                
            
