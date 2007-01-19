import ConfigParser, os
from workspace.util import stats
from workspace.traces.files import TraceFile, TraceEntry, TraceEntryV2
from mx.DateTime import *
import random
import sys

GENERAL_SEC = "general"
INTERVAL_SEC = "interval"
NUMNODES_SEC = "numnodes"
NUMNODESAR_SEC = "numnodesAR"
DURATION_SEC = "duration"
DURATIONAR_SEC = "durationAR"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"
SIMULTANEOUS_SEC = "simultaneousrequests"
ADMISSIONCONTROL_SEC = "admissioncontrol"

BANDWIDTH_OPT = "bandwidth"
DURATION_OPT = "duration"
AR_OPT = "ar"
BATCH_OPT = "batch"
TYPE_OPT = "type"
SIZES_OPT = "imagesizes"
ROUND_OPT = "round"

DISTRIBUTION_OPT = "distribution"
MIN_OPT = "min"
MAX_OPT = "max"
ITEMS_OPT = "items"
ITEMSPROBS_OPT = "itemswithprobs"
MEAN_OPT = "mean"
STDEV_OPT = "stdev"

MAXDURATION_OPT = "maxduration"
MAXNODES_OPT = "maxnodes"
TRUNCATEDURATION_OPT = "truncateduration"
QUEUE_OPT = "queue"
PARTITION_OPT = "partition"

NUMC_OPT = "numclusters"
WINSIZE_OPT = "windowsize"

        
class Cooker(object):
    def __init__(self, conffile):
        self.conf = TraceConf.fromFile(conffile)
    
    def generateTrace(self):
        entries = []
        maxtime = self.conf.traceDuration
        time = 0
        totalDurationBatch = 0
        totalDurationAR = 0
        while int(time) < int(maxtime):
            entrytime = self.conf.intervalDist.get()
            if self.conf.round != None:
                entrytime = ((entrytime / self.conf.round) + 1) * self.conf.round             
            time += entrytime
            if self.conf.simulDist == None:
                numRequests = 1
            else:
                numRequests = self.conf.simulDist.get()

            for i in xrange(0,numRequests):
                fields = {}
                fields["time"] = str(time)
                img = self.conf.imageDist.get()
                fields["uri"] = img
                imgsize = self.conf.imageSizes[img]
                fields["size"] = str(imgsize)
                # TODO: Make memory and CPU configurable
                fields["memory"] = str(512)
                fields["cpu"] = str(50)
                fields["mode"] = "RW"
                type = self.conf.arbatchDist.get()
                duration=None
                if type == "AR":
                    numNodes = self.conf.numNodesARDist.get()
                    fields["numNodes"] = str(numNodes)
                    tightness = self.conf.deadlineDist.get() / 100
                    imgsizeKB = imgsize * 1024 * numNodes
                    transferTime = imgsizeKB / self.conf.bandwidth
                    deadline = int(transferTime * (1 + tightness))
                    if self.conf.round != None:
                        deadline = ((deadline / self.conf.round) + 1) * self.conf.round 
                    if time + deadline > int(maxtime):
                        continue # We don't want AR's beyond the maximum trace duration
                    fields["deadline"] = str(deadline)
                    fields["tag"] = "AR"
                    duration = int(self.conf.durationARDist.get())
                    if self.conf.round != None and duration % self.conf.round != 0:
                        duration = ((duration / self.conf.round) + 1) * self.conf.round
                    totalDurationAR += duration*numNodes
                elif type == "BATCH":
                    fields["deadline"] = "NULL"
                    fields["tag"] = "BATCH"
                    numNodes = self.conf.numNodesDist.get()
                    fields["numNodes"] = str(numNodes)
                    duration = int(self.conf.durationDist.get())
                    if self.conf.round != None and duration % self.conf.round != 0:
                        duration = ((duration / self.conf.round) + 1) * self.conf.round
                    totalDurationBatch += duration*numNodes
                
                fields["duration"] = str(duration)
                entries.append(TraceEntryV2(fields))
        return (TraceFile(entries), totalDurationAR, totalDurationBatch)


class OfflineScheduleEntry(object):
    def __init__(self, traceentry, transferTime, absdeadline):
        self.traceentry = traceentry
        self.transferTime = transferTime
        self.absdeadline = absdeadline
        
    def compare(a, b):
        return a.absdeadline - b.absdeadline
     
class OfflineAdmissionControl(object):
    def __init__(self, trace, bandwidth, mode, numNodesDist, numClusters, windowSize):
        self.trace = trace
        self.bandwidth = bandwidth
        self.numNodesDist = numNodesDist
        self.mode = mode
        self.numClusters = numClusters
        self.windowSize = windowSize

    def filterInfeasible(self):
        dlentries = []
        for entry in self.trace.entries:
            time = int(entry.fields["time"])
            deadline = int (entry.fields["deadline"])
            absdeadline = time + deadline
            numNodes = int(entry.fields["numNodes"])
            imgsize = int(entry.fields["size"])
            imgsizeKB = imgsize * 1024 * numNodes
            transferTime = (float(imgsizeKB) / self.bandwidth) +(10*numNodes)
            dlentry = OfflineScheduleEntry(entry, transferTime, absdeadline)
            dlentries.append(dlentry)

        dlentries.sort(OfflineScheduleEntry.compare)
        
        nextstarttime = 0
        accepted = []
        rejected = []
        if self.mode == "clustered_dl":
            clusterinterval = dlentries[-1].absdeadline / self.numClusters
            curwindow = [clusterinterval, clusterinterval + self.windowSize]
            for entry in dlentries:
                if entry.absdeadline > curwindow[0] and entry.absdeadline < curwindow[1]:
                    transferendtime = nextstarttime + entry.transferTime
                    if transferendtime < entry.absdeadline:
                        accepted.append(entry.traceentry)
                        nextstarttime = nextstarttime + entry.transferTime
                    else:
                        rejected.append(entry.traceentry)
                if entry.absdeadline > curwindow[1]:
                    curwindow[0] += clusterinterval
                    curwindow[1] += clusterinterval
        elif self.mode == "nodes":
            desiredNumNodes = self.numNodesDist.get()
            for entry in dlentries:
                if int(entry.traceentry.fields["numNodes"]) == desiredNumNodes:
                    transferendtime = nextstarttime + entry.transferTime
                    if transferendtime < entry.absdeadline:
                        accepted.append(entry.traceentry)
                        nextstarttime = nextstarttime + entry.transferTime
                        desiredNumNodes = self.numNodesDist.get()
                    else:
                        rejected.append(entry.traceentry)

            
        accepted.sort(TraceEntry.compare)
        rejected.sort(TraceEntry.compare)
            
        return (TraceFile(accepted), TraceFile(rejected))        
    
        
class ConfFile(object):
    
    def __init__(self):
        pass
        
    @staticmethod
    def createDiscreteDistributionFromSection(config, section):
        distType = config.get(section, DISTRIBUTION_OPT)
        probs = None
        if config.has_option(section, MIN_OPT) and config.has_option(section, MAX_OPT):
            min = config.getint(section, MIN_OPT)
            max = config.getint(section, MAX_OPT)
            values = range(min,max+1)
        elif config.has_option(section, ITEMS_OPT):
            filename = config.get(section, ITEMS_OPT)
            file = open (filename, "r")
            values = []
            for line in file:
                value = line.strip().split(";")[0]
                values.append(value)
        elif config.has_option(section, ITEMSPROBS_OPT):
	    itemsprobsOpt = config.get(section, ITEMSPROBS_OPT).split(",")
            itemsFile = open(itemsprobsOpt[0], "r")
            probsField = int(itemsprobsOpt[1])
            values = []
            probs = []
            for line in itemsFile:
                fields = line.split(";")
                itemname = fields[0]
                itemprob = float(fields[probsField])/100
                values.append(itemname)
                probs.append(itemprob)
        dist = None
        if distType == "uniform":
            dist = stats.DiscreteUniformDistribution(values)
        elif distType == "explicit":
            if probs == None:
                raise Exception, "No probabilities specified"
            dist = stats.DiscreteDistribution(values, probs) 
            
        return dist
        

    @staticmethod
    def createContinuousDistributionFromSection(config, section):
        distType = config.get(section, DISTRIBUTION_OPT)
        min = config.getfloat(section, MIN_OPT)
        max = config.get(section, MAX_OPT)
        if max == "unbounded":
            max = float("inf")
        if distType == "uniform":
            dist = stats.ContinuousUniformDistribution(min, max)
        elif distType == "normal":
            mu = config.getfloat(section, MEAN_OPT)
            sigma = config.getfloat(section, STDEV_OPT)
            dist = stats.ContinuousNormalDistribution(min,max,mu,sigma)
        elif distType == "pareto":
            pass 
        
        return dist
        
        
class TraceConf(ConfFile):
    
    def __init__(self, _imageDist, _numNodesDist, _deadlineDist,
                   _durationDist, _imageSizes, _bandwidth, _intervalDist, _simulDist,
                   _duration, _arbatchDist, _type, _admissioncontrol, _numc, _winsize,
                   _round, _durationARDist, _numNodesARDist, _config):
        self.imageDist = _imageDist
        self.imageSizes = _imageSizes
        self.numNodesDist = _numNodesDist
        self.numNodesARDist = _numNodesARDist
        self.deadlineDist = _deadlineDist
        self.durationDist = _durationDist
        self.durationARDist = _durationARDist
        self.intervalDist = _intervalDist
        self.traceDuration = _duration
        self.bandwidth = _bandwidth
        self.arbatchDist = _arbatchDist
        self.simulDist = _simulDist
        self.type = _type
        self.admissioncontrol = _admissioncontrol
        self.numc = _numc
        self.winsize = _winsize
        self.round = _round
        self.config = _config
    
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        if config.has_option(GENERAL_SEC, BANDWIDTH_OPT):
            bandwidth = config.getint(GENERAL_SEC, BANDWIDTH_OPT)
        else:
            bandwidth = None
        if config.has_option(GENERAL_SEC, ROUND_OPT):
            round = config.getint(GENERAL_SEC, ROUND_OPT)
        else:
            round = None
        duration = config.getint(GENERAL_SEC, DURATION_OPT)
        arPercent = config.get(GENERAL_SEC, AR_OPT)
        batchPercent = config.get(GENERAL_SEC, BATCH_OPT)
        
        arProb = float(arPercent)/100
        batchProb = float(batchPercent)/100

        arbatchDist = stats.DiscreteDistribution(["AR","BATCH"], [arProb,batchProb])
        
        if config.has_option(GENERAL_SEC, TYPE_OPT):
            type = config.get(GENERAL_SEC, TYPE_OPT)
        else:
            type = None

        if config.has_section(ADMISSIONCONTROL_SEC):
            admissioncontrol = config.get(ADMISSIONCONTROL_SEC, TYPE_OPT)
            if admissioncontrol == "clustered_dl":
                numc = config.getint(ADMISSIONCONTROL_SEC, NUMC_OPT)
                winsize = config.getint(ADMISSIONCONTROL_SEC, WINSIZE_OPT)
        else:
            admissioncontrol = None
            numc = None
            winsize = None


        numNodesDist = cls.createDiscreteDistributionFromSection(config, NUMNODES_SEC)
        if config.has_section(NUMNODESAR_SEC):
            numNodesARDist = cls.createDiscreteDistributionFromSection(config, NUMNODESAR_SEC)
        else:
            numNodesARDist = numNodesDist
        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)
        intervalDist = cls.createDiscreteDistributionFromSection(config, INTERVAL_SEC)
        durationDist = cls.createContinuousDistributionFromSection(config, DURATION_SEC)
        if config.has_section(DURATIONAR_SEC):
            durationARDist = cls.createDiscreteDistributionFromSection(config, DURATIONAR_SEC)
        else:
            durationARDist = durationDist

        if config.has_section(DEADLINE_SEC):
            deadlineDist = cls.createContinuousDistributionFromSection(config, DEADLINE_SEC)
        else:
            deadlineDist = None
        
        if config.has_section(SIMULTANEOUS_SEC):
            simulDist = cls.createDiscreteDistributionFromSection(config, SIMULTANEOUS_SEC)
        else:
            simulDist = None

        # Get image sizes
        imageSizesOpt = config.get(IMAGES_SEC, SIZES_OPT).split(",")
        imageSizesFile = open(imageSizesOpt[0], "r")
        imageSizesField = int(imageSizesOpt[1])
        imageSizes = {}
        for line in imageSizesFile:
            fields = line.split(";")
            imgname = fields[0]
            imgsize = fields[imageSizesField]
            imageSizes[imgname] = int(imgsize)

        return cls(_imageDist=imagesDist, _numNodesDist=numNodesDist, _deadlineDist=deadlineDist, 
                   _durationDist = durationDist, _imageSizes = imageSizes, _bandwidth = bandwidth, _intervalDist= intervalDist,
                   _duration = duration, _arbatchDist = arbatchDist, _type = type, _simulDist = simulDist,
                   _admissioncontrol = admissioncontrol, _numc = numc, _winsize = winsize, _round = round,
                   _numNodesARDist=numNodesARDist, _durationARDist=durationARDist, _config=config)        


class InjectorConf(ConfFile):
    
    def __init__(self, _imageDist, _intervalDist, _numNodesDist, _deadlineDist,
                   _durationDist, _imageSizes, _bandwidth):
        self.imageDist = _imageDist
        self.imageSizes = _imageSizes
        self.numNodesDist = _numNodesDist
        self.deadlineDist = _deadlineDist
        self.durationDist = _durationDist
        self.intervalDist = _intervalDist
        self.bandwidth = _bandwidth
            
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        bandwidth = config.getint(GENERAL_SEC, BANDWIDTH_OPT)

        numNodesDist = cls.createDiscreteDistributionFromSection(config, NUMNODES_SEC)
        intervalDist = cls.createDiscreteDistributionFromSection(config, INTERVAL_SEC)
        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)
        deadlineDist = cls.createContinuousDistributionFromSection(config, DEADLINE_SEC)
        durationDist = cls.createContinuousDistributionFromSection(config, DURATION_SEC)
        
        # Get image sizes
        imageSizesOpt = config.get(IMAGES_SEC, SIZES_OPT).split(",")
        imageSizesFile = open(imageSizesOpt[0], "r")
        imageSizesField = int(imageSizesOpt[1])
        imageSizes = {}
        for line in imageSizesFile:
            fields = line.split(";")
            imgname = fields[0]
            imgsize = fields[imageSizesField]
            imageSizes[imgname] = int(imgsize)

        return cls(_imageDist=imagesDist, _numNodesDist=numNodesDist, _deadlineDist=deadlineDist,
                   _durationDist = durationDist, _imageSizes = imageSizes, _bandwidth = bandwidth,
                   _intervalDist = intervalDist) 
 
class SWF2TraceConf(ConfFile):
    
    def __init__(self, _imageDist, _maxNodes, _maxDuration, _truncateDuration,
                   _imageSizes, _range, _partition, _queue):
        self.imageDist = _imageDist
        self.maxNodes = _maxNodes
        self.maxDuration = _maxDuration
        self.truncateDuration = _truncateDuration
        self.imageSizes = _imageSizes
        self.range = _range
        self.partition = _partition
        self.queue = _queue
    
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        maxduration = config.getint(GENERAL_SEC, MAXDURATION_OPT)
        maxnodes = config.getint(GENERAL_SEC, MAXNODES_OPT)
        truncateduration = config.getboolean(GENERAL_SEC, TRUNCATEDURATION_OPT)
        if config.has_option(GENERAL_SEC, QUEUE_OPT):
            queue = config.getint(GENERAL_SEC, QUEUE_OPT)
        else:
            queue = None
        if config.has_option(GENERAL_SEC, PARTITION_OPT):
            partition = config.getint(GENERAL_SEC, PARTITION_OPT)
        else:
            partition = None
            
        if config.has_option(GENERAL_SEC, MIN_OPT) and config.has_option(GENERAL_SEC, MAX_OPT):
            min = config.getint(GENERAL_SEC, MIN_OPT)
            max = config.getint(GENERAL_SEC, MAX_OPT)
            r = (min,max)
        else:
            r = None

        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)

        # Get image sizes
        imageSizesOpt = config.get(IMAGES_SEC, SIZES_OPT).split(",")
        imageSizesFile = open(imageSizesOpt[0], "r")
        imageSizesField = int(imageSizesOpt[1])
        imageSizes = {}
        for line in imageSizesFile:
            fields = line.split(";")
            imgname = fields[0]
            imgsize = fields[imageSizesField]
            imageSizes[imgname] = int(imgsize)

        return cls(_imageDist=imagesDist, _maxNodes=maxnodes, _maxDuration=maxduration, _truncateDuration=truncateduration,
                   _imageSizes=imageSizes, _partition=partition, _queue=queue, _range=r)        
    

# TODO: Merge into TraceFile
class ARInjector(object):
    def __init__(self, trace, conffile):
        self.trace = trace
        self.conf = InjectorConf.fromFile(conffile)
        
    def injectIntoTrace(self):
        artrace = TraceFile()
        maxtime = self.trace.entries[-1].fields["time"]
        time = 0
        while int(time) < int(maxtime):
            entrytime = self.conf.intervalDist.get()
            time += entrytime
            fields = {}
            fields["time"] = str(time)
            img = self.conf.imageDist.get()
            fields["uri"] = img
            imgsize = self.conf.imageSizes[img]
            fields["size"] = str(imgsize)
            fields["numNodes"] = str(self.conf.numNodesDist.get())
            fields["mode"] = "RW"
            tightness = self.conf.deadlineDist.get() / 100
            imgsizeKB = imgsize * 1024
            transferTime = imgsizeKB / self.conf.bandwidth
            deadline = int(transferTime * (1 + tightness))
            fields["deadline"] = str(deadline)
            fields["duration"] = str(int(self.conf.durationDist.get()))
            fields["tag"] = "AR"
            artrace.entries.append(TraceEntry(fields))
        
        # Add BATCH tag
        for entry in self.trace.entries:
            entry.fields["tag"] = "BATCH"
            
        newTraceEntries = self.trace.entries + artrace.entries    
        newTraceEntries.sort(TraceEntry.compare)
        return TraceFile(newTraceEntries)
    
    def run(self, *argv):
        print "Does nothing"
     
     
     
# Merge into TraceFile     
class Thermometer(object):
    def __init__(self, trace, nodes = None):
        self.trace = trace
        self.nodes = nodes

    def ratio(self, a, b):
        if b == 0:
            return float("inf")
        else:
            return float(a)/float(b)

    def printStats(self):
        numBatch = 0
        numAR = 0
        numBatchNodes = 0
        numARNodes = 0
        totalSubmit = len(self.trace.entries)
        totalNodes = 0
        totalMB = 0
        totalDurationBatch = 0
        totalDurationAR = 0
        totalDurationARSerial = 0
        traceDuration = int(self.trace.entries[-1].fields["time"])
        images = {}        
        imagesNodes = {}
 
        for entry in self.trace.entries:
            numNodes = int(entry.fields["numNodes"])
            duration = int(entry.fields["duration"])
            serialDuration= duration * numNodes
            img = entry.fields["uri"]
            if images.has_key(img):
                images[img] += 1
                imagesNodes[img] += numNodes
            else:
                images[img] = 1
                imagesNodes[img] = numNodes
            
            if entry.fields["tag"] == "AR":
                numAR += 1
                numARNodes += numNodes
                totalDurationAR += duration
                totalDurationARSerial += serialDuration
            elif entry.fields["tag"] == "BATCH":
                numBatch += 1
                numBatchNodes += numNodes
                totalDurationBatch += serialDuration
            totalNodes += numNodes
            totalMB += int(entry.fields["size"]) * numNodes 

        submissionRatioA2B = self.ratio(numAR, numBatch)
        submissionRatioB2A = self.ratio(numBatch, numAR)
        nodesRatioA2B = self.ratio(numARNodes, numBatchNodes)
        nodesRatioB2A = self.ratio(numBatchNodes, numARNodes)

        batchPercent = float(numBatch) / totalSubmit
        ARPercent = float(numAR) / totalSubmit
        batchNodePercent = float(numBatchNodes) / totalNodes
        ARNodePercent = float(numARNodes) / totalNodes
        
        avgBatchDuration = float(totalDurationBatch) / numBatchNodes
        avgARDuration = float(totalDurationAR) / numAR
        avgARNodeSize = float(numARNodes) / numAR
        
        print "# of Batch and AR (measured in SUBMISSIONS)"
        print "-------------------------------------------"
        print "  Batch =",numBatch
        print "     AR =", numAR
        print "% Batch =",batchPercent
        print "   % AR =", ARPercent
        print "Ratio (AR-to-Batch) =", submissionRatioA2B
        print "Ratio (Batch-to-AR) =", submissionRatioB2A
        print ""
        print "# of Batch and AR (measured in VIRTUAL NODES)"
        print "---------------------------------------------"
        print "Batch =",numBatchNodes
        print "   AR =", numARNodes
        print "Ratio (AR-to-Batch) =", nodesRatioA2B 
        print "Ratio (Batch-to-AR) =", nodesRatioB2A
        print "% Batch =",batchNodePercent
        print "   % AR =", ARNodePercent
        print ""
        print "# of Batch and AR (measured in DURATION)"
        print "----------------------------------------"
        print "Batch =", totalDurationBatch
        print "   AR =", totalDurationARSerial
        print "% Batch =", float(totalDurationBatch) / (totalDurationBatch+totalDurationARSerial)
        print "   % AR =", float(totalDurationARSerial) / (totalDurationBatch+totalDurationARSerial)
        print ""
        print "AR"
        print "--"
        print "Average # of virtual nodes in an AR request: %.2f" % avgARNodeSize
        print ""
        print "DURATION STATS"
        print "--------------"
        print "Average duration of a batch request: %.2f" % avgBatchDuration
        print "Average duration of an AR request: %.2f" % avgARDuration
        print "Sum of duration of batch requests (1): %s" % TimeDelta(seconds=totalDurationBatch)
        if self.nodes != None:
            print "Time to process (1) assuming %i virtual nodes: %s" % (self.nodes, TimeDelta(seconds=totalDurationBatch) / self.nodes)
            print "Time to process entire workload assuming %i virtual nodes: %s" % (self.nodes, TimeDelta(seconds=totalDurationBatch+totalDurationARSerial) / self.nodes)
        print "Approximate total duration required by ARs: %s" % TimeDelta(seconds=numAR*avgARDuration)
        print ""
        print "IMAGES"
        print "------"
        print "Total MB =",totalMB

        bandwidth = float(totalMB)/traceDuration
        print "Bandwidth = %.2f (MB/s)" % bandwidth        
        sortedkeys = images.keys()
        sortedkeys.sort()
        for img in sortedkeys:
            imgname = img.split("/")[-1]
            percentOfSubmit = float(images[img]) / totalSubmit
            percentOfNodes = float(imagesNodes[img]) / totalNodes
            print "%s: %u  (%.2f %% of submissions) %u nodes (%.2f %% of nodes)" % (imgname, images[img],  percentOfSubmit, imagesNodes[img], percentOfNodes)

        
if __name__ == "__main__":
    #trace = TraceFile.fromFile("256.random.trace")
    #injector = ARInjector(trace, "example.inject")
    #newtrace = injector.injectIntoTrace()
    #newtrace.toFile(sys.stdout)
    #therm = Thermometer(newtrace)
    #therm.printStats()
    c = Cooker("exampletrace.desc")
    trace = c.generateTrace()
    #trace.toFile(sys.stdout)
    #therm = Thermometer(trace)
    #therm.printStats()
    
    ac = OfflineAdmissionControl(trace, 10240, c.conf.admissioncontrol, None, c.conf.numc, c.conf.winsize)
    (accepted, rejected) = ac.filterInfeasible()
    print "\n\n\nACCEPTED"
    accepted.toFile(sys.stdout)
    therm = Thermometer(accepted)
    therm.printStats()
    
    #print "\n\n\nREJECTED"
    #if len(rejected.entries) > 0:
    #    rejected.toFile(sys.stdout)
    #    therm = Thermometer(rejected)
    #    therm.printStats()
    #else:
    #    print "None"
    
    