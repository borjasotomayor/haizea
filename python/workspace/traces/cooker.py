import ConfigParser, os
from workspace.util import stats
from workspace.traces.files import TraceFile, TraceEntry
import random
import sys

GENERAL_SEC = "general"
INTERVAL_SEC = "interval"
NUMNODES_SEC = "numnodes"
DURATION_SEC = "duration"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"
SIMULTANEOUS_SEC = "simultaneousrequests"

BANDWIDTH_OPT = "bandwidth"
DURATION_OPT = "duration"
AR_OPT = "ar"
BATCH_OPT = "batch"
TYPE_OPT = "type"
SIZES_OPT = "imagesizes"

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
        
class Cooker(object):
    def __init__(self, conffile):
        self.conf = TraceConf.fromFile(conffile)
    
    def generateTrace(self):
        entries = []
        maxtime = self.conf.traceDuration
        time = 0
        while int(time) < int(maxtime):
            entrytime = self.conf.intervalDist.get()
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
                numNodes = self.conf.numNodesDist.get()
                fields["numNodes"] = str(numNodes)
                fields["mode"] = "RW"
                type = self.conf.arbatchDist.get()
                if type == "AR":
                    tightness = self.conf.deadlineDist.get() / 100
                    imgsizeKB = imgsize * 1024 * numNodes
                    transferTime = imgsizeKB / self.conf.bandwidth
                    deadline = int(transferTime * (1 + tightness))
                    fields["deadline"] = str(deadline)
                    fields["tag"] = "AR"
                elif type == "BATCH":
                    fields["deadline"] = "NULL"
                    fields["tag"] = "BATCH"
                    
                fields["duration"] = str(int(self.conf.durationDist.get()))
                entries.append(TraceEntry(fields))
        return TraceFile(entries)


class OfflineScheduleEntry(object):
    def __init__(self, traceentry, transferTime, absdeadline):
        self.traceentry = traceentry
        self.transferTime = transferTime
        self.absdeadline = absdeadline
        
    def compare(a, b):
        return a.absdeadline - b.absdeadline
     
class OfflineAdmissionControl(object):
    def __init__(self, trace, bandwidth, numNodesDist):
        self.trace = trace
        self.bandwidth = bandwidth
        self.numNodesDist = numNodesDist
        self.schedule = []

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
                   _duration, _arbatchDist, _type):
        self.imageDist = _imageDist
        self.imageSizes = _imageSizes
        self.numNodesDist = _numNodesDist
        self.deadlineDist = _deadlineDist
        self.durationDist = _durationDist
        self.intervalDist = _intervalDist
        self.traceDuration = _duration
        self.bandwidth = _bandwidth
        self.arbatchDist = _arbatchDist
        self.simulDist = _simulDist
        self.type = _type
    
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        if config.has_option(GENERAL_SEC, BANDWIDTH_OPT):
            bandwidth = config.getint(GENERAL_SEC, BANDWIDTH_OPT)
        else:
            bandwidth = None
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

        numNodesDist = cls.createDiscreteDistributionFromSection(config, NUMNODES_SEC)
        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)
        intervalDist = cls.createDiscreteDistributionFromSection(config, INTERVAL_SEC)
        durationDist = cls.createContinuousDistributionFromSection(config, DURATION_SEC)
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
                   _duration = duration, _arbatchDist = arbatchDist, _type = type, _simulDist = simulDist)        


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
    def __init__(self, trace):
        self.trace = trace

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
        duration = int(self.trace.entries[-1].fields["time"])
        images = {}        
        imagesNodes = {}
 
        for entry in self.trace.entries:
	    numNodes = int(entry.fields["numNodes"])
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
            elif entry.fields["tag"] == "BATCH":
                numBatch += 1
                numBatchNodes += numNodes
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
        
        
        print "SUBMISSIONS"
        print "-----------"
        print "  Batch =",numBatch
        print "     AR =", numAR
        print "% Batch =",batchPercent
        print "   % AR =", ARPercent
        print "Ratio (AR-to-Batch) =", submissionRatioA2B
        print "Ratio (Batch-to-AR) =", submissionRatioB2A
        print ""
        print ""
        print "NODES"
        print "-----"
        print "Batch =",numBatchNodes
        print "   AR =", numARNodes
        print "Ratio (AR-to-Batch) =", nodesRatioA2B 
        print "Ratio (Batch-to-AR) =", nodesRatioB2A
        print "% Batch =",batchNodePercent
        print "   % AR =", ARNodePercent
        print ""
        print ""
        print "IMAGES"
        print "------"
        print "Total MB =",totalMB
        bandwidth = float(totalMB)/duration
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
    trace.toFile(sys.stdout)
    therm = Thermometer(trace)
    therm.printStats()
    
    ac = OfflineAdmissionControl(trace, 10240, 800)
    (accepted, rejected) = ac.filterInfeasible()
    print "\n\n\nACCEPTED"
    accepted.toFile(sys.stdout)
    therm = Thermometer(accepted)
    therm.printStats()
    
    print "\n\n\nREJECTED"
    if len(rejected.entries) > 0:
        rejected.toFile(sys.stdout)
        therm = Thermometer(rejected)
        therm.printStats()
    else:
        print "None"
    
    