import ConfigParser, os
from workspace.util import stats
from workspace.traces import files
import random
import sys

GENERAL_SEC = "general"
INTERVAL_SEC = "interval"
NUMNODES_SEC = "numnodes"
DURATION_SEC = "duration"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"

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
MEAN_OPT = "mean"
STDEV_OPT = "stdev"
        
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
            fields = {}
            fields["time"] = str(time)
            img = self.conf.imageDist.get()
            fields["uri"] = img
            imgsize = self.conf.imageSizes[img]
            fields["size"] = str(imgsize)
            fields["numNodes"] = str(self.conf.numNodesDist.get())
            fields["mode"] = "RW"
            type = self.conf.arbatchDist.get()
            if type == "AR":
                tightness = self.conf.deadlineDist.get() / 100
                imgsizeKB = imgsize * 1024
                transferTime = imgsizeKB / self.conf.bandwidth
                deadline = int(transferTime * (1 + tightness))
                fields["deadline"] = str(deadline)
                fields["tag"] = "AR"
            elif type == "BATCH":
                fields["deadline"] = "NULL"
                fields["tag"] = "BATCH"
                
            fields["duration"] = str(int(self.conf.durationDist.get()))
            entries.append(files.TraceEntry(fields))
            
        return files.TraceFile(entries)
    
        
class ConfFile(object):
    
    def __init__(self):
        pass
        
    @staticmethod
    def createDiscreteDistributionFromSection(config, section):
        distType = config.get(section, DISTRIBUTION_OPT)
        probs = None
        if config.has_option(section, MIN_OPT) and config.has_option(section, MIN_OPT):
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
            values = []
            probs = []
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
                   _durationDist, _imageSizes, _bandwidth, _intervalDist,
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
        self.type = _type
    
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        bandwidth = config.getint(GENERAL_SEC, BANDWIDTH_OPT)
        duration = config.getint(GENERAL_SEC, DURATION_OPT)
        arPercent = config.get(GENERAL_SEC, AR_OPT)
        batchPercent = config.get(GENERAL_SEC, BATCH_OPT)
        
        arProb = float(arPercent)/100
        batchProb = float(batchPercent)/100

        arbatchDist = stats.DiscreteDistribution(["AR","BATCH"], [arProb,batchProb])
        
        type = config.get(GENERAL_SEC, TYPE_OPT)

        numNodesDist = cls.createDiscreteDistributionFromSection(config, NUMNODES_SEC)
        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)
        intervalDist = cls.createDiscreteDistributionFromSection(config, INTERVAL_SEC)
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
                   _durationDist = durationDist, _imageSizes = imageSizes, _bandwidth = bandwidth, _intervalDist= intervalDist,
                   _duration = duration, _arbatchDist = arbatchDist, _type = type)        


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
    

# TODO: Merge into TraceFile
class ARInjector(object):
    def __init__(self, trace, conffile):
        self.trace = trace
        self.conf = InjectorConf.fromFile(conffile)
        
    def injectIntoTrace(self):
        artrace = files.TraceFile()
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
            artrace.entries.append(files.TraceEntry(fields))
        
        # Add BATCH tag
        for entry in self.trace.entries:
            entry.fields["tag"] = "BATCH"
            
        newTraceEntries = self.trace.entries + artrace.entries    
        newTraceEntries.sort(files.TraceEntry.compare)
        return files.TraceFile(newTraceEntries)
    
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
        
        for entry in self.trace.entries:
            if entry.fields["tag"] == "AR":
                numAR += 1
                numARNodes += int(entry.fields["numNodes"])
            elif entry.fields["tag"] == "BATCH":
                numBatch += 1
                numBatchNodes += int(entry.fields["numNodes"])
            totalNodes += int(entry.fields["numNodes"])
            totalMB += int(entry.fields["size"])

        submissionRatioA2B = self.ratio(numAR, numBatch)
        submissionRatioB2A = self.ratio(numBatch, numAR)
        nodesRatioA2B = self.ratio(numARNodes, numBatchNodes)
        nodesRatioB2A = self.ratio(numBatchNodes, numARNodes)

        batchPercent = float(numBatch) / totalSubmit
        ARPercent = float(numAR) / totalSubmit
        batchNodePercent = float(numBatchNodes) / totalNodes
        ARNodePercent = float(numARNodes) / totalNodes
        
        bandwidth = float(totalMB) / duration
        
        print "SUBMISSIONS"
        print "  Batch =",numBatch
        print "     AR =", numAR
        print "% Batch =",batchPercent
        print "   % AR =", ARPercent
        print "Ratio (AR-to-Batch) =", submissionRatioA2B
        print "Ratio (Batch-to-AR) =", submissionRatioB2A
        print ""
        print "NODES"
        print "Batch =",numBatchNodes
        print "   AR =", numARNodes
        print "Ratio (AR-to-Batch) =", nodesRatioA2B 
        print "Ratio (Batch-to-AR) =", nodesRatioB2A
        print "% Batch =",batchNodePercent
        print "   % AR =", ARNodePercent
        print ""
        print "IMAGES"
        print "     Total MB =",totalMB
        print "Req.Bandwidth =",bandwidth, "MB/s"

        
if __name__ == "__main__":
    #trace = files.TraceFile.fromFile("256.random.trace")
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
    