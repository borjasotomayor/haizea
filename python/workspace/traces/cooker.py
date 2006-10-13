import ConfigParser, os
from workspace.util import stats
import random

GENERAL_SEC = "general"
NUMNODES_SEC = "numnodes"
DURATION_SEC = "duration"
DEADLINE_SEC = "deadline"
IMAGES_SEC = "images"

BANDWIDTH_OPT = "bandwidth"
DURATION_OPT = "duration"
ARBATCH_OPT = "ARtoBatch"
TYPE_OPT = "type"
SIZES_OPT = "imagesizes"

DISTRIBUTION_OPT = "distribution"
MIN_OPT = "min"
MAX_OPT = "max"
ITEMS_OPT = "items"
MEAN_OPT = "mean"
STDEV_OPT = "stdev"
        
class Cooker(object):
    def __init__(self):
        pass
    
    def generateTrace(self):
        pass
    
    def run(self, *argv):
        print "Does nothing"
        
class TraceConf(object):
    
    def __init__(self, _imageDist, _numNodesDist, _deadlineDist,
                   _durationDist, _imageSizes, _bandwidth,
                   _duration, _arbatchRatio, _type):
        self.imageDist = _imageDist
        self.imageSizes = _imageSizes
        self.numNodesDist = _numNodesDist
        self.deadlineDist = _deadlineDist
        self.durationDist = _durationDist
        self.traceDuration = _duration
        self.bandwidth = _bandwidth
        self.arbatchRatio = _arbatchRatio
        self.type = _type
    
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)
        
        bandwidth = config.getint(GENERAL_SEC, BANDWIDTH_OPT)
        duration = config.getint(GENERAL_SEC, DURATION_OPT)
        arbatchRatio = config.get(GENERAL_SEC, ARBATCH_OPT).split("/")
        for i,num in enumerate(arbatchRatio):
            arbatchRatio[i] = num.strip()
        type = config.get(GENERAL_SEC, TYPE_OPT)

        numNodesDist = cls.createDiscreteDistributionFromSection(config, NUMNODES_SEC)
        imagesDist = cls.createDiscreteDistributionFromSection(config, IMAGES_SEC)
        deadlineDist = cls.createContinuousDistributionFromSection(config, DEADLINE_SEC)
        durationDist = cls.createContinuousDistributionFromSection(config, DURATION_SEC)
        
        # Get image sizes
        imageSizesOpt = config.get(IMAGES_SEC, SIZES_OPT).split(",")
        imageSizesFile = open(imageSizesOpt[0], "r")
        imageSizesField = imageSizesOpt[1]
        imageSizes = {}
        for line in file:
            fields = line.split(",")
            imgname = fields[0]
            imgsize = fields[imageSizesField]
            imageSizes[imgname] = imgsize

        imagesDist.testDistribution()

        return cls(_imageDist=imagesDist, _numNodesDist=numNodesDist, _deadlineDist=deadlineDist,
                   _durationDist = durationDist, _imageSizes = imageSizes, _bandwidth = bandwidth,
                   _duration = duration, _arbatchRatio = arbatchRatio, _type = type)        
    
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
                value = line.strip().split(",")[0]
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

        
        
if __name__ == "__main__":
    traceconf= TraceConf.fromFile("exampletrace.desc")