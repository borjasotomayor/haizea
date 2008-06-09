import ConfigParser
from mx.DateTime import ISO
from mx.DateTime import TimeDelta
import haizea.common.constants as constants
import haizea.common.stats as stats
import os.path
from haizea.common.utils import genDataDirName

class Config(object):
    def __init__(self, config):
        self.config = config
        
    @classmethod
    def fromFile(cls, configfile):
        file = open (configfile, "r")
        c = ConfigParser.ConfigParser()
        c.readfp(file)
        return cls(c)
       
    def createDiscreteDistributionFromSection(self, section):
        distType = self.config.get(section, constants.DISTRIBUTION_OPT)
        probs = None
        if self.config.has_option(section, constants.MIN_OPT) and self.config.has_option(section, constants.MAX_OPT):
            min = self.config.getint(section, constants.MIN_OPT)
            max = self.config.getint(section, constants.MAX_OPT)
            values = range(min,max+1)
        elif self.config.has_option(section, constants.ITEMS_OPT):
            pass
        elif self.config.has_option(section, constants.ITEMSPROBS_OPT):
            pass
        elif self.config.has_option(section, constants.ITEMSFILE_OPT):
            filename = config.get(section, constants.ITEMSFILE_OPT)
            file = open (filename, "r")
            values = []
            for line in file:
                value = line.strip().split(";")[0]
                values.append(value)
        elif self.config.has_option(section, constants.ITEMSPROBSFILE_OPT):
            itemsprobsOpt = self.config.get(section, constants.ITEMSPROBSFILE_OPT).split(",")
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
        if distType == constants.DIST_UNIFORM:
            dist = stats.DiscreteUniformDistribution(values)
        elif distType == constants.DIST_EXPLICIT:
            if probs == None:
                raise Exception, "No probabilities specified"
            dist = stats.DiscreteDistribution(values, probs) 
            
        return dist
        
    def createContinuousDistributionFromSection(self, section):
        distType = self.config.get(section, DISTRIBUTION_OPT)
        min = self.config.getfloat(section, MIN_OPT)
        max = self.config.get(section, MAX_OPT)
        if max == "unbounded":
            max = float("inf")
        if distType == "uniform":
            dist = stats.ContinuousUniformDistribution(min, max)
        elif distType == "normal":
            mu = self.config.getfloat(section, MEAN_OPT)
            sigma = self.config.getfloat(section, STDEV_OPT)
            dist = stats.ContinuousNormalDistribution(min,max,mu,sigma)
        elif distType == "pareto":
            pass 
        
        return dist
        

        
class RMConfig(Config):
    def __init__(self, config):
        Config.__init__(self, config)
        
    #
    # GENERAL OPTIONS
    #

    def getLogLevel(self):
        return self.config.get(constants.GENERAL_SEC, constants.LOGLEVEL_OPT)
    
    def getProfile(self):
        return self.config.get(constants.GENERAL_SEC, constants.PROFILE_OPT)

    #
    # SIMULATION OPTIONS
    #
        
    def getInitialTime(self):
        timeopt = self.config.get(constants.SIMULATION_SEC,constants.STARTTIME_OPT)
        return ISO.ParseDateTime(timeopt)
    
    def getNumPhysicalNodes(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.NODES_OPT)
    
    def getResourcesPerPhysNode(self):
        return self.config.get(constants.SIMULATION_SEC, constants.RESOURCES_OPT).split(";")
    
    def getBandwidth(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.BANDWIDTH_OPT)

    def getSuspendResumeRate(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.SUSPENDRATE_OPT)
    
    def stopWhen(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.STOPWHEN_OPT):
            return None
        else:
            return self.config.get(constants.SIMULATION_SEC, constants.STOPWHEN_OPT)

    def getForceTransferTime(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.FORCETRANSFERT_OPT):
            return None
        else:
            return TimeDelta(seconds=self.config.getint(constants.SIMULATION_SEC, constants.FORCETRANSFERT_OPT))

    def getRuntimeOverhead(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.RUNOVERHEAD_OPT):
            return None
        else:
            return self.config.getint(constants.SIMULATION_SEC, constants.RUNOVERHEAD_OPT)

    def getBootOverhead(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.BOOTOVERHEAD_OPT):
            time = 0
        else:
            time = self.config.getint(constants.SIMULATION_SEC, constants.BOOTOVERHEAD_OPT)
        return TimeDelta(seconds=time)

    def overheadOnlyBestEffort(self):
        if not self.config.has_option(constants.SIMULATION_SEC, constants.RUNOVERHEADBE_OPT):
            return False
        else:
            return self.config.getboolean(constants.SIMULATION_SEC, constants.RUNOVERHEADBE_OPT)

    #
    # SCHEDULING OPTIONS
    #

    def getSuspensionType(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.SUSPENSION_OPT)

    def isMigrationAllowed(self):
        return self.config.getboolean(constants.SCHEDULING_SEC, constants.MIGRATION_OPT)

    def getMustMigrate(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.MIGRATE_OPT)

    def getMaxReservations(self):
        if self.getBackfillingType() == constants.BACKFILLING_OFF:
            return 0
        elif self.getBackfillingType() == constants.BACKFILLING_AGGRESSIVE:
            return 1
        elif self.getBackfillingType() == constants.BACKFILLING_CONSERVATIVE:
            return 1000000
        elif self.getBackfillingType() == constants.BACKFILLING_INTERMEDIATE:
            r = self.config.getint(constants.SCHEDULING_SEC, constants.RESERVATIONS_OPT)
            return r

    def getSuspendThreshold(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLD_OPT):
            return 0
        else:
            return TimeDelta(seconds=self.config.getint(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLD_OPT))

    def getSuspendThresholdFactor(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLDFACTOR_OPT):
            return None
        else:
            return self.config.getfloat(constants.SCHEDULING_SEC, constants.SUSPENDTHRESHOLDFACTOR_OPT)

    def isBackfilling(self):
        if self.getBackfillingType() == constants.BACKFILLING_OFF:
            return False
        else:
            return True
        
    def getBackfillingType(self):
        return self.config.get(constants.SCHEDULING_SEC, constants.BACKFILLING_OPT)

    def getTransferType(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.TRANSFER_OPT):
            return constants.TRANSFER_NONE
        else:
            return self.config.get(constants.SCHEDULING_SEC, constants.TRANSFER_OPT)

    def getReuseAlg(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.REUSE_OPT):
            return constants.REUSE_NONE
        else:
            return self.config.get(constants.SCHEDULING_SEC, constants.REUSE_OPT)
        
    def getMaxPoolSize(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.MAXPOOL_OPT):
            return constants.POOL_UNLIMITED
        else:
            return self.config.getint(constants.SCHEDULING_SEC, constants.MAXPOOL_OPT)        
        
    def isAvoidingRedundantTransfers(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.AVOIDREDUNDANT_OPT):
            return False
        else:
            return self.config.getboolean(constants.SCHEDULING_SEC, constants.AVOIDREDUNDANT_OPT)

    def getNodeSelectionPolicy(self):
        if not self.config.has_option(constants.SCHEDULING_SEC, constants.NODESELECTION_OPT):
            return constants.NODESELECTION_AVOIDPREEMPT
        else:
            return self.config.get(constants.SCHEDULING_SEC, constants.NODESELECTION_OPT)


    #
    # TRACEFILE OPTIONS
    #
    def getTracefile(self):
        return self.config.get(constants.TRACEFILE_SEC, constants.TRACEFILE_OPT)

    def getInjectfile(self):
        injfile = self.config.get(constants.TRACEFILE_SEC, constants.INJFILE_OPT)
        if injfile == "None":
            return None
        else:
            return injfile

    def getImagefile(self):
        imgfile = self.config.get(constants.TRACEFILE_SEC, constants.IMGFILE_OPT)
        if imgfile == "None":
            return None
        else:
            return imgfile


class GraphDataEntry(object):
    def __init__(self, title, profile, trace, inject):
        self.title = title
        self.dirname = genDataDirName(profile,trace,inject)

class GraphConfig(Config):
    def __init__(self, config):
        Config.__init__(self, config)    
    
    def getTitle(self):
        return self.config.get(constants.GENERAL_SEC, constants.TITLE_OPT)
    
    def getDatafile(self):
        return self.config.get(constants.GENERAL_SEC, constants.DATAFILE_OPT)

    def getTitleX(self):
        return self.config.get(constants.GENERAL_SEC, constants.TITLEX_OPT)
    
    def getTitleY(self):
        return self.config.get(constants.GENERAL_SEC, constants.TITLEY_OPT)

    def getGraphType(self):
        graphname = self.config.get(constants.GENERAL_SEC, constants.GRAPHTYPE_OPT)
        return constants.graphtype[graphname]
    
    def getDataEntries(self):
        datasections = [s for s in self.config.sections() if s.startswith("data")]
        datasections.sort()
        data = []
        for s in datasections:
            title = self.config.get(s, constants.TITLE_OPT)
            profile = self.config.get(s, constants.PROFILE_OPT)
            trace = self.config.get(s, constants.TRACE_OPT)
            inject = self.config.get(s, constants.INJ_OPT)
            if inject == "NONE":
                inject = None
            data.append(GraphDataEntry(title, profile, trace, inject))
        return data
    
class RMMultiConfig(Config):
    def __init__(self, config):
        Config.__init__(self, config)
        
    def getProfiles(self):
        names = self.config.get(constants.PROFILES_SEC, constants.NAMES_OPT).split(",")
        return [s.strip(' ') for s in names]
    
    def getProfilesSubset(self, sec):
        profiles = self.config.get(sec, constants.PROFILES_OPT)
        if profiles == "ALL":
            profiles = self.getProfiles()
        else:
            profiles = profiles.split()
        return profiles

    def getTracesSubset(self, sec):
        traces = self.config.get(sec, constants.TRACES_OPT)
        if traces == "ALL":
            traces = [os.path.basename(t) for t in self.getTracefiles()]
        else:
            traces = traces.split()
            
        return traces

    def getInjSubset(self, sec):
        injs = self.config.get(sec, constants.INJS_OPT)
        if injs == "ALL":
            injs = [os.path.basename(t) for t in self.getInjectfiles() if t!=None]
            injs.append(None)
        elif injs == "NONE":
            injs = [None]
        else:
            injs = injs.split()
        return injs

    def getTracefiles(self):
        dir = self.config.get(constants.TRACES_SEC, constants.TRACEDIR_OPT)
        traces = self.config.get(constants.TRACES_SEC, constants.TRACEFILES_OPT).split()
        return [dir + "/" + t for t in traces]

    def getInjectfiles(self):
        dir = self.config.get(constants.INJECTIONS_SEC, constants.INJDIR_OPT)
        inj = self.config.get(constants.INJECTIONS_SEC, constants.INJFILES_OPT).split()
        inj = [dir + "/" + i for i in inj]
        inj.append(None)
        return inj

    def getCSS(self):
        return self.config.get(constants.REPORTING_SEC, constants.CSS_OPT)

    
    def getConfigs(self):
        profiles = self.getProfiles()
        tracefiles = self.getTracefiles()
        injectfiles = self.getInjectfiles()
    
        configs = []
        for profile in profiles:
            for tracefile in tracefiles:
                for injectfile in injectfiles:
                    profileconfig = ConfigParser.ConfigParser()
                    commonsections = [s for s in self.config.sections() if s.startswith("common:")]
                    profilesections = [s for s in self.config.sections() if s.startswith(profile +":")]
                    sections = commonsections + profilesections
                    for s in sections:
                        s_noprefix = s.split(":")[1]
                        items = self.config.items(s)
                        if not profileconfig.has_section(s_noprefix):
                            profileconfig.add_section(s_noprefix)
                        for item in items:
                            profileconfig.set(s_noprefix, item[0], item[1])
                    profileconfig.set(constants.GENERAL_SEC, constants.PROFILE_OPT, profile)
                    profileconfig.set(constants.GENERAL_SEC, constants.TRACEFILE_OPT, tracefile)
                    if injectfile == None:
                        inj = "None"
                    else:
                        inj = injectfile
                    profileconfig.set(constants.GENERAL_SEC, constants.INJFILE_OPT, inj)
                    c = RMConfig(profileconfig)
                    configs.append(c)
        
        return configs
    
    def getReportDir(self):
        return self.config.get(constants.REPORTING_SEC, constants.REPORTDIR_OPT)
            
    def getConfigsToRun(self):
        configs = self.getConfigs()
        profiles = self.getProfilesSubset(constants.RUN_SEC)
        traces = self.getTracesSubset(constants.RUN_SEC)
        injs = self.getInjSubset(constants.RUN_SEC)
        
        confs = []
        for c in configs:
            p = c.getProfile()
            t = os.path.basename(c.getTracefile())
            i = c.getInjectfile()
            if i != None: 
                i = os.path.basename(i)

            if p in profiles and t in traces and i in injs:
                confs.append(c)

        return confs
        
    def getConfigsToReport(self):
        configs = self.getConfigs()
        profiles = self.getProfilesSubset(constants.REPORTING_SEC)
        traces = self.getTracesSubset(constants.REPORTING_SEC)
        injs = self.getInjSubset(constants.REPORTING_SEC)

        confs = []
        for c in configs:
            p = c.getProfile()
            t = os.path.basename(c.getTracefile())
            i = c.getInjectfile()
            if i != None: 
                i = os.path.basename(i)
            
            if p in profiles and t in traces and i in injs:
                confs.append(c)
                
        return confs
    
    def getGraphSections(self):
        secs = [s for s in self.config.sections() if s.startswith("graph-")]
        secs.sort()
        return secs
        
    def getGraphTitle(self, graphsec):
        return self.config.get(graphsec, constants.TITLE_OPT)
    
    def getGraphDatafile(self, graphsec):
        return self.config.get(graphsec, constants.DATAFILE_OPT)
    
    def getGraphType(self, graphsec):
        return self.config.get(graphsec, constants.GRAPHTYPE_OPT)
    
    def getGraphTable(self, graphsec):
        if self.config.has_option(graphsec, constants.TABLE_OPT):
            return self.config.get(graphsec, constants.TABLE_OPT)
        else:
            return None
    
    def getGraphClip(self, graphsec):
        def parseClip(clip):
            if clip[-1] == "%":
                return (constants.CLIP_PERCENTSUBMITTED, int(clip[:-1]))
            elif clip[-1] == "s":
                return (constants.CLIP_TIMESTAMP, int(clip[:-1]))
            elif clip == constants.CLIP_LASTSUBMISSION:
                return (constants.CLIP_LASTSUBMISSION, None)
            elif clip == constants.CLIP_NOCLIP:
                return (constants.CLIP_NOCLIP, None)
        
        if self.config.has_option(graphsec, constants.CLIPSTART_OPT) and self.config.has_option(graphsec, constants.CLIPEND_OPT):
            clipstart = self.config.get(graphsec, constants.CLIPSTART_OPT)
            clipend = self.config.get(graphsec, constants.CLIPEND_OPT)
            return (parseClip(clipstart), parseClip(clipend))
        else:
            return None
    
    def getGraphSlideshow(self, graphsec):
        if not self.config.has_option(graphsec, constants.SLIDESHOW_OPT):
            return False
        else:
            return self.config.getboolean(graphsec, constants.SLIDESHOW_OPT)
    
        
class TraceConfig(Config):
    def __init__(self, c):
        Config.__init__(self, c)
        self.numnodesdist = self.createDiscreteDistributionFromSection(constants.NUMNODES_SEC)
        self.deadlinedist = self.createDiscreteDistributionFromSection(constants.DEADLINE_SEC)
        self.durationdist = self.createDiscreteDistributionFromSection(constants.DURATION_SEC)
        self.imagesdist = self.createDiscreteDistributionFromSection(constants.IMAGES_SEC)
        if self.isGenerateBasedOnWorkload():
            # Find interval between requests
            tracedur = self.getTraceDuration()
            percent = self.getPercent()
            nodes = self.getNumNodes()
            accumduration = tracedur * nodes * percent
            numreqs = accumduration / (self.numnodesdist.getAvg() * self.durationdist.getAvg())
            intervalavg = int(tracedur / numreqs)
            min = intervalavg - 3600 # Make this configurable
            max = intervalavg + 3600 # Make this configurable
            values = range(min,max+1)
            self.intervaldist = stats.DiscreteUniformDistribution(values)
        else:
            self.intervaldist = self.createDiscreteDistributionFromSection(constants.INTERVAL_SEC)
        
    def getTraceDuration(self):
        return self.config.getint(constants.GENERAL_SEC, constants.DURATION_OPT)
        
    def getPercent(self):
        percent = self.config.getint(constants.WORKLOAD_SEC, constants.PERCENT_OPT)
        percent = percent / 100.0
        return percent
    
    def getNumNodes(self):
        return self.config.getint(constants.WORKLOAD_SEC, constants.NUMNODES_OPT)

    def getDuration(self):
        return self.durationdist.get()
    
    def isGenerateBasedOnWorkload(self):
        return self.config.has_section(constants.WORKLOAD_SEC)

    
class ImageConfig(Config):
    def __init__(self, c):
        Config.__init__(self, c)
        self.sizedist = self.createDiscreteDistributionFromSection(constants.SIZE_SEC)
        numimages = self.config.getint(constants.GENERAL_SEC, constants.IMAGES_OPT)
        self.images = ["image_" + str(i+1) for i in range(numimages)]
        
        distribution = self.config.get(constants.GENERAL_SEC, constants.DISTRIBUTION_OPT)
        if distribution == "uniform":
            self.imagedist = stats.DiscreteUniformDistribution(self.images) 
        else:
            probs = []
            explicitprobs = distribution.split()
            for p in explicitprobs:
                numitems, prob = p.split(",")
                itemprob = float(prob)/100
                for i in range(int(numitems)):
                    probs.append(itemprob)
            self.imagedist = stats.DiscreteDistribution(self.images, probs)
            print probs
    
    def getFileLength(self):
        return self.config.getint(constants.GENERAL_SEC, constants.LENGTH_OPT)
        

        
        