import ConfigParser
from mx.DateTime import ISO
import workspace.haizea.common.constants as constants
import workspace.haizea.common.stats as stats
import os.path

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
        
    def getInitialTime(self):
        timeopt = self.config.get(constants.SIMULATION_SEC,constants.STARTTIME_OPT)
        return ISO.ParseDateTime(timeopt)
    
    def getLogLevel(self):
        return self.config.get(constants.GENERAL_SEC, constants.LOGLEVEL_OPT)
    
    def getProfile(self):
        return self.config.get(constants.GENERAL_SEC, constants.PROFILE_OPT)

    def isSuspensionAllowed(self):
        return self.config.getboolean(constants.GENERAL_SEC, constants.SUSPENSION_OPT)

    def getMaxReservations(self):
        r = self.config.get(constants.GENERAL_SEC, constants.RESERVATIONS_OPT)
        if r == "unlimited":
            return None
        else:
            return int(r)

    def getDBTemplate(self):
        return self.config.get(constants.SIMULATION_SEC, constants.TEMPLATEDB_OPT)
    
    def getTargetDB(self):
        return self.config.get(constants.SIMULATION_SEC, constants.TARGETDB_OPT)
    
    def getNumPhysicalNodes(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.NODES_OPT)
    
    def getResourcesPerPhysNode(self):
        return self.config.get(constants.SIMULATION_SEC, constants.RESOURCES_OPT).split(";")
    
    def getBandwidth(self):
        return self.config.getint(constants.SIMULATION_SEC, constants.BANDWIDTH_OPT)

    def getTracefile(self):
        return self.config.get(constants.GENERAL_SEC, constants.TRACEFILE_OPT)

    def getInjectfile(self):
        injfile = self.config.get(constants.GENERAL_SEC, constants.INJFILE_OPT)
        if injfile == "None":
            return None
        else:
            return injfile

    
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
            traces = self.getTracefiles()
        else:
            traces = traces.split()

        return traces

    def getInjSubset(self, sec):
        injs = self.config.get(sec, constants.INJS_OPT)
        if injs == "ALL":
            injs = self.getInjectfiles()
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
        
        
class TraceConfig(Config):
    def __init__(self, c):
        Config.__init__(self, c)
        self.intervaldist = self.createDiscreteDistributionFromSection(constants.INTERVAL_SEC)
        self.numnodesdist = self.createDiscreteDistributionFromSection(constants.NUMNODES_SEC)
        self.deadlinedist = self.createDiscreteDistributionFromSection(constants.DEADLINE_SEC)
        self.durationdist = self.createDiscreteDistributionFromSection(constants.DURATION_SEC)
        self.imagesdist = self.createDiscreteDistributionFromSection(constants.IMAGES_SEC)
        
    def getTraceDuration(self):
        return self.config.getint(constants.GENERAL_SEC, constants.DURATION_OPT)
        
    def getDuration(self):
        return self.durationdist.get()
        