import ConfigParser
from mx.DateTime import ISO
import workspace.haizea.common.constants as constants

class Config(object):
    def __init__(self):
        self.config = None
        
    def loadFile(self, configfile):
        file = open (configfile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(file)
        
    def loadFromConfig(self, config):
        self.config = config
        
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

    
class MultiConfig(object):
    def __init__(self, configfile):
        file = open (configfile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(file)  
        
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
                    c = Config()
                    c.loadFromConfig(profileconfig)
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
            t = c.getTracefile()
            i = c.getInjectfile()
            
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
            t = c.getTracefile()
            i = c.getInjectfile()
            
            if p in profiles and t in traces and i in injs:
                confs.append(c)
                
        return confs
        