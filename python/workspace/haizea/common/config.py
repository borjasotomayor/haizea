import ConfigParser
from mx.DateTime import ISO
import workspace.haizea.common.constants as constants

class Config(object):
    def __init__(self, configfile):
        file = open (configfile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(file)        
        
    def getInitialTime(self):
        timeopt = self.config.get(constants.SIMULATION_SEC,constants.STARTTIME_OPT)
        return ISO.ParseDateTime(timeopt)
    
    def getLogLevel(self):
        return self.config.get(constants.GENERAL_SEC, constants.LOGLEVEL_OPT)
    
    def getProfile(self):
        return self.config.get(constants.GENERAL_SEC, constants.PROFILE_OPT)

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
    
class MultiConfig(object):
    def __init__(self, configfile):
        file = open (configfile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(file)  
        
    def getProfiles(self):
        names = self.config.get(constants.PROFILES_SEC, constants.NAMES_OPT).split(",")
        return [s.strip(' ') for s in names]

    def getCSS(self):
        return self.config.get(constants.REPORTING_SEC, constants.CSS_OPT)

    
    def getConfigs(self):
        pass
#        if onlyprofile == None:
#            self.profilenames = [s.strip(' ') for s in config.get(MULTIRUN_SEC, PROFILES_OPT).split(",")]
#        else:
#            self.profilenames = [onlyprofile]
#    
#        for profile in self.profilenames:
#            profileconfig = ConfigParser.ConfigParser()
#            commonsections = [s for s in config.sections() if s.startswith("common:")]
#            profilesections = [s for s in config.sections() if s.startswith(profile +":")]
#            sections = commonsections + profilesections
#            for s in sections:
#                s_noprefix = s.split(":")[1]
#                items = config.items(s)
#                if not profileconfig.has_section(s_noprefix):
#                    profileconfig.add_section(s_noprefix)
#                for item in items:
#                    profileconfig.set(s_noprefix, item[0], item[1])
        