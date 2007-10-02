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
        profiles = self.getProfiles()
    
        configs = []
        for profile in profiles:
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
            c = Config()
            c.loadFromConfig(profileconfig)
            configs.append(c)
        
        return configs
            
        