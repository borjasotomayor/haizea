import ConfigParser
from mx.DateTime import ISO
import workspace.haizea.constants as constants

class Config(object):
    def __init__(self, configfile):
        file = open (configfile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(file)        
        
    def getInitialTime(self):
        timeopt = self.config.get(constants.SIMULATION_SEC,constants.STARTTIME_OPT)
        return ISO.ParseDateTime(timeopt)