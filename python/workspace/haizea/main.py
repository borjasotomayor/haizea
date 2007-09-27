import ConfigParser
import workspace.haizea.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.constants as constants

config = None

class Config(object):
    def __init__(self, configfile):
        file = open (configfile, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)        
        
    def getInitialTime(self):
        pass

def simulate(configfile, tracefile, tracetype, injectedfile):
    
    # Create config file
    global config
    config = Config(configfile)
    
    # Read trace file
    # Requests is a list of lease requests
    requests = None
    if tracetype == constants.TRACE_CSV:
        requests = tracereaders.CSV(tracefile)
    elif tracetype == constants.TRACE_GWF:
        requests = tracereaders.GWF(tracefile)
        
    if injectedfile != None:
        injectedleases = tracereaders.LWF(injectedfile)
        # TODO: Merge requests and injectedLeases
        
    resourceManager = rm.ResourceManager(requests, config)
    
    resourceManager.run()
    
    # TODO: Write results to disk
    
if __name__ == "__main__":
    configfile="configfiles/test.conf"
    tracefile="traces/examples/test1.csv"
    simulate(configfile, tracefile, constants.TRACE_CSV, None)