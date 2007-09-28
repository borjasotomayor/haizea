import workspace.haizea.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.constants as constants
from workspace.haizea.config import Config
from workspace.haizea.log import log, loglevel

def simulate(configfile, tracefile, tracetype, injectedfile):
    # Create config file
    config = Config(configfile)
    
    level = config.getLogLevel()
    log.setLevel(loglevel[level])

    
    # Read trace file
    # Requests is a list of lease requests
    requests = None
    if tracetype == constants.TRACE_CSV:
        requests = tracereaders.CSV(tracefile, config)
    elif tracetype == constants.TRACE_GWF:
        requests = tracereaders.GWF(tracefile, config)
        
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