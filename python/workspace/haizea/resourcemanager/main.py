import os
import os.path
import workspace.haizea.resourcemanager.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.common.constants as constants
import workspace.haizea.common.utils as utils
from workspace.haizea.common.config import Config
from workspace.haizea.common.log import log, loglevel

def simulate(configfile, tracefile, tracetype, injectedfile, statsdir):
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
    
    # Write data to disk
    profile = config.getProfile()
    dir = statsdir + "/" + utils.genDataDirName(profile, tracefile, injectedfile)
    
    writeDataToDisk(resourceManager, dir)
    
def writeDataToDisk(resourcemanager, dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    


if __name__ == "__main__":
    configfile="../configfiles/test.conf"
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    simulate(configfile, tracefile, constants.TRACE_CSV, injectedfile, statsdir)