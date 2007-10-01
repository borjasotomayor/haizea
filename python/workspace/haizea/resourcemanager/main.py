import os
import os.path
import workspace.haizea.resourcemanager.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.common.constants as constants
import workspace.haizea.common.utils as utils
from workspace.haizea.common.config import Config
from workspace.haizea.common.log import log, loglevel
from pickle import Pickler, Unpickler

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
        
    cpuutilization = resourcemanager.stats.getUtilization(constants.RES_CPU)
    cpuutilizationavg = resourcemanager.stats.getUtilizationAvg(constants.RES_CPU)
    #memutilization = resourcemanager.stats.getUtilization(constants.RES_MEM)
    #memutilizationavg = resourcemanager.stats.getUtilizationAvg(constants.RES_MEM)
    exactaccepted = resourcemanager.stats.getExactAccepted()
    exactrejected = resourcemanager.stats.getExactRejected()
    besteffortcompleted = resourcemanager.stats.getBestEffortCompleted()
    queuesize = resourcemanager.stats.getQueueSize()
    queuewait = resourcemanager.stats.getQueueWait()
    
    pickle(cpuutilization, dir, constants.CPUUTILFILE)
    pickle(cpuutilizationavg, dir, constants.CPUUTILAVGFILE)
    #pickle(memutilization, dir, constants.MEMUTILFILE)
    #pickle(memutilizationavg, dir, constants.MEMUTILAVGFILE)
    pickle(exactaccepted, dir, constants.ACCEPTEDFILE)
    pickle(exactrejected, dir, constants.REJECTEDFILE)
    pickle(besteffortcompleted, dir, constants.COMPLETEDFILE)
    pickle(queuesize, dir, constants.QUEUESIZEFILE)
    pickle(queuewait, dir, constants.QUEUEWAITFILE)
    
        
def pickle(data, dir, file):
    f = open (dir + "/" + file, "w")
    p = Pickler(f)
    p.dump(data)
    f.close()


if __name__ == "__main__":
    configfile="../configfiles/test.conf"
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    simulate(configfile, tracefile, constants.TRACE_CSV, injectedfile, statsdir)