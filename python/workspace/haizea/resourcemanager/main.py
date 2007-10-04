import os
import os.path
import workspace.haizea.resourcemanager.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.common.constants as constants
import workspace.haizea.common.utils as utils
from workspace.haizea.common.config import Config
from workspace.haizea.common.log import log, loglevel
from pickle import Pickler, Unpickler

def simulate(config, statsdir):
    level = config.getLogLevel()
    log.setLevel(loglevel[level])

    tracefile = config.getTracefile()
    injectfile = config.getInjectfile()
    
    # Read trace file
    # Requests is a list of lease requests
    requests = None
    if tracefile.endswith(".csv"):
        requests = tracereaders.CSV(tracefile, config)
    elif tracefile.endswith(".swf"):
        requests = tracereaders.SWF(tracefile, config)
    elif tracefile.endswith(".gwf"):
        requests = tracereaders.GWF(tracefile, config)
    print len(requests)

    if injectfile != None:
        injectedleases = tracereaders.LWF(injectedfile)
        # TODO: Merge requests and injectedLeases
        
    resourceManager = rm.ResourceManager(requests, config)
    
    resourceManager.run()
    
    # Write data to disk
    profile = config.getProfile()
    dir = statsdir + "/" + utils.genDataDirName(profile, tracefile, injectfile)
    
    writeDataToDisk(resourceManager, dir)
    
def writeDataToDisk(resourcemanager, dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    cpuutilization = resourcemanager.stats.getUtilization(constants.RES_CPU)
    #memutilization = resourcemanager.stats.getUtilization(constants.RES_MEM)
    exactaccepted = resourcemanager.stats.getExactAccepted()
    exactrejected = resourcemanager.stats.getExactRejected()
    besteffortcompleted = resourcemanager.stats.getBestEffortCompleted()
    queuesize = resourcemanager.stats.getQueueSize()
    queuewait = resourcemanager.stats.getQueueWait()
    execwait = resourcemanager.stats.getExecWait()
    
    pickle(cpuutilization, dir, constants.CPUUTILFILE)
    #pickle(memutilization, dir, constants.MEMUTILFILE)
    #pickle(memutilizationavg, dir, constants.MEMUTILAVGFILE)
    pickle(exactaccepted, dir, constants.ACCEPTEDFILE)
    pickle(exactrejected, dir, constants.REJECTEDFILE)
    pickle(besteffortcompleted, dir, constants.COMPLETEDFILE)
    pickle(queuesize, dir, constants.QUEUESIZEFILE)
    pickle(queuewait, dir, constants.QUEUEWAITFILE)
    pickle(execwait, dir, constants.EXECWAITFILE)
    
        
def pickle(data, dir, file):
    f = open (dir + "/" + file, "w")
    p = Pickler(f)
    p.dump(data)
    f.close()


if __name__ == "__main__":
    configfile="../configfiles/das2.conf"
    config = Config()
    config.loadFile(configfile)
    tracefile="../traces/examples/DAS2/das200lines.gwf"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    simulate(config, tracefile, constants.TRACE_GWF, injectedfile, statsdir)