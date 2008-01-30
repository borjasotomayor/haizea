import os
import os.path
import workspace.haizea.resourcemanager.rm as rm
import workspace.haizea.traces.readers as tracereaders
import workspace.haizea.common.constants as constants
import workspace.haizea.common.utils as utils
from workspace.haizea.common.config import RMConfig
from workspace.haizea.common.log import log, loglevel
from pickle import Pickler, Unpickler
import operator

def simulate(config, statsdir):
    level = config.getLogLevel()
    log.setLevel(loglevel[level])
    if level == "EXTREMEDEBUG":
        log.extremedebug = True

    tracefile = config.getTracefile()
    injectfile = config.getInjectfile()
    imagefile = config.getImagefile()
    
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
        injectedleases = tracereaders.LWF(injectfile, config)
        requests += injectedleases
        requests.sort(key=operator.attrgetter("tSubmit"))
        
    if imagefile != None:
        imagesizes, images = tracereaders.IMG(imagefile)
        for r,i in zip(requests,images):
            r.vmimage = i
            r.vmimagesize = imagesizes[i]
            r.resreq.set(constants.RES_DISK, imagesizes[i] + r.resreq.get(constants.RES_MEM))
       
    resourceManager = rm.ResourceManager(requests, config)
    
    resourceManager.run()
    
    for n in resourceManager.enactment.nodes:
        n.printFiles()
    
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
    utilratio = resourcemanager.stats.getUtilizationRatio()
    diskusage = resourcemanager.stats.getDiskUsage()
    boundedslowdown = resourcemanager.stats.getBoundedSlowdown()

    pickle(cpuutilization, dir, constants.CPUUTILFILE)
    #pickle(memutilization, dir, constants.MEMUTILFILE)
    #pickle(memutilizationavg, dir, constants.MEMUTILAVGFILE)
    pickle(exactaccepted, dir, constants.ACCEPTEDFILE)
    pickle(exactrejected, dir, constants.REJECTEDFILE)
    pickle(besteffortcompleted, dir, constants.COMPLETEDFILE)
    pickle(queuesize, dir, constants.QUEUESIZEFILE)
    pickle(queuewait, dir, constants.QUEUEWAITFILE)
    pickle(execwait, dir, constants.EXECWAITFILE)
    pickle(utilratio, dir, constants.UTILRATIOFILE)
    pickle(diskusage, dir, constants.DISKUSAGEFILE)
    pickle(boundedslowdown, dir, constants.SLOWDOWNFILE)
        
def pickle(data, dir, file):
    f = open (dir + "/" + file, "w")
    p = Pickler(f)
    p.dump(data)
    f.close()


if __name__ == "__main__":
    configfile="../configfiles/test.conf"
    tracefile="../traces/examples/test_simple.csv"
    imagefile="../traces/examples/1GBfiles.images"
    injectedfile="None"
    #tracefile="../traces/examples/test_inject.csv"
    #injectedfile="../traces/examples/test_inject.lwf"
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"

    config = RMConfig.fromFile(configfile)
    config.config.set(constants.GENERAL_SEC, constants.TRACEFILE_OPT, tracefile)
    config.config.set(constants.GENERAL_SEC, constants.INJFILE_OPT, injectedfile)
    config.config.set(constants.GENERAL_SEC, constants.IMGFILE_OPT, imagefile)

    simulate(config, statsdir)