import os
import os.path
import haizea.resourcemanager.rm as rm
import haizea.resourcemanager.datastruct as ds
import haizea.traces.readers as tracereaders
import haizea.common.constants as constants
import haizea.common.utils as utils
from haizea.common.config import RMConfig

if __name__ == "__main__":
    configfile="../configfiles/test.conf"
    tracefile="../traces/examples/test_preempt1.csv"
    imagefile="../traces/examples/1GBfiles.images"
    injectedfile="None"
    #tracefile="../traces/examples/test_inject.csv"
    #injectedfile="../traces/examples/test_inject.lwf"
    

    config = RMConfig.fromFile(configfile)
    config.config.set(constants.GENERAL_SEC, constants.TRACEFILE_OPT, tracefile)
    config.config.set(constants.GENERAL_SEC, constants.INJFILE_OPT, injectedfile)
    config.config.set(constants.GENERAL_SEC, constants.IMGFILE_OPT, imagefile)

    simulate(config, statsdir)