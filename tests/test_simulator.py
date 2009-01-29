import ConfigParser

from haizea.resourcemanager.configfile import HaizeaConfig
from haizea.resourcemanager.rm import ResourceManager


def load_configfile(configfile, tracefile):
    file = open (configfile, "r")
    c = ConfigParser.ConfigParser()
    c.readfp(file)
    c.set("tracefile", "tracefile", tracefile)
    cfg = HaizeaConfig(c)
    return cfg
    
def test_preemption():
    config = load_configfile("base_config_simulator.conf", "preemption.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_preemption_prematureend():
    config = load_configfile("base_config_simulator.conf", "preemption_prematureend.lwf")
    rm = ResourceManager(config)
    rm.start()