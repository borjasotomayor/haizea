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
    
def test_preemption_prematureend2():
    config = load_configfile("base_config_simulator.conf", "preemption_prematureend2.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_reservation():
    config = load_configfile("base_config_simulator.conf", "reservation.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_reservation_prematureend():
    config = load_configfile("base_config_simulator.conf", "reservation_prematureend.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_migrate():
    config = load_configfile("base_config_simulator.conf", "migrate.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_reuse1():
    config = load_configfile("base_config_simulator.conf", "reuse1.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_reuse2():
    config = load_configfile("base_config_simulator.conf", "reuse2.lwf")
    rm = ResourceManager(config)
    rm.start()
    
def test_wait():
    config = load_configfile("base_config_simulator.conf", "wait.lwf")
    rm = ResourceManager(config)
    rm.start()