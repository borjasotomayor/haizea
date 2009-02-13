import ConfigParser
import os
import shutil

from haizea.resourcemanager.configfile import HaizeaConfig
from haizea.resourcemanager.rm import ResourceManager

class BaseTest(object):
    def __init__(self):
        pass

    def load_configfile(self, configfile):
        file = open (configfile, "r")
        c = ConfigParser.ConfigParser()
        c.readfp(file)
        return c


class BaseSimulatorTest(BaseTest):
    def __init__(self):
        pass

    def set_tracefile(self, tracefile):
        self.config.set("tracefile", "tracefile", tracefile)

    def test_preemption(self):
        self.set_tracefile("preemption.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_preemption_prematureend(self):
        self.set_tracefile("preemption_prematureend.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_preemption_prematureend2(self):
        self.set_tracefile("preemption_prematureend2.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_reservation(self):
        self.set_tracefile("reservation.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_reservation_prematureend(self):
        self.set_tracefile("reservation_prematureend.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_migrate(self):
        self.set_tracefile("migrate.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_reuse1(self):
        self.set_tracefile("reuse1.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_reuse2(self):
        self.set_tracefile("reuse2.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
    def test_wait(self):
        self.set_tracefile("wait.lwf")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        
        
class BaseOpenNebulaTest(BaseTest):
    def __init__(self):
        pass

    def do_test(self, db):
        shutil.copyfile(db, "one.db")
        rm = ResourceManager(HaizeaConfig(self.config))
        rm.start()
        os.remove("one.db")

