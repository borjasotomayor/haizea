import ConfigParser
import os
import threading
import shutil

from haizea.core.configfile import HaizeaConfig
from haizea.core.scheduler.slottable import ResourceReservation, SlotTable
from haizea.core.leases import Lease, Timestamp, Duration
from haizea.core.manager import Manager

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
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_preemption_prematureend(self):
        self.set_tracefile("preemption_prematureend.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_preemption_prematureend2(self):
        self.set_tracefile("preemption_prematureend2.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_reservation(self):
        self.set_tracefile("reservation.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_reservation_prematureend(self):
        self.set_tracefile("reservation_prematureend.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_migrate(self):
        self.set_tracefile("migrate.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_reuse1(self):
        self.set_tracefile("reuse1.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_reuse2(self):
        self.set_tracefile("reuse2.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
    def test_wait(self):
        self.set_tracefile("wait.lwf")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        
        
class BaseOpenNebulaTest(BaseTest):
    def __init__(self):
        pass

    def do_test(self, db):
        shutil.copyfile(db, "one.db")
        haizea = Manager(HaizeaConfig(self.config))
        haizea.start()
        os.remove("one.db")
    

class BaseXMLRPCTest(BaseTest):
    def __init__(self):
        self.haizea_thread = None

    def start(self):
        self.haizea = Manager(HaizeaConfig(self.config))
        self.haizea_thread = threading.Thread(target=self.haizea.start)
        self.haizea_thread.start()
        
    def stop(self):
        self.haizea.stop()
        self.haizea_thread.join()
        
        
def create_ar_lease(lease_id, submit_time, start, end, preemptible, requested_resources):
    start = Timestamp(start)
    duration = Duration(end - start.requested)
    lease = Lease.create_new(submit_time = submit_time, 
                  requested_resources = requested_resources, 
                  start = start, 
                  duration = duration,
                  deadline = None, 
                  preemptible = preemptible, 
                  software = None)
    
    lease.id = lease_id
    
    return lease

def create_reservation_from_lease(lease, mapping, slottable):
    start = lease.start.requested
    end = start + lease.duration.requested
    res = dict([(mapping[vnode],r) for vnode,r in lease.requested_resources.items()])
    rr = ResourceReservation(lease, start, end, res)
    slottable.add_reservation(rr)

def create_tmp_slottable(slottable):
    tmp_slottable = SlotTable(slottable.resource_types)
    tmp_slottable.nodes = slottable.nodes
    tmp_slottable.reservations_by_start = slottable.reservations_by_start[:]
    tmp_slottable.reservations_by_end = slottable.reservations_by_end[:]

    return tmp_slottable