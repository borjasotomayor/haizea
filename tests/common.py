import ConfigParser
import threading

from haizea.core.configfile import HaizeaConfig
from haizea.core.scheduler.slottable import ResourceReservation, SlotTable
from haizea.core.leases import Lease, Timestamp, Duration
from haizea.core.manager import Manager

class BaseTest(object):
    def __init__(self, config):
        self.config = config
        self.haizea = None

    @staticmethod
    def load_configfile(configfile):
        f = open (configfile, "r")
        c = ConfigParser.ConfigParser()
        c.readfp(f)
        return c
    
    def _tracefile_test(self, tracefile):
        self.config.set("tracefile", "tracefile", tracefile)
        Manager.reset_singleton()
        self.haizea = Manager(HaizeaConfig(self.config))
        self.haizea.start()    


class BaseSimulatorTest(BaseTest):
    def __init__(self, config):
        BaseTest.__init__(self, config)

    def test_preemption(self):
        self._tracefile_test("preemption.lwf")
        
    def test_preemption_prematureend(self):
        self._tracefile_test("preemption_prematureend.lwf")
        
    def test_preemption_prematureend2(self):
        self._tracefile_test("preemption_prematureend2.lwf")

    def test_reservation(self):
        self._tracefile_test("reservation.lwf")
        
    def test_reservation_prematureend(self):
        self._tracefile_test("reservation_prematureend.lwf")
        
    def test_migrate(self):
        self._tracefile_test("migrate.lwf")
        
    def test_reuse1(self):
        self._tracefile_test("reuse1.lwf")
        
    def test_reuse2(self):
        self._tracefile_test("reuse2.lwf")
        
    def test_wait(self):
        self._tracefile_test("wait.lwf")

    

class BaseXMLRPCTest(BaseTest):
    def __init__(self, config):
        BaseTest.__init__(self, config)
        self.haizea = None
        self.haizea_thread = None

    def start(self):
        Manager.reset_singleton()
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