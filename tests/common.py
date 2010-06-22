import ConfigParser
import threading

from haizea.core.configfile import HaizeaConfig
from haizea.core.scheduler.slottable import ResourceReservation, SlotTable
from haizea.core.leases import Lease, Timestamp, Duration
from haizea.core.manager import Manager
from haizea.common.utils import reset_lease_id_counter

PREEMPTION_TRACE = "preemption.lwf"
PREEMPTION_PREMATUREEND_TRACE = "preemption_prematureend.lwf"
PREEMPTION_PREMATUREEND2_TRACE = "preemption_prematureend2.lwf"
RESERVATION_TRACE = "reservation.lwf"
RESERVATION_PREMATUREEND_TRACE = "reservation_prematureend.lwf"
MIGRATE_TRACE = "migrate.lwf"
REUSE1_TRACE = "reuse1.lwf"
REUSE2_TRACE = "reuse2.lwf"
PIGGYBACKING_TRACE = "piggybacking.lwf"
WAIT_TRACE = "wait.lwf"

def load_configfile(configfile):
    f = open (configfile, "r")
    c = ConfigParser.ConfigParser()
    c.readfp(f)
    return c

def load_tracefile(config, tracefile):
    config.set("tracefile", "tracefile", tracefile)
    Manager.reset_singleton()
    reset_lease_id_counter()
    return Manager(HaizeaConfig(config))

def verify_done(haizea, ids):
    for id in ids:
        lease = haizea.scheduler.completed_leases.get_lease(id)
        
        assert lease.get_state() == Lease.STATE_DONE
        
        if lease.deadline != None:
            assert lease.end <= lease.deadline
            
        if lease.duration.known != None:
            duration = lease.duration.known
        else:
            duration = lease.duration.requested
            
        assert duration == lease.duration.actual

def verify_rejected(haizea, ids):
    for id in ids:
        lease = haizea.scheduler.completed_leases.get_lease(id)
        
        assert lease.get_state() == Lease.STATE_REJECTED
            
def verify_rejected_by_user(haizea, ids):
    for id in ids:
        lease = haizea.scheduler.completed_leases.get_lease(id)
        
        assert lease.get_state() == Lease.STATE_REJECTED_BY_USER
    
def create_haizea_thread(config):
    Manager.reset_singleton()
    haizea = Manager(HaizeaConfig(self.config))
    return haizea, threading.Thread(target=self.haizea.start)
        
#def stop(self):
#    self.haizea.stop()
#    self.haizea_thread.join()
        
        
def create_ar_lease(lease_id, submit_time, start, end, preemptible, requested_resources):
    start = Timestamp(start)
    duration = Duration(end - start.requested)
    lease = Lease.create_new(submit_time = submit_time, 
                  user_id = None,
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