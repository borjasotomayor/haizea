from common import *

def get_config():
    c = load_configfile("base_config_simulator.conf")
    c.set("general", "lease-preparation", "imagetransfer")
    return c
   
def test_preemption():
    h = load_tracefile(get_config(), PREEMPTION_TRACE)
    h.start()
    
def test_preemption_prematureend():
    h = load_tracefile(get_config(), PREEMPTION_PREMATUREEND_TRACE)
    h.start()
        
def test_preemption_prematureend2():
    h = load_tracefile(get_config(), PREEMPTION_PREMATUREEND2_TRACE)
    h.start()
        
def test_reservation():
    h = load_tracefile(get_config(), RESERVATION_TRACE)
    h.start()
        
def test_reservation_prematureend():
    h = load_tracefile(get_config(), RESERVATION_PREMATUREEND_TRACE)
    h.start()
        
def test_migrate():
    h = load_tracefile(get_config(), MIGRATE_TRACE)
    h.start()
        
def test_reuse1():
    h = load_tracefile(get_config(), REUSE1_TRACE)
    h.start()
        
def test_reuse2():
    h = load_tracefile(get_config(), REUSE2_TRACE)
    h.start()

def test_piggybacking():
    h = load_tracefile(get_config(), PIGGYBACKING_TRACE)
    h.start()

def test_wait():
    h = load_tracefile(get_config(), WAIT_TRACE)
    h.start()
