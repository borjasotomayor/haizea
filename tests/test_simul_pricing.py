from haizea.core.leases import Lease
from common import *

def get_config():
    c = load_configfile("base_config_simulator.conf")
    c.add_section("pricing")
    return c
   
def test_pricing1():
    c = get_config()
    c.set("scheduling", "policy-pricing", "free")
    h = load_tracefile(c, "price1.lwf")
    h.start()
    verify_done(h, [1])

def test_pricing2():
    c = get_config()
    c.set("scheduling", "policy-pricing", "constant")
    c.set("pricing", "rate", "0.10")        
    h = load_tracefile(c, "price1.lwf")
    h.start()
    verify_done(h, [1])
    
def test_pricing3():
    c = get_config()
    c.set("scheduling", "policy-pricing", "constant")
    c.set("pricing", "rate", "1.00")        
    h = load_tracefile(c, "price2.lwf")
    h.start()
    verify_rejected_by_user(h, [1])
    
def test_pricing_surcharge():
    c = get_config()
    c.set("scheduling", "mapper", "deadline")
    c.set("scheduling", "policy-preemption", "deadline")
    c.set("scheduling", "suspension", "all")
    c.set("scheduling", "policy-pricing", "constant")
    c.set("pricing", "rate", "0.10")        
    h = load_tracefile(c, "pricedeadline.lwf")
    h.start()
    verify_done(h, [1])        