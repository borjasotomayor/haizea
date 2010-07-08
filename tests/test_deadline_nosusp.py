from common import *
from haizea.core.leases import Lease

def get_config():
    c = load_configfile("base_config_simulator.conf")
    c.set("scheduling", "mapper", "deadline")
    c.set("scheduling", "policy-preemption", "deadline")
    c.set("scheduling", "suspension", "none")
    return c

def test_deadline1():
    c = get_config()
    h = load_tracefile(c, "deadline1.lwf")
    h.start()    
    verify_done(h, [1,2])          
    
def test_deadline2():
    c = get_config()
    h = load_tracefile(c, "deadline2.lwf")
    h.start()
    verify_done(h, [1,2])          
    
def test_deadline3():
    c = get_config()
    h = load_tracefile(c, "deadline3.lwf")
    h.start()
    verify_done(h, [1,2])          
    
def test_deadline4():
    c = get_config()
    h = load_tracefile(c, "deadline4.lwf")
    h.start()
    verify_done(h, [1])          
    verify_rejected(h, [2])          
            
def test_deadline5():
    c = get_config()
    h = load_tracefile(c, "deadline5.lwf")
    h.start()
    verify_done(h, [1,2])          

def test_deadline6():
    c = get_config()
    h = load_tracefile(c, "deadline6.lwf")
    h.start()
    verify_done(h, [1,2,3])          
    
def test_deadline7():
    c = get_config()
    h = load_tracefile(c, "deadline7.lwf")
    h.start()
    verify_done(h, [1,2,3])          
    
def test_deadline8():
    c = get_config()
    h = load_tracefile(c, "deadline8.lwf")
    h.start()
    verify_done(h, [1,2])          
    verify_rejected(h, [3])
                            
def test_deadline9_1():
    c = get_config()
    h = load_tracefile(c, "deadline9.lwf")
    h.start()
    verify_done(h, [1,2,3])
    
def test_deadline9_2():
    c = get_config()    
    h = load_tracefile(c, "deadline9.lwf")
    h.start()       
    verify_done(h, [1,2,3])      
    
def test_deadline10_1():
    c = get_config() 
    h = load_tracefile(c, "deadline10-1.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_2():
    c = get_config()   
    h = load_tracefile(c, "deadline10-2.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_3():
    c = get_config()   
    h = load_tracefile(c, "deadline10-3.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_4():
    c = get_config()
    h = load_tracefile(c, "deadline10-4.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_5():
    c = get_config()
    h = load_tracefile(c, "deadline10-5.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_6():
    c = get_config()  
    h = load_tracefile(c, "deadline10-6.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_7():
    c = get_config() 
    h = load_tracefile(c, "deadline10-7.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_8():
    c = get_config()   
    h = load_tracefile(c, "deadline10-8.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_9():
    c = get_config()
    h = load_tracefile(c, "deadline10-9.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_10():
    c = get_config()   
    h = load_tracefile(c, "deadline10-10.lwf")
    h.start()       
    verify_done([1,2,3])

def test_deadline10_11():
    c = get_config()  
    h = load_tracefile(c, "deadline10-11.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline10_12():
    c = get_config()    
    h = load_tracefile(c, "deadline10-12.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline11_1():
    c = get_config() 
    h = load_tracefile(c, "deadline11-1.lwf")
    h.start()       
    verify_done(h, [1,2,3])

def test_deadline11_2():
    c = get_config()
    h = load_tracefile(c, "deadline11-2.lwf")
    h.start()       
    verify_done(h, [1,2,3])
