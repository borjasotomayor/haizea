from common import BaseXMLRPCTest
from mx.DateTime import TimeDelta
import haizea.cli.rpc_commands as rpc
import time

class TestXMLRPC(BaseXMLRPCTest):
    def __init__(self):
        self.config = self.load_configfile("base_config_simulator.conf")
        self.config.set("simulation", "clock", "real")
        self.config.set("scheduling", "backfilling", "off")
        self.config.set("scheduling", "non-schedulable-interval", "2")
   
    def test_getters(self):
        self.start()
        cmd = rpc.haizea_list_hosts(["-D"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        cmd = rpc.haizea_show_queue(["-D"])
        cmd.run()
        self.stop()
        
    def test_ar(self):
        self.start()
        cmd = rpc.haizea_request_lease(["-D", "-t", "+00:01:00", "-d", "00:10:00", "-n", "1", "--non-preemptible", 
                                        "-c", "1", "-m", "512", "-i", "foobar.img", "-z", "600"])
        cmd.run()
        time.sleep(5)
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "1"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        self.stop()
    
    def test_be(self):
        self.start()
        cmd = rpc.haizea_request_lease(["-D", "-t", "best_effort", "-d", "00:10:00", "-n", "4", "--non-preemptible", 
                                        "-c", "1", "-m", "512", "-i", "foobar.img", "-z", "600"])
        cmd.run()
        cmd = rpc.haizea_request_lease(["-D", "-t", "best_effort", "-d", "00:10:00", "-n", "4", "--non-preemptible", 
                                        "-c", "1", "-m", "512", "-i", "foobar.img", "-z", "600"])
        cmd.run()
        time.sleep(7)
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        # Cancel the queued request
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "3"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        # Cancel the running request
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "2"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        self.stop()
    
    def test_im(self):
        self.start()
        cmd = rpc.haizea_request_lease(["-D", "-t", "now", "-d", "00:10:00", "-n", "1", "--non-preemptible", 
                                        "-c", "1", "-m", "512", "-i", "foobar.img", "-z", "600"])
        cmd.run()
        time.sleep(5)
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "4"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        self.stop()
