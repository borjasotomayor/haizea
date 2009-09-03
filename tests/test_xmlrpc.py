from common import BaseXMLRPCTest
from haizea.common.utils import reset_lease_id_counter
import haizea.cli.rpc_commands as rpc
import time

class TestXMLRPC(BaseXMLRPCTest):
    def __init__(self):
        config = BaseXMLRPCTest.load_configfile("base_config_simulator.conf")
        config = self.load_configfile("base_config_simulator.conf")
        config.set("simulation", "clock", "real")
        config.set("simulation", "resources", "4  CPU:100 Memory:1024")
        config.set("scheduling", "backfilling", "off")
        config.set("scheduling", "non-schedulable-interval", "2")
        BaseXMLRPCTest.__init__(self, config)
   
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
        reset_lease_id_counter()
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
        reset_lease_id_counter()        
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
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "2"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        # Cancel the running request
        cmd = rpc.haizea_cancel_lease(["-D", "-l", "1"])
        cmd.run()
        cmd = rpc.haizea_list_leases(["-D"])
        cmd.run()
        self.stop()
    
    def test_im(self):
        reset_lease_id_counter()  
        self.start()
        cmd = rpc.haizea_request_lease(["-D", "-t", "now", "-d", "00:10:00", "-n", "1", "--non-preemptible", 
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
