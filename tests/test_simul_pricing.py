from common import BaseTest
from haizea.core.leases import Lease

class TestSimulator(BaseTest):
    def __init__(self):
        config = BaseTest.load_configfile("base_config_simulator.conf")
        BaseTest.__init__(self, config)
        self.config.add_section("pricing")
            
    def test_pricing1(self):
        self.config.set("scheduling", "policy-pricing", "free")
        self._tracefile_test("price1.lwf")
        self._verify_done([1])

    def test_pricing2(self):
        self.config.set("scheduling", "policy-pricing", "constant")
        self.config.set("pricing", "rate", "0.10")        
        self._tracefile_test("price1.lwf")
        self._verify_done([1])
        
    def test_pricing3(self):
        self.config.set("scheduling", "policy-pricing", "constant")
        self.config.set("pricing", "rate", "1.00")        
        self._tracefile_test("price2.lwf")
        self._verify_rejected_by_user([1])
        
    def test_pricing_surcharge(self):
        self.config.set("scheduling", "policy-preemption", "deadline")
        self.config.set("scheduling", "suspension", "all")
        self.config.set("scheduling", "policy-pricing", "constant")
        self.config.set("pricing", "rate", "0.10")        
        self._tracefile_test("pricedeadline.lwf")
        self._verify_done([1])        