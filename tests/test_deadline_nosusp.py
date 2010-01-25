from common import BaseTest
from haizea.core.leases import Lease

class TestSimulator(BaseTest):
    def __init__(self):
        config = BaseTest.load_configfile("base_config_simulator.conf")
        BaseTest.__init__(self, config)
        self.config.set("scheduling", "mapper", "deadline")
        self.config.set("scheduling", "policy-preemption", "deadline")
        self.config.set("scheduling", "suspension", "none")
            
    def test_deadline1(self):
        self._tracefile_test("deadline1.lwf")
        self._verify_done([1,2])          
        
    def test_deadline2(self):
        self._tracefile_test("deadline2.lwf")
        self._verify_done([1,2])          
        
    def test_deadline3(self):
        self._tracefile_test("deadline3.lwf")
        self._verify_done([1,2])          
        
    def test_deadline4(self):
        self._tracefile_test("deadline4.lwf")
        self._verify_done([1])          
        self._verify_rejected([2])          
                
    def test_deadline5(self):
        self._tracefile_test("deadline5.lwf")
        self._verify_done([1,2])          

    def test_deadline6(self):
        self._tracefile_test("deadline6.lwf")
        self._verify_done([1,2,3])          
        
    def test_deadline7(self):
        self._tracefile_test("deadline7.lwf")
        self._verify_done([1,2,3])          
        
    def test_deadline8(self):
        self._tracefile_test("deadline8.lwf")
        self._verify_done([1,2])          
        self._verify_rejected([3])
       
    def test_deadline9(self):
        self._tracefile_test("deadline9.lwf")
        self._verify_done([1,2,3])      
        
    def test_deadline10_1(self):
        self._tracefile_test("deadline10-1.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_2(self):
        self._tracefile_test("deadline10-2.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_3(self):
        self._tracefile_test("deadline10-3.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_4(self):
        self._tracefile_test("deadline10-4.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_5(self):
        self._tracefile_test("deadline10-5.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_6(self):
        self._tracefile_test("deadline10-6.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_7(self):
        self._tracefile_test("deadline10-7.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_8(self):
        self._tracefile_test("deadline10-8.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_9(self):
        self._tracefile_test("deadline10-9.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_10(self):
        self._tracefile_test("deadline10-10.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_11(self):
        self._tracefile_test("deadline10-11.lwf")
        self._verify_done([1,2,3])

    def test_deadline10_12(self):
        self._tracefile_test("deadline10-12.lwf")
        self._verify_done([1,2,3])

    def test_deadline11_1(self):
        self._tracefile_test("deadline11-1.lwf")
        self._verify_done([1,2,3])

    def test_deadline11_2(self):
        self._tracefile_test("deadline11-2.lwf")
        self._verify_done([1,2,3])