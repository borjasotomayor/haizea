from common import BaseTest
from haizea.core.leases import Lease

class TestSimulator(BaseTest):
    def __init__(self):
        config = BaseTest.load_configfile("base_config_simulator.conf")
        BaseTest.__init__(self, config)
            
    def test_deadline1(self):
        self._tracefile_test("deadline1.lwf")
        self.__verify_done([1,2])          
        
    def test_deadline2(self):
        self._tracefile_test("deadline2.lwf")
        self.__verify_done([1,2])          
        
    def test_deadline3(self):
        self._tracefile_test("deadline3.lwf")
        self.__verify_done([1,2])          
        
    def test_deadline4(self):
        self._tracefile_test("deadline4.lwf")
        self.__verify_done([1])          
        self.__verify_rejected([2])          
                
    def test_deadline5(self):
        self._tracefile_test("deadline5.lwf")
        self.__verify_done([1,2,3])          
        
    def test_deadline6(self):
        self._tracefile_test("deadline6.lwf")
        self.__verify_done([1,2,3])          
        
    def test_deadline7(self):
        self._tracefile_test("deadline7.lwf")
        self.__verify_done([1,2,3])          
        
    def test_deadline8(self):
        self._tracefile_test("deadline1.lwf")
        self.__verify_done([1,2])          
        self.__verify_rejected([3])
                                