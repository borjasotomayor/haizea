from common import BaseSimulatorTest

class TestSimulator(BaseSimulatorTest):
    def __init__(self):
        config = BaseSimulatorTest.load_configfile("base_config_simulator.conf")
        BaseSimulatorTest.__init__(self, config)
   
    def test_preemption(self):
        BaseSimulatorTest.test_preemption(self)
        self._verify_done([1,2]) 
        
    def test_preemption_prematureend(self):
        BaseSimulatorTest.test_preemption_prematureend(self)
        self._verify_done([1,2]) 
                
    def test_preemption_prematureend2(self):
        BaseSimulatorTest.test_preemption_prematureend2(self)
        self._verify_done([1,2]) 

    def test_reservation(self):
        BaseSimulatorTest.test_reservation(self)
        self._verify_done([1,2]) 
        
    def test_reservation_prematureend(self):
        BaseSimulatorTest.test_reservation_prematureend(self)
        self._verify_done([1,2]) 
        
    def test_migrate(self):
        BaseSimulatorTest.test_migrate(self)
        self._verify_done([1,3]) 
        
    def test_reuse1(self):
        BaseSimulatorTest.test_reuse1(self)
        self._verify_done([1,2]) 
        
    def test_reuse2(self):
        BaseSimulatorTest.test_reuse2(self)
        self._verify_done([1,2,3]) 
        
    def test_wait(self):
        BaseSimulatorTest.test_wait(self)

    def test_prematureend_in_start(self):
        self.config.set("scheduling", "override-suspend-time", "30")
        self._tracefile_test("prematureend_in_start.lwf")          