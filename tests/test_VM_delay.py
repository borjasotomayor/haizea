from mx.DateTime import TimeDelta
from _pytest.python import raises
from sample_slottables import *
from common import *
import haizea.common.constants as constants
from haizea.core.scheduler.vm_scheduler import *
from haizea.core.log import *
from haizea.core.manager import Manager
from haizea.core.configfile import HaizeaConfig
from haizea.common.config import ConfigException
from haizea.core.scheduler.resourcepool import ResourcePool

# Done for not having problems when any part of the program call log.VDEBUG
logging.setLoggerClass(HaizeaLogger)

M1 = TimeDelta(0,1)
M2 = TimeDelta(0,2)
M3 = TimeDelta(0,3)
M4 = TimeDelta(0,4)

def create_VMreservation(lease, mapping, slottable):
    start = lease.start.requested
    end = start + lease.duration.requested
    res = dict([(mapping[vnode],r) for vnode,r in lease.requested_resources.items()])
    vmrr = VMResourceReservation(lease, start, end,{1:1}, res)

    return vmrr,res



def test_configuration_with_file():
    c = load_configfile("./base_config_simulator.conf")
    c.set("scheduling","max-delay-action","ALGO")
    with raises(ConfigException):
        Manager(HaizeaConfig(c))

def test_delay_VM_Shutdown():
    slottable, lease = sample_slottable_5()
    lease = lease[0]
    rr = create_reservation_from_lease(lease,{1:1},slottable)
    # Delay one minute
    scheduler = VMScheduler(slottable,None,None,0)
    old_end = rr.end
    scheduler._delay_rr_to(rr.start + M1 ,rr)
    assert rr.end == old_end + M1
    assert len(slottable.get_reservations_ending_at(old_end + M1)) == 1
    assert len(slottable.get_reservations_ending_at(old_end)) == 0

def test_time_delayed():
    time = TimeDelayed(M4,M1)
    assert time.duration == M3
    assert time.start == M4
    assert time.end == M1
    time.duration = M1
    assert time.start == M4
    assert time.duration == M1
    assert time.end == M4 - M1

class TestDelayVMRR():
    def setup_method(self,method):
        self.slottable, lease = sample_slottable_5()
        self.lease = lease[0]
        self.scheduler = VMScheduler(self.slottable, None, None, 0)
        self.vmrr,res = create_VMreservation(self.lease,{1:1}, self.slottable)
        self.slottable.add_reservation(self.vmrr)

    def test_only_start_with_default_options(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        action, delay_time = self.scheduler._delay_vmrr_to(old_start + M1, self.vmrr, False,50,80,constants.DELAY_CANCEL)
        assert action == constants.DELAY_STARTVM
        assert self.vmrr.end == old_end
        assert self.vmrr.start == old_start + M1
        assert delay_time.start == M1
        assert self.vmrr.time_delayed.start == M1
        assert self.vmrr.time_delayed.duration == TimeDelta(0,1)

    def test_not_delaying(self):
        old_start,old_end = self.vmrr.start,self.vmrr.end
        action, delay_time = self.scheduler._delay_vmrr_to(self.vmrr.start, self.vmrr, False, 50,80,constants.DELAY_CANCEL)
        assert self.vmrr.start == old_start
        assert self.vmrr.end == old_end
        assert action == constants.DELAY_STARTVM
        assert delay_time.start == TimeDelta()


    def test_start_and_end(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        action, delay_time = self.scheduler._delay_vmrr_to(old_start + M1, self.vmrr,False, 0, 80, constants.DELAY_CANCEL)
        assert action == constants.DELAY_STARTVM
        assert self.vmrr.end == old_end + M1
        assert self.vmrr.start == old_start + M1
        assert delay_time.start == M1
        assert delay_time.end == M1

    def test_if_delay_is_negative_then_raise_Exception(self):
        negative = TimeDelta(-1)
        with raises(Exception):
            self.scheduler._delay_vmrr_to(negative, self.vmrr, False)


    def test_only_delay_end_percent_needed(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        M3 = TimeDelta(0,3)
        action, delay_time = self.scheduler._delay_vmrr_to(old_start + M3, self.vmrr, False, 6,50, constants.DELAY_CANCEL)
        delay_end = M3 - (old_end-old_start)*0.06
        delay_end = TimeDelta(delay_end.hour, delay_end.minute, int(delay_end.second))
        assert self.vmrr.end == old_end + delay_end
        assert delay_time.start == M3
        assert delay_time.end == delay_end
        assert delay_time.duration == M3 - delay_end
        assert action == constants.DELAY_STARTVM

    def test_percent_should_be_after_been_delayed_before(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        self.vmrr.time_delayed.start = M1
        self.vmrr.time_delayed.duration = TimeDelta()
        action, delay_time = self.scheduler._delay_vmrr_to(old_start + M3, self.vmrr, False, 6,50, constants.DELAY_CANCEL)
        delay_end = M3 + M1 - (old_end-old_start)*0.06
        assert self.vmrr.end == old_end + delay_end
        assert delay_time.end == delay_end
        assert action == constants.DELAY_STARTVM
        assert self.vmrr.time_delayed.duration == M3 - delay_end
        assert self.vmrr.time_delayed.end == delay_end + M1
        assert delay_time.start == M3

    def test_simulate_works(self):
        old_start, old_end = self.vmrr.start,self.vmrr.end
        self.scheduler._delay_vmrr_to(old_start + M3,self.vmrr, True, 6, 50, constants.DELAY_CANCEL)
        assert self.vmrr.start == old_start
        assert self.vmrr.end == old_end

    def test_configurations(self):
        # TODO: Do it by the configuration file.
        old_start, old_end = self.vmrr.start, self.vmrr.end
        load_configfile("base_config_simulator.conf")
        def maxdelaystart_bigger_than_maxdelay():
            with raises (Exception):
                self.scheduler._delay_vmrr_to(old_start + M1, self.vmrr, True, 90 , 80, constants.DELAY_CANCEL)
        def maxdelay_bigger_than_100P():
            with raises(Exception):
                self.scheduler._delay_vmrr_to(old_end + M1,self.vmrr,True,80,101,constants.DELAY_CANCEL)
        def maxdelay_is_0_act_like_100():
            pass
        def maxdelaystart_is_0_then_be_maxdelay():
            pass
    
        maxdelaystart_bigger_than_maxdelay()
        maxdelay_bigger_than_100P()
        maxdelay_is_0_act_like_100()
        maxdelaystart_is_0_then_be_maxdelay()

    def test_if_more_than_maxdelay_then_cancel_vmrr(self):
        pass


class TestVMDelay:
    def setup_method(self,method):
        self.slottable, lease = sample_slottable_5()
        self.lease = lease[0]
        self.scheduler = VMScheduler(self.slottable, None, None, 0)
        self.vmrr,res = create_VMreservation(self.lease,{1:1}, self.slottable)
        pres_rr = ResourceReservation(self.lease,self.vmrr.start-M1,self.vmrr.end,res)
        post_rr = ResourceReservation(self.lease,self.vmrr.end,self.vmrr.end+M1,res)
        self.vmrr.update_start(self.vmrr.start - M1)
        self.vmrr.update_end(self.vmrr.end + M1)
        self.vmrr.pre_rrs.append(pres_rr)
        self.vmrr.post_rrs.append(post_rr)
        self.slottable.add_reservation(self.vmrr)
        self.slottable.add_reservation(pres_rr)
        self.slottable.add_reservation(post_rr)

    def test_delay(self):
        old_start = self.vmrr.get_first_start()
        old_vmrr_start = self.vmrr.start
        old_vmrr_end = self.vmrr.end
        old_end = self.vmrr.get_final_end()
        delay_end = M3 - (old_end-old_start)*0.06
        self.lease.state_machine.state = self.lease.STATE_READY
        self.scheduler._delay_vm_to(self.vmrr.get_first_start()+M3,self.vmrr,False,6,50,constants.DELAY_CANCEL)
        assert self.vmrr.get_first_start() == old_start + M3
        assert self.vmrr.start == old_vmrr_start+M3
        assert self.vmrr.end == old_vmrr_end + delay_end
        assert self.vmrr.get_final_end() + delay_end


class DummyResourcePool(object):
    def __init__(self):
        self.VM_delay_end = {}

    def add_delay_end_to_VM(self,vm,time):
        self.VM_delay_end[vm] = time
        vm.delayed = 0

    def verify_shutdown(self,vm):
        pass


class FTestFreeSpaceDelayingVM:
    '''
    In this test we just care if all the nodes are well fit, meaning that they
    don't have used more resource that the avaibles
    '''
    def check_node(self,node):
        assert node.used_resource < node.requested_resources

    def setup_method(self,method):
        pass

    # you can see the figures in the attached image,
    # where you can see in which situation you are in every test

    def add_VMS(self, slottable_VM):
        for VM_config in slottable_VM:
            vm = self.add_VM(VM_config[0], VM_config[1], VM_config[2])
            if VM_config[3] > 0: self.VM_delay_end(vm, VM_config[3])



    def test_case1(self):
        slottable_VM = [
            [start,end,resources,delay_end]

        ]
        self.add_VMS(slottable_VM)

