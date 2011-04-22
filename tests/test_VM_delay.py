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

# Done for not having problems when any part of the program call log.VDEBUG
logging.setLoggerClass(HaizeaLogger)

M1 = TimeDelta(0,1)
M3 = TimeDelta(0,3)

def create_VMreservation_from_lease_with_shutdown(lease, mapping, slottable):
    start = lease.start.requested
    end = start + lease.duration.requested
    res = dict([(mapping[vnode],r) for vnode,r in lease.requested_resources.items()])
    vmrr = VMResourceReservation(lease, start, end,{1:1}, res)

    shutdown_time = TimeDelta(0,0,30)

    start = vmrr.end - shutdown_time
    end = vmrr.end

    shutdown_rr = ShutdownResourceReservation(vmrr.lease, start, end, vmrr.resources_in_pnode, vmrr.nodes, vmrr)
    shutdown_rr.state = ResourceReservation.STATE_SCHEDULED

    vmrr.update_end(start)

    # If there are any post RRs, remove them
    for rr in vmrr.post_rrs:
        slottable.remove_reservation(rr)
    vmrr.post_rrs = []

    vmrr.post_rrs.append(shutdown_rr)
    slottable.add_reservation(vmrr)
    slottable.add_reservation(shutdown_rr)
    return vmrr



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


class TestDelayVMRR():
    def setup_method(self,method):
        self.slottable, lease = sample_slottable_5()
        self.lease = lease[0]
        self.scheduler = VMScheduler(self.slottable, None, None, 0)
        self.vmrr = create_VMreservation_from_lease_with_shutdown(self.lease,{1:1}, self.slottable)

    def test_only_start_with_default_options(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        action, delayTime = self.scheduler._delay_vmrr_to(old_start + M1, self.vmrr, False,50,80,constants.DELAY_CANCEL)
        assert action == constants.DELAY_STARTVM
        assert self.vmrr.end == old_end
        assert self.vmrr.start == old_start + M1
        assert delayTime == 0
        assert self.vmrr.percent_delayed == int(M1)*100.0/int(old_end - old_start)

    def test_not_delaying(self):
        old_start,old_end = self.vmrr.start,self.vmrr.end
        action, delayTime = self.scheduler._delay_vmrr_to(self.vmrr.start, self.vmrr, False, 50,80,constants.DELAY_CANCEL)
        assert self.vmrr.start == old_start
        assert self.vmrr.end == old_end
        assert action == constants.DELAY_STARTVM
        assert delayTime == 0


    def test_start_and_end(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        action, delayTime = self.scheduler._delay_vmrr_to(old_start + M1, self.vmrr,False, 0, 80, constants.DELAY_CANCEL)
        assert action == constants.DELAY_STARTVM
        assert self.vmrr.end == old_end + M1
        assert self.vmrr.start == old_start + M1
        assert delayTime == M1


    def test_if_delay_is_negative_then_raise_Exception(self):
        negative = TimeDelta(-1)
        with raises(Exception):
            self.scheduler._delay_vmrr_to(negative, self.vmrr, False)


    def test_only_delay_end_percent_needed(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        M3 = TimeDelta(0,3)
        action, delayTime = self.scheduler._delay_vmrr_to(old_start + M3, self.vmrr, False, 6,50, constants.DELAY_CANCEL)
        delay_end = M3 - (old_end-old_start)*0.06
        delay_end = TimeDelta(delay_end.hour, delay_end.minute, int(delay_end.second))
        assert self.vmrr.end == old_end + delay_end
        assert delayTime == delay_end
        assert action == constants.DELAY_STARTVM

    def test_percent_should_be_after_been_delayed_before(self):
        old_start, old_end = self.vmrr.start, self.vmrr.end
        self.vmrr.percent_delayed = 3
        action, delayTime = self.scheduler._delay_vmrr_to(old_start + M3, self.vmrr, False, 6,50, constants.DELAY_CANCEL)
        delay_end = M3 - (old_end-old_start)*0.03
        delay_end = TimeDelta(delay_end.hour, delay_end.minute, int(delay_end.second))
        assert self.vmrr.end == old_end + delay_end
        assert delayTime == delay_end
        assert action == constants.DELAY_STARTVM

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


class TestVMDelay:
    def setup_method(self,method):
        self.slottable, lease = sample_slottable_5()
        self.lease = lease[0]
        self.scheduler = VMScheduler(self.slottable, None, None, 0)
        self.vmrr = create_VMreservation_from_lease_with_shutdown(self.lease,{1:1}, self.slottable)

    def test_delay_VM_since_RR_to_final(self):
        def before_VMRR():
            pass
        def after_VMRR():
            pass
        def VMRR_itself():
            pass
        pass

