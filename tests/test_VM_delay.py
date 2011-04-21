from _pytest.python import raises
from sample_slottables import *
import haizea.common.constants as constants
from haizea.core.scheduler.vm_scheduler import *
from mx.DateTime import TimeDelta
from pytest import *


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

def setup_VMRR_delay():
    slottable, lease = sample_slottable_5()
    lease = lease[0]
    scheduler = VMScheduler(slottable, None, None, 0)
    vmrr = create_VMreservation_from_lease_with_shutdown(lease,{1:1}, slottable)
    return slottable,lease,scheduler,vmrr


def test_delay_VMRR_configurations():
    slottable,lease,scheduler,vmrr = setup_VMRR_delay()
    one_minute = TimeDelta(0,1)
    old_start, old_end = vmrr.start, vmrr.end
    def maxdelaystart_bigger_than_maxdelay():
        with raises (Exception):
            scheduler._delay_vmrr_to(old_start + one_minute, vmrr, True, 90 , 80, constants.DELAY_CANCEL)
    def maxdelay_bigger_than_100P():
        with raises(Exception):
            scheduler._delay_vmrr_to(old_end + one_minute,vmrr,True,80,101,constants.DELAY_CANCEL)
    def maxdelay_is_0_act_like_100():
        pass
    def maxdelaystart_is_0_then_be_maxdelay():
        pass

    maxdelaystart_bigger_than_maxdelay()
    maxdelay_bigger_than_100P()


def test_delay_VM_Shutdown_end():
    slottable, lease = sample_slottable_5()
    lease = lease[0]
    rr = create_reservation_from_lease(lease,{1:1},slottable)
    # Delay one minute
    one_minute = TimeDelta(0,1,0)
    scheduler = VMScheduler(slottable,None,None,0)
    old_end = rr.end
    scheduler._delay_rr_to(rr.start + one_minute ,rr)
    assert rr.end == old_end + one_minute
    assert len(slottable.get_reservations_ending_at(old_end + one_minute)) == 1
    assert len(slottable.get_reservations_ending_at(old_end)) == 0



def test_delay_VMRR_not_delayed_before():
    slottable,lease,scheduler,vmrr = setup_VMRR_delay()
    old_start, old_end = vmrr.start, vmrr.end
    one_minute = TimeDelta(0,1)
    action, delayTime = scheduler._delay_vmrr_to(old_start + one_minute, vmrr, False,50,80,constants.DELAY_CANCEL)
    assert action == constants.DELAY_STARTVM
    assert vmrr.end == old_end
    assert vmrr.start == old_start + one_minute
    assert delayTime == 0
    assert vmrr.percent_delayed == int(one_minute)*100.0/int(old_end - old_start)

def test_delay_VMRR_0S():
    slottable,lease,scheduler,vmrr = setup_VMRR_delay()
    old_start,old_end = vmrr.start,vmrr.end
    action, delayTime = scheduler._delay_vmrr_to(vmrr.start, vmrr, False, 50,80,constants.DELAY_CANCEL)
    assert vmrr.start == old_start
    assert vmrr.end == old_end
    assert action == constants.DELAY_STARTVM
    assert delayTime == 0


def test_VM_delay_start_and_end():
    slottable,lease,scheduler,vmrr = setup_VMRR_delay()
    one_minute = TimeDelta(0,1)
    old_start, old_end = vmrr.start, vmrr.end
    action, delayTime = scheduler._delay_vmrr_to(old_start + one_minute, vmrr,False, 0, 80, constants.DELAY_CANCEL)
    assert action == constants.DELAY_STARTVM
    assert vmrr.end == old_end + one_minute
    assert vmrr.start == old_start + one_minute
    assert delayTime == one_minute


def test_VMRR_delay_is_negative():
    pass

def test_VMRR_only_delay_end_percent_needed():
    slottable,lease,scheduler,vmrr = setup_VMRR_delay()
    old_start, old_end = vmrr.start, vmrr.end
    M3 = TimeDelta(0,3)
    action, delayTime = scheduler._delay_vmrr_to(old_start + M3, vmrr, False, 6,50, constants.DELAY_CANCEL)
    delay_end = M3 - (old_end-old_start)*0.06
    delay_end = TimeDelta(delay_end.hour, delay_end.minute, int(delay_end.second))
    assert vmrr.end == old_end + delay_end
    assert delayTime == delay_end
    assert action == constants.DELAY_STARTVM


