from sample_slottables import *
import haizea.common.constants as constants
from haizea.core.scheduler.vm_scheduler import *
from mx.DateTime import TimeDelta


def create_VMreservation_from_lease_with_shutdown(lease, mapping, slottable):
    start = lease.start.requested
    end = start + lease.duration.requested
    res = dict([(mapping[vnode],r) for vnode,r in lease.requested_resources.items()])
    vmrr = VMResourceReservation(lease, start, end, res)

    shutdown_time = vmrr.lease.estimate_shutdown_time()

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
    slottable.add_reservartion(shutdown_rr)


def test_delay_VM_Shutdown_end():
    slottable, lease = sample_slottable_5()
    lease = lease[0]
    rr = create_reservation_from_lease(lease,{1:1},slottable)
    # Delay one minute
    one_minute = TimeDelta(0,1,0)
    scheduler = VMScheduler(slottable,None,None,0)
    old_end = rr.end
    scheduler.delay_rr_to(rr.start + one_minute ,rr)
    assert rr.end == old_end + one_minute
    assert len(slottable.get_reservations_ending_at(old_end + one_minute)) == 1
    assert len(slottable.get_reservations_ending_at(old_end)) == 0


