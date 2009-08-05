from haizea.core.leases import Lease, Capacity
from haizea.core.scheduler.resourcepool import ResourcePoolNode
from haizea.core.scheduler.slottable import ResourceTuple, SlotTable, ResourceReservation, AvailabilityWindow
from mx import DateTime
import haizea.common.constants as constants
from common import create_ar_lease,  create_reservation_from_lease

T1200 = DateTime.DateTime(2006,11,25,12,00)
T1255 = DateTime.DateTime(2006,11,25,12,55)
T1300 = DateTime.DateTime(2006,11,25,13,00)
T1305 = DateTime.DateTime(2006,11,25,13,05)
T1315 = DateTime.DateTime(2006,11,25,13,15)
T1325 = DateTime.DateTime(2006,11,25,13,25)
T1330 = DateTime.DateTime(2006,11,25,13,30)
T1335 = DateTime.DateTime(2006,11,25,13,35)
T1345 = DateTime.DateTime(2006,11,25,13,45)
T1350 = DateTime.DateTime(2006,11,25,13,50)
T1355 = DateTime.DateTime(2006,11,25,13,55)
T1400 = DateTime.DateTime(2006,11,25,14,00)
T1415 = DateTime.DateTime(2006,11,25,14,15)
T1420 = DateTime.DateTime(2006,11,25,14,20)

resource_types_with_max_instances = [(constants.RES_CPU,1),(constants.RES_MEM,1)]

def create_capacities(slottable):
    FULL_NODE = Capacity([constants.RES_CPU,constants.RES_MEM])
    FULL_NODE.set_quantity(constants.RES_CPU, 100)
    FULL_NODE.set_quantity(constants.RES_MEM, 1024)
    FULL_NODE = slottable.create_resource_tuple_from_capacity(FULL_NODE)
    
    HALF_NODE = Capacity([constants.RES_CPU,constants.RES_MEM])
    HALF_NODE.set_quantity(constants.RES_CPU, 50)
    HALF_NODE.set_quantity(constants.RES_MEM, 512)
    HALF_NODE = slottable.create_resource_tuple_from_capacity(HALF_NODE)

    QRTR_NODE = Capacity([constants.RES_CPU,constants.RES_MEM])
    QRTR_NODE.set_quantity(constants.RES_CPU, 25)
    QRTR_NODE.set_quantity(constants.RES_MEM, 256)
    QRTR_NODE = slottable.create_resource_tuple_from_capacity(QRTR_NODE)

    EMPT_NODE = slottable.create_empty_resource_tuple()
    
    return FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE
        
def sample_slottable_1():
    slottable = SlotTable([(constants.RES_CPU,ResourceTuple.SINGLE_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)])
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    slottable.add_node(1, FULL_NODE)
    slottable.add_node(2, FULL_NODE)  
    slottable.add_node(3, FULL_NODE)  
    slottable.add_node(4, FULL_NODE)  

    lease1 = Lease.create_new(None,{},None,None,None,1,None)
    lease1.id = 1
    res1 = {2: HALF_NODE}
    rr1_1 = ResourceReservation(lease1, T1315, T1325, res1)
    rr1_2 = ResourceReservation(lease1, T1325, T1330, res1)
    slottable.add_reservation(rr1_1)
    slottable.add_reservation(rr1_2)

    lease2 = Lease.create_new(None,{},None,None,None,2,None)
    lease2.id = 2
    res2 = {2: FULL_NODE, 3: FULL_NODE}
    rr2 = ResourceReservation(lease2, T1330, T1345, res2)
    slottable.add_reservation(rr2)

    lease3 = Lease.create_new(None,{},None,None,None,1,None)
    lease3.id = 3
    res3 = {4: FULL_NODE}
    rr3_1 = ResourceReservation(lease3, T1330, T1355, res3)
    rr3_2 = ResourceReservation(lease3, T1355, T1400, res3)
    slottable.add_reservation(rr3_1)
    slottable.add_reservation(rr3_2)

    lease4 = Lease.create_new(None,{},None,None,None,1,None)
    lease4.id = 4
    res4 = {2: QRTR_NODE, 3: HALF_NODE}
    rr4 = ResourceReservation(lease4, T1350, T1415, res4)
    slottable.add_reservation(rr4)

    lease5 = Lease.create_new(None,{},None,None,None,1,None)
    lease5.id = 5
    res5 = {2: QRTR_NODE}
    rr5 = ResourceReservation(lease5, T1350, T1415, res5)
    slottable.add_reservation(rr5)
    
    lease6 = Lease.create_new(None,{},None,None,None,1,None)
    lease6.id = 6
    res6 = {1: FULL_NODE}
    rr6 = ResourceReservation(lease6, T1255, T1305, res6)
    slottable.add_reservation(rr6)      
    
    return slottable, [lease1,lease2,lease3,lease4,lease5,lease6]
        
def sample_slottable_2():
    slottable = SlotTable([(constants.RES_CPU,ResourceTuple.SINGLE_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)])
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    slottable.add_node(1, FULL_NODE)
    slottable.add_node(2, FULL_NODE)  
    slottable.add_node(3, FULL_NODE)  
    slottable.add_node(4, FULL_NODE)  

    lease1 = create_ar_lease(lease_id = 1,
                             submit_time = T1200,
                             start = T1330,
                             end   = T1345,
                             preemptible = False,
                             requested_resources = {1: FULL_NODE, 2: FULL_NODE})
    create_reservation_from_lease(lease1, {1:1,2:2}, slottable)
    
    lease2 = create_ar_lease(lease_id = 2,
                             submit_time = T1200,
                             start = T1315,
                             end   = T1330,
                             preemptible = False,
                             requested_resources = {1: HALF_NODE})
    create_reservation_from_lease(lease2, {1:1}, slottable)

    return slottable, [lease1,lease2]

def sample_slottable_3():
    slottable = SlotTable([(constants.RES_CPU,ResourceTuple.SINGLE_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)])
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    slottable.add_node(1, FULL_NODE)
    slottable.add_node(2, FULL_NODE)  
    slottable.add_node(3, FULL_NODE)  
    slottable.add_node(4, FULL_NODE)  

    lease1 = create_ar_lease(lease_id = 1,
                             submit_time = T1200,
                             start = T1345,
                             end   = T1415,
                             preemptible = False,
                             requested_resources = {1: FULL_NODE})
    create_reservation_from_lease(lease1, {1:1}, slottable)

    lease2 = create_ar_lease(lease_id = 2,
                             submit_time = T1200,
                             start = T1330,
                             end   = T1415,
                             preemptible = False,
                             requested_resources = {1: HALF_NODE})
    create_reservation_from_lease(lease2, {1:2}, slottable)

    lease3 = create_ar_lease(lease_id = 3,
                             submit_time = T1200,
                             start = T1400,
                             end   = T1415,
                             preemptible = False,
                             requested_resources = {1: HALF_NODE})
    create_reservation_from_lease(lease3, {1:2}, slottable)

    return slottable, [lease1, lease2, lease3]

def sample_slottable_4():
    slottable = SlotTable([(constants.RES_CPU,ResourceTuple.SINGLE_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)])
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    slottable.add_node(1, FULL_NODE)

    lease1 = create_ar_lease(lease_id = 1,
                             submit_time = T1200,
                             start = T1315,
                             end   = T1420,
                             preemptible = False,
                             requested_resources = {1: HALF_NODE})
    create_reservation_from_lease(lease1, {1:1}, slottable)

    lease2 = create_ar_lease(lease_id = 2,
                             submit_time = T1200,
                             start = T1330,
                             end   = T1415,
                             preemptible = False,
                             requested_resources = {1: QRTR_NODE})
    create_reservation_from_lease(lease2, {1:1}, slottable)

    #lease3 = create_ar_lease(lease_id = 3,
    #                         submit_time = T1200,
    #                         start = T1345,
    #                         end   = T1400,
    #                         preemptible = False,
    #                         requested_resources = {1: QRTR_NODE})
    #create_reservation_from_lease(lease1, {1:1}, slottable)


    return slottable, [lease1, lease2, None]