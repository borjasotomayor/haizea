from haizea.core.leases import Capacity
from haizea.core.scheduler.slottable import ResourceTuple, SlotTable, ResourceReservation
from sample_slottables import *
import haizea.common.constants as constants

def test_resource_tuple():
    multiinst = [(constants.RES_CPU,ResourceTuple.MULTI_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)]
    
    slottable = SlotTable(multiinst)
            
    c1_100 = Capacity([constants.RES_CPU,constants.RES_MEM])
    c1_100.set_quantity(constants.RES_CPU, 100)
    c1_100.set_quantity(constants.RES_MEM, 1024)
    c1_100 = slottable.create_resource_tuple_from_capacity(c1_100)

    c2_100 = Capacity([constants.RES_CPU,constants.RES_MEM])
    c2_100.set_ninstances(constants.RES_CPU, 2)
    c2_100.set_quantity_instance(constants.RES_CPU, 1, 100)
    c2_100.set_quantity_instance(constants.RES_CPU, 2, 100)
    c2_100.set_quantity(constants.RES_MEM, 1024)
    c2_100 = slottable.create_resource_tuple_from_capacity(c2_100)

    c1_50 = Capacity([constants.RES_CPU,constants.RES_MEM])
    c1_50.set_quantity(constants.RES_CPU, 50)
    c1_50.set_quantity(constants.RES_MEM, 1024)
    c1_50 = slottable.create_resource_tuple_from_capacity(c1_50)

    c2_50 = Capacity([constants.RES_CPU,constants.RES_MEM])
    c2_50.set_ninstances(constants.RES_CPU, 2)
    c2_50.set_quantity_instance(constants.RES_CPU, 1, 50)
    c2_50.set_quantity_instance(constants.RES_CPU, 2, 50)
    c2_50.set_quantity(constants.RES_MEM, 1024)
    c2_50 = slottable.create_resource_tuple_from_capacity(c2_50)

    assert c1_100.fits_in(c2_100)
    assert not c1_100.fits_in(c1_50)
    assert not c1_100.fits_in(c2_50)

    assert not c2_100.fits_in(c1_100)
    assert not c2_100.fits_in(c1_50)
    assert not c2_100.fits_in(c2_50)

    assert c1_50.fits_in(c1_100)
    assert c1_50.fits_in(c2_100)
    assert c1_50.fits_in(c2_50)

    assert c2_50.fits_in(c1_100)
    assert c2_50.fits_in(c2_100)
    assert not c2_50.fits_in(c1_50)

    empty = slottable.create_empty_resource_tuple()
    empty.incr(c2_100)
    assert empty._single_instance[0] == 1024
    assert empty._multi_instance[1] == [100,100]
    
    empty = slottable.create_empty_resource_tuple()
    empty.incr(c1_100)
    assert empty._single_instance[0] == 1024
    assert empty._multi_instance[1] == [100]
    empty.incr(c1_100)
    assert empty._single_instance[0] == 2048
    assert empty._multi_instance[1] == [100,100]

    empty = slottable.create_empty_resource_tuple()
    empty.incr(c1_100)
    assert empty._single_instance[0] == 1024
    assert empty._multi_instance[1] == [100]
    empty.incr(c1_50)
    assert empty._single_instance[0] == 2048
    assert empty._multi_instance[1] == [100,50]

    c1_100a = ResourceTuple.copy(c1_100)
    c1_100a.decr(c1_50)
    assert c1_100a._single_instance[0] == 0
    assert c1_100a._multi_instance[1] == [50]

    c2_100a = ResourceTuple.copy(c2_100)
    c2_100a._single_instance[0] = 2048
    c2_100a.decr(c1_50)
    
    assert c2_100a._single_instance[0] == 1024
    assert c2_100a._multi_instance[1] == [50,100]
    c2_100a.decr(c1_50)
    assert c2_100a._single_instance[0] == 0
    assert c2_100a._multi_instance[1] == [0,100]

    c2_100a = ResourceTuple.copy(c2_100)
    c2_100a._single_instance[0] = 2048
    c2_100a.decr(c2_50)
    assert c2_100a._single_instance[0] == 1024
    assert c2_100a._multi_instance[1] == [0,100]
    c2_100a.decr(c2_50)
    assert c2_100a._single_instance[0] == 0
    assert c2_100a._multi_instance[1] == [0,0]

def test_slottable():
    def assert_capacity(node, percent):
        assert node.capacity.get_by_type(constants.RES_CPU) == percent * 100
        assert node.capacity.get_by_type(constants.RES_MEM) == percent * 1024
        
    def reservations_1_assertions():
        assert not slottable.is_empty()
        nodes = slottable.get_availability(T1300)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        nodes = slottable.get_availability(T1330)
        assert_capacity(nodes[1], 1.0)
        assert_capacity(nodes[2], 1.0)
        
    def reservations_2_assertions():
        nodes = slottable.get_availability(T1300)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1300)
        assert len(rrs) == 1
        assert rrs[0] == rr1

        nodes = slottable.get_availability(T1330)
        assert_capacity(nodes[1], 0.75)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1330)
        assert len(rrs) == 1
        assert rrs[0] == rr2

        nodes = slottable.get_availability(T1400)
        assert_capacity(nodes[1], 1.0)
        assert_capacity(nodes[2], 1.0)
        rrs = slottable.get_reservations_at(T1400)
        assert len(rrs) == 0
        
    def reservations_3_assertions():
        nodes = slottable.get_availability(T1300)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1300)
        assert len(rrs) == 1
        assert rrs[0] == rr1

        nodes = slottable.get_availability(T1315)
        assert_capacity(nodes[1], 0.25)
        assert_capacity(nodes[2], 0.25)
        rrs = slottable.get_reservations_at(T1315)
        assert len(rrs) == 2
        assert rr1 in rrs and rr3 in rrs

        nodes = slottable.get_availability(T1330)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.25)
        rrs = slottable.get_reservations_at(T1330)
        assert len(rrs) == 2
        assert rr2 in rrs and rr3 in rrs

        nodes = slottable.get_availability(T1345)
        assert_capacity(nodes[1], 0.75)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1345)
        assert len(rrs) == 1
        assert rrs[0] == rr2

        nodes = slottable.get_availability(T1400)
        assert_capacity(nodes[1], 1.0)
        assert_capacity(nodes[2], 1.0)
        rrs = slottable.get_reservations_at(T1400)
        assert len(rrs) == 0

    def reservations_4_assertions():
        nodes = slottable.get_availability(T1300)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1300)
        assert len(rrs) == 1
        assert rrs[0] == rr1

        nodes = slottable.get_availability(T1315)
        assert_capacity(nodes[1], 0.25)
        assert_capacity(nodes[2], 0.25)
        rrs = slottable.get_reservations_at(T1315)
        assert len(rrs) == 2
        assert rr1 in rrs and rr3 in rrs

        nodes = slottable.get_availability(T1330)
        assert_capacity(nodes[1], 0)
        assert_capacity(nodes[2], 0)
        rrs = slottable.get_reservations_at(T1330)
        assert len(rrs) == 3
        assert rr4 in rrs and rr2 in rrs and rr3 in rrs

        nodes = slottable.get_availability(T1345)
        assert_capacity(nodes[1], 0.25)
        assert_capacity(nodes[2], 0.25)
        rrs = slottable.get_reservations_at(T1345)
        assert len(rrs) == 2
        assert rr2 in rrs and rr4 in rrs

        nodes = slottable.get_availability(T1400)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.75)
        rrs = slottable.get_reservations_at(T1400)
        assert len(rrs) == 1
        assert rrs[0] == rr4

        nodes = slottable.get_availability(T1415)
        assert_capacity(nodes[1], 1.0)
        assert_capacity(nodes[2], 1.0)
        rrs = slottable.get_reservations_at(T1415)
        assert len(rrs) == 0
    
    def reservations_5_assertions():
        nodes = slottable.get_availability(T1300)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1300)
        assert len(rrs) == 1
        assert rrs[0] == rr1

        nodes = slottable.get_availability(T1315)
        assert_capacity(nodes[1], 0.25)
        assert_capacity(nodes[2], 0.25)
        rrs = slottable.get_reservations_at(T1315)
        assert len(rrs) == 2
        assert set(rrs) == set([rr1,rr3])

        nodes = slottable.get_availability(T1330)
        assert_capacity(nodes[1], 0)
        assert_capacity(nodes[2], 0)
        rrs = slottable.get_reservations_at(T1330)
        assert len(rrs) == 3
        assert set(rrs) == set([rr2,rr3,rr4])

        nodes = slottable.get_availability(T1345)
        assert_capacity(nodes[1], 0.25)
        assert_capacity(nodes[2], 0)
        rrs = slottable.get_reservations_at(T1345)
        assert len(rrs) == 3
        assert set(rrs) == set([rr2,rr4,rr5])

        nodes = slottable.get_availability(T1400)
        assert_capacity(nodes[1], 0.5)
        assert_capacity(nodes[2], 0.5)
        rrs = slottable.get_reservations_at(T1400)
        assert len(rrs) == 2
        assert set(rrs) == set([rr4,rr5])

        nodes = slottable.get_availability(T1415)
        assert_capacity(nodes[1], 1.0)
        assert_capacity(nodes[2], 1.0)
        rrs = slottable.get_reservations_at(T1415)
        assert len(rrs) == 0
        
        rrs = slottable.get_reservations_starting_between(T1300, T1315)
        assert set(rrs) == set([rr1,rr3])
        rrs = slottable.get_reservations_starting_between(T1300, T1330)
        assert set(rrs) == set([rr1,rr2,rr3,rr4])
        rrs = slottable.get_reservations_starting_between(T1300, T1345)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_starting_between(T1315, T1330)
        assert set(rrs) == set([rr2,rr3,rr4])
        rrs = slottable.get_reservations_starting_between(T1315, T1345)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_starting_between(T1330, T1345)
        assert set(rrs) == set([rr2,rr4,rr5])
        rrs = slottable.get_reservations_starting_between(T1400, T1415)
        assert len(rrs) == 0
        rrs = slottable.get_reservations_starting_between(T1305, T1335)
        assert set(rrs) == set([rr3,rr2,rr4])

        rrs = slottable.get_reservations_ending_between(T1300, T1305)
        assert len(rrs) == 0
        rrs = slottable.get_reservations_ending_between(T1300, T1315)
        assert len(rrs) == 0
        rrs = slottable.get_reservations_ending_between(T1300, T1330)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1300, T1335)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1300, T1345)
        assert set(rrs) == set([rr1,rr3])
        rrs = slottable.get_reservations_ending_between(T1300, T1400)
        assert set(rrs) == set([rr1,rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1300, T1415)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1305, T1315)
        assert len(rrs) == 0
        rrs = slottable.get_reservations_ending_between(T1305, T1330)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1305, T1335)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1305, T1345)
        assert set(rrs) == set([rr1,rr3])
        rrs = slottable.get_reservations_ending_between(T1305, T1400)
        assert set(rrs) == set([rr1,rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1305, T1415)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1315, T1330)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1315, T1335)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1315, T1345)
        assert set(rrs) == set([rr1,rr3])
        rrs = slottable.get_reservations_ending_between(T1315, T1400)
        assert set(rrs) == set([rr1,rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1315, T1415)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1330, T1335)
        assert set(rrs) == set([rr1])
        rrs = slottable.get_reservations_ending_between(T1330, T1345)
        assert set(rrs) == set([rr1,rr3])
        rrs = slottable.get_reservations_ending_between(T1330, T1400)
        assert set(rrs) == set([rr1,rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1330, T1415)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1335, T1345)
        assert set(rrs) == set([rr3])
        rrs = slottable.get_reservations_ending_between(T1335, T1400)
        assert set(rrs) == set([rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1335, T1415)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1345, T1400)
        assert set(rrs) == set([rr2,rr3])
        rrs = slottable.get_reservations_ending_between(T1345, T1415)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_between(T1400, T1415)
        assert set(rrs) == set([rr2,rr4,rr5])
        
        rrs = slottable.get_reservations_starting_on_or_after(T1300)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1305)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1315)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1330)
        assert set(rrs) == set([rr2,rr4,rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1335)
        assert set(rrs) == set([rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1345)
        assert set(rrs) == set([rr5])
        rrs = slottable.get_reservations_starting_on_or_after(T1400)
        assert len(rrs) == 0
        rrs = slottable.get_reservations_starting_on_or_after(T1415)
        assert len(rrs) == 0
        
        rrs = slottable.get_reservations_ending_on_or_after(T1300)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1305)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1315)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1330)
        assert set(rrs) == set([rr1,rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1335)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1345)
        assert set(rrs) == set([rr2,rr3,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1400)
        assert set(rrs) == set([rr2,rr4,rr5])
        rrs = slottable.get_reservations_ending_on_or_after(T1415)
        assert set(rrs) == set([rr4,rr5])
        
        assert slottable.get_next_changepoint(T1255) == T1300
        assert slottable.get_next_changepoint(T1300) == T1315
        assert slottable.get_next_changepoint(T1315) == T1330
        assert slottable.get_next_changepoint(T1330) == T1345
        assert slottable.get_next_changepoint(T1335) == T1345
        assert slottable.get_next_changepoint(T1345) == T1400
        assert slottable.get_next_changepoint(T1400) == T1415
        assert slottable.get_next_changepoint(T1415) == None
        assert slottable.get_next_changepoint(T1420) == None
    
    slottable = SlotTable([(constants.RES_CPU,ResourceTuple.SINGLE_INSTANCE),(constants.RES_MEM,ResourceTuple.SINGLE_INSTANCE)])
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    slottable.add_node(1, FULL_NODE)
    slottable.add_node(2, FULL_NODE)  
    
    assert slottable.get_total_capacity(constants.RES_CPU) == 200
    assert slottable.get_total_capacity(constants.RES_MEM) == 2048
    assert slottable.is_empty()

    res1 = {1: HALF_NODE, 2: HALF_NODE}
    rr1 = ResourceReservation(None, T1300, T1330, res1)
    slottable.add_reservation(rr1)
    reservations_1_assertions()

    res2 = {1: QRTR_NODE, 2: HALF_NODE}
    rr2 = ResourceReservation(None, T1330, T1400, res2)
    slottable.add_reservation(rr2)
    reservations_2_assertions()

    res3 = {1: QRTR_NODE, 2: QRTR_NODE}
    rr3 = ResourceReservation(None, T1315, T1345, res3)
    slottable.add_reservation(rr3)
    reservations_3_assertions()

    res4 = {1: HALF_NODE, 2: QRTR_NODE}
    rr4 = ResourceReservation(None, T1330, T1415, res4)
    slottable.add_reservation(rr4)
    reservations_4_assertions()

    res5 = {2: QRTR_NODE}
    rr5 = ResourceReservation(None, T1345, T1415, res5)
    slottable.add_reservation(rr5)
    reservations_5_assertions()

    slottable.remove_reservation(rr5)
    reservations_4_assertions()
    slottable.remove_reservation(rr4)
    reservations_3_assertions()
    slottable.remove_reservation(rr3)
    reservations_2_assertions()
    slottable.remove_reservation(rr2)
    reservations_1_assertions()
    slottable.remove_reservation(rr1)
    
    assert slottable.is_empty()
    
def test_availabilitywindow():
    def avail_node_assertions(time, avail, node_id, leases, next_cp):
        node = aw.changepoints[time].nodes[node_id]
        nleases = len(leases)
        assert(node.available == avail)
        
        assert(len(node.leases)==nleases)
        for l in leases:
            assert(l in node.leases)
        assert(len(node.available_if_preempting) == nleases)
        for l in leases:
            assert(node.available_if_preempting[l] == leases[l])
        assert(node.next_cp == next_cp)
        if next_cp != None:
            assert(node.next_nodeavail == aw.changepoints[next_cp].nodes[node_id])
    
    slottable, leases = sample_slottable_1()        
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    lease1,lease2,lease3,lease4,lease5,lease6 = leases
    
    aw = slottable.get_availability_window(T1300)
    
    # TODO: Factor out data into a data structure so we can do more
    # elaborate assertions
    
    # 13:00
    avail_node_assertions(time = T1300, avail = EMPT_NODE, node_id = 1, 
                          leases = {lease6:FULL_NODE}, next_cp = T1305)
    avail_node_assertions(time = T1300, avail = FULL_NODE, node_id = 2, 
                          leases = {}, next_cp = T1315)
    avail_node_assertions(time = T1300, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = T1330)
    avail_node_assertions(time = T1300, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = T1330)


    # 13:05
    avail_node_assertions(time = T1305, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1305, avail = FULL_NODE, node_id = 2, 
                          leases = {}, next_cp = T1315)
    avail_node_assertions(time = T1305, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = T1330)
    avail_node_assertions(time = T1305, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = T1330)

    # 13:15
    avail_node_assertions(time = T1315, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1315, avail = HALF_NODE, node_id = 2, 
                          leases = {lease1:HALF_NODE}, next_cp = T1330)
    avail_node_assertions(time = T1315, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = T1330)
    avail_node_assertions(time = T1315, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = T1330)

    # 13:25
    avail_node_assertions(time = T1325, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1325, avail = HALF_NODE, node_id = 2, 
                          leases = {lease1:HALF_NODE}, next_cp = T1330)
    avail_node_assertions(time = T1325, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = T1330)
    avail_node_assertions(time = T1325, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = T1330)

    # 13:30
    avail_node_assertions(time = T1330, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1330, avail = EMPT_NODE, node_id = 2, 
                          leases = {lease2:FULL_NODE}, next_cp = T1345)
    avail_node_assertions(time = T1330, avail = EMPT_NODE, node_id = 3, 
                          leases = {lease2:FULL_NODE}, next_cp = T1345)
    avail_node_assertions(time = T1330, avail = EMPT_NODE, node_id = 4, 
                          leases = {lease3:FULL_NODE}, next_cp = T1400)

    # 13:45
    avail_node_assertions(time = T1345, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1345, avail = FULL_NODE, node_id = 2, 
                          leases = {}, next_cp = T1350)
    avail_node_assertions(time = T1345, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = T1350)
    avail_node_assertions(time = T1345, avail = EMPT_NODE, node_id = 4, 
                          leases = {lease3:FULL_NODE}, next_cp = T1400)

    # 13:50
    avail_node_assertions(time = T1350, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1350, avail = HALF_NODE, node_id = 2, 
                          leases = {lease4:QRTR_NODE,lease5:QRTR_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1350, avail = HALF_NODE, node_id = 3, 
                          leases = {lease4:HALF_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1350, avail = EMPT_NODE, node_id = 4, 
                          leases = {lease3:FULL_NODE}, next_cp = T1400)

    # 13:55
    avail_node_assertions(time = T1355, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1355, avail = HALF_NODE, node_id = 2, 
                          leases = {lease4:QRTR_NODE,lease5:QRTR_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1355, avail = HALF_NODE, node_id = 3, 
                          leases = {lease4:HALF_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1355, avail = EMPT_NODE, node_id = 4, 
                          leases = {lease3:FULL_NODE}, next_cp = T1400)

    # 14:00
    avail_node_assertions(time = T1400, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1400, avail = HALF_NODE, node_id = 2, 
                          leases = {lease4:QRTR_NODE,lease5:QRTR_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1400, avail = HALF_NODE, node_id = 3, 
                          leases = {lease4:HALF_NODE}, next_cp = T1415)
    avail_node_assertions(time = T1400, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = None)

    # 14:15
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 2, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = None)
    
    avail = aw.get_ongoing_availability(T1300, 1)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == EMPT_NODE)
    assert(avail.avail_list[0].until     == None)

    avail = aw.get_ongoing_availability(T1300, 2)
    assert(len(avail.avail_list)==3)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1315)
    assert(avail.avail_list[1].available == HALF_NODE)
    assert(avail.avail_list[1].until     == T1330)
    assert(avail.avail_list[2].available == EMPT_NODE)
    assert(avail.avail_list[2].until     == None)

    avail = aw.get_ongoing_availability(T1300, 3)
    assert(len(avail.avail_list)==2)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1330)
    assert(avail.avail_list[1].available == EMPT_NODE)
    assert(avail.avail_list[1].until     == None)

    avail = aw.get_ongoing_availability(T1300, 4)
    assert(len(avail.avail_list)==2)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1330)
    assert(avail.avail_list[1].available == EMPT_NODE)
    assert(avail.avail_list[1].until     == None)


    avail = aw.get_ongoing_availability(T1330, 1)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None)

    avail = aw.get_ongoing_availability(T1330, 2)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == EMPT_NODE)
    assert(avail.avail_list[0].until     == None)

    avail = aw.get_ongoing_availability(T1330, 3)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == EMPT_NODE)
    assert(avail.avail_list[0].until     == None)

    avail = aw.get_ongoing_availability(T1330, 4)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == EMPT_NODE)
    assert(avail.avail_list[0].until     == None)
    
    
    avail = aw.get_ongoing_availability(T1345, 1)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None)

    avail = aw.get_ongoing_availability(T1345, 2)
    assert(len(avail.avail_list)==2)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1350)
    assert(avail.avail_list[1].available == HALF_NODE)
    assert(avail.avail_list[1].until     == None)

    avail = aw.get_ongoing_availability(T1345, 3)
    assert(len(avail.avail_list)==2)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1350)
    assert(avail.avail_list[1].available == HALF_NODE)
    assert(avail.avail_list[1].until     == None)

    avail = aw.get_ongoing_availability(T1345, 4)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == EMPT_NODE)
    assert(avail.avail_list[0].until     == None)        

    slottable.awcache = None
    aw = slottable.get_availability_window(T1415)
    # 14:15
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 2, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 3, 
                          leases = {}, next_cp = None)
    avail_node_assertions(time = T1415, avail = FULL_NODE, node_id = 4, 
                          leases = {}, next_cp = None)

    avail = aw.get_ongoing_availability(T1415, 1)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None)
    
    avail = aw.get_ongoing_availability(T1415, 2)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None)
    
    avail = aw.get_ongoing_availability(T1415, 3)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None)
    
    avail = aw.get_ongoing_availability(T1415, 4)
    assert(len(avail.avail_list)==1)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == None) 
    
    slottable, leases = sample_slottable_4()        
    FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(slottable)
    
    lease1,lease2,lease3 = leases
    aw = slottable.get_availability_window(T1300)
    
    # 13:30
    avail_node_assertions(time = T1300, avail = FULL_NODE, node_id = 1, 
                          leases = {}, next_cp = T1315)
    avail_node_assertions(time = T1315, avail = HALF_NODE, node_id = 1, 
                          leases = {lease1:HALF_NODE}, next_cp = T1330)
    avail_node_assertions(time = T1330, avail = QRTR_NODE, node_id = 1, 
                          leases = {lease1:HALF_NODE,lease2:QRTR_NODE}, next_cp = T1415)
    
    avail = aw.get_ongoing_availability(T1300, 1)
    assert(len(avail.avail_list)==3)
    assert(avail.avail_list[0].available == FULL_NODE)
    assert(avail.avail_list[0].until     == T1315)
    assert(avail.avail_list[1].available == HALF_NODE)
    assert(avail.avail_list[1].until     == T1330)
    assert(avail.avail_list[2].available == QRTR_NODE)
    assert(avail.avail_list[2].until     == None)
    
                