from haizea.core.scheduler.policy import PolicyManager
from haizea.core.scheduler.slottable import ResourceReservation
from haizea.core.scheduler.mapper import GreedyMapper
from haizea.pluggable.policies.host_selection import GreedyPolicy
from sample_slottables import *
from common import create_tmp_slottable

class SimplePolicy(PolicyManager):
    def __init__(self, slottable, preemption):
        PolicyManager.__init__(self, None, None, None, None)
        self.preemption = preemption
        self.host_selection = GreedyPolicy(slottable)
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        if self.preemption:
            return 1
        else:
            return -1
        
    def accept_lease(self, lease):
        return True  
    

class TestMapper(object):
    def __init__(self):
        pass
   
    def mapping_assertions(self, start, end, requested_resources, strictend, mustmap, 
                           maxend = None, can_preempt = []):
        lease = create_ar_lease(lease_id = 100,
                                submit_time = T1200,
                                start = start,
                                end = end,
                                preemptible = False,
                                requested_resources = requested_resources)
        
        mapping, actualend, preemptions = self.mapper.map(lease, requested_resources,
                                                     start, end, 
                                                     strictend = strictend,
                                                     allow_preemption = True)
        
        if mustmap:
            assert(mapping != None and actualend != None and preemptions != None)
            if strictend:
                assert(end == actualend)
            else:
                assert(actualend <= maxend)
            assert(set(preemptions).issubset(set(can_preempt)))

        else:
            assert(mapping == None and actualend == None and preemptions == None)
            return
        
        # Sanity check slottable
        tmp_slottable = create_tmp_slottable(self.slottable)
        
        # Add reservation
        res = dict([(mapping[vnode],r) for vnode,r in requested_resources.items()])
        rr = ResourceReservation(lease, start, actualend, res)
        tmp_slottable.add_reservation(rr)
        
        if len(preemptions) > 0:
            passed, node, time, capacity = tmp_slottable.sanity_check()
            assert(not passed)
            
            # Remove preempted reservations
            remove = set()
            for rr in [x.value for x in tmp_slottable.reservations_by_start]:
                if rr.lease in preemptions:
                    remove.add(rr)

            for rr in remove:
                tmp_slottable.remove_reservation(rr)

        passed, node, time, capacity = tmp_slottable.sanity_check()
        assert(passed)

   
    def test_mapping_nopreemption_strictend(self):
        self.slottable, leases = sample_slottable_2()
        FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(self.slottable)
        policy = SimplePolicy(self.slottable, preemption = False)
        self.mapper = GreedyMapper(self.slottable, policy)
        
        self.mapping_assertions(start = T1300, 
                                end = T1345,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1300, 
                                end = T1330,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: HALF_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1300, 
                                end = T1315,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1330, 
                                end = T1345,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1330, 
                                end = T1345,
                                requested_resources = {1: HALF_NODE, 2: HALF_NODE, 3: HALF_NODE, 4: HALF_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1300, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE},
                                strictend = True,
                                mustmap = True)

        self.mapping_assertions(start = T1300, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = False)

        self.mapping_assertions(start = T1300, 
                                end = T1330,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = False)

        self.mapping_assertions(start = T1330, 
                                end = T1345,
                                requested_resources = {1: HALF_NODE, 2: HALF_NODE, 3: HALF_NODE, 4: HALF_NODE, 5: HALF_NODE},
                                strictend = True,
                                mustmap = False)

    def test_mapping_nopreemption_nostrictend(self):
        self.slottable, leases = sample_slottable_3()
        FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(self.slottable)
        policy = SimplePolicy(self.slottable, preemption = False)
        self.mapper = GreedyMapper(self.slottable, policy)
        
        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: HALF_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1400)
        
        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1330)

        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: HALF_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1345)
        
        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: HALF_NODE, 4: HALF_NODE, 5: HALF_NODE, 6: HALF_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1330)

        self.mapping_assertions(start = T1330, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: HALF_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1345)

        self.mapping_assertions(start = T1345, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: HALF_NODE},
                                strictend = False,
                                mustmap = True,
                                maxend = T1400)

        self.mapping_assertions(start = T1330, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = False,
                                mustmap = False)
        
        self.mapping_assertions(start = T1400, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: HALF_NODE},
                                strictend = False,
                                mustmap = False)
        
    def test_mapping_preemption_strictend(self):
        self.slottable, leases = sample_slottable_3()
        FULL_NODE, HALF_NODE, QRTR_NODE, EMPT_NODE = create_capacities(self.slottable)
        policy = SimplePolicy(self.slottable, preemption = True)        
        self.mapper = GreedyMapper(self.slottable, policy)

        self.mapping_assertions(start = T1315, 
                                end = T1345,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = True,
                                can_preempt = [leases[1]])

        self.mapping_assertions(start = T1330, 
                                end = T1345,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = True,
                                can_preempt = [leases[1]])

        self.mapping_assertions(start = T1345, 
                                end = T1400,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE},
                                strictend = True,
                                mustmap = True,
                                can_preempt = [leases[0],leases[1]])
        
        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: FULL_NODE, 4: FULL_NODE},
                                strictend = True,
                                mustmap = True,
                                can_preempt = leases)
                
        self.mapping_assertions(start = T1315, 
                                end = T1415,
                                requested_resources = {1: FULL_NODE, 2: FULL_NODE, 3: HALF_NODE},
                                strictend = True,
                                mustmap = True,
                                can_preempt = leases)
