# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

"""This module provides pluggable lease preemption policies. See the documentation
for haizea.core.schedule.policy.PreemptabilityPolicy for more details on
lease preemption policies.
"""

from haizea.core.leases import Lease
from haizea.core.scheduler.policy import PreemptabilityPolicy


class NoPreemptionPolicy(PreemptabilityPolicy):
    """Simple preemption policy: preemption is never allowed.
    """
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        PreemptabilityPolicy.__init__(self, slottable)
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        """Computes the lease preemptability score
        
        See class documentation for details on what policy is implemented here.
        See documentation of PreemptabilityPolicy.get_lease_preemptability_score
        for more details on this function.
        
        Arguments:
        preemptor -- Preemptor lease
        preemptee -- Preemptee lease
        time -- Time at which preemption would take place
        """                    
        return -1

class ARPreemptsEverythingPolicy(PreemptabilityPolicy):
    """A simple preemption policy where AR leases can always preempt
    every other type of lease. Given two possible leases to preempt,
    the "youngest" one is preferred (i.e., the one that was most recently
    submitted).
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        PreemptabilityPolicy.__init__(self, slottable)
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        """Computes the lease preemptability score
        
        See class documentation for details on what policy is implemented here.
        See documentation of PreemptabilityPolicy.get_lease_preemptability_score
        for more details on this function.
        
        Arguments:
        preemptor -- Preemptor lease
        preemptee -- Preemptee lease
        time -- Time at which preemption would take place
        """        
        if preemptor.get_type() == Lease.ADVANCE_RESERVATION and preemptee.get_type() == Lease.BEST_EFFORT:
            return self._get_aging_factor(preemptee, time)
        else:
            return -1
        
class DeadlinePolicy(PreemptabilityPolicy):
    """Only leases that will still meet their deadline after preemption can
    be preempted. Furthermore, leases with the most slack time are preferred.
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        PreemptabilityPolicy.__init__(self, slottable)
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        """Computes the lease preemptability score
        
        See class documentation for details on what policy is implemented here.
        See documentation of PreemptabilityPolicy.get_lease_preemptability_score
        for more details on this function.
        
        Arguments:
        preemptor -- Preemptor lease
        preemptee -- Preemptee lease
        time -- Time at which preemption would take place
        """        
        if preemptee.get_type() == Lease.DEADLINE:
            deadline = preemptee.deadline
            vmrr = preemptee.get_last_vmrr()
            remaining_duration = vmrr.end - time
            slack = (deadline - time) / remaining_duration
            delay = preemptee.estimate_suspend_time() + preemptor.duration.requested + preemptee.estimate_resume_time()
            if time + delay + remaining_duration < deadline and not vmrr.is_suspending():
                return slack
            else:
                return -1
