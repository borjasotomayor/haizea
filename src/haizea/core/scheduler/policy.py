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

"""Haizea uses a policy manager that allows certain scheduling decisions to
be delegated to pluggable policies. This is done so scheduling policies
can be (1) modified without having to modify core components of Haizea, and
(2) implemented by writing a single Python class that implements a given
interface for pluggable policies.

Three policies are currently pluggable: lease preemptability ("Can lease X
preempt lease Y?"), host selection ("I want to deploy a VM, what host should
I select for this?") and lease admission ("Should I accept/reject this lease
request?"). Haizea provides several simple policy modules in the
haizea.policies package. The policy to use is selected in the configuration
file. See the Haizea Documentation for more details on how this is done.

This module provides Haizea's policy manager and the base classes for
pluggable policies.  
"""


from haizea.common.utils import abstract
from mx.DateTime import DateTimeDelta
import operator

class PolicyManager(object):
    """The Policy Manager
    
    This class manages the policy modules and provides methods to
    access these modules.
    
    """    
    def __init__(self, admission, preemption, host_selection):
        """Constructor
        
        Expects fully-constructed policies (these are currently
        loaded in the Manager class, based on the config file).
        
        Arguments:
        admission -- A child of LeaseAdmissionPolicy
        preemption -- A child of PreemptabilityPolicy
        host_selection -- A child of HostSelectionPolicy
        
        """
        self.admission = admission
        self.preemption = preemption
        self.host_selection = host_selection
    
    def sort_leases(self, preemptor, preemptees, time):
        """Sorts a list of leases by their preemptability
        
        Takes a list of leases (the "preemptees"), determines their preemptability
        by another lease (the "preemptor"), and returns a list with the
        leases sorted by decreasing preemptability score (most preemptable
        leases first)
        
        See documentation of PreemptabilityPolicy.get_lease_preemptability_score
        for more details on the preemptability score.
        
        Argument
        preemptor -- Preemptor lease
        preemptees -- List of preemptee leases
        time -- Time at which preemption would take place        
        """              
        leases_score = [(preemptee, self.get_lease_preemptability_score(preemptor,preemptee, time)) for preemptee in preemptees]
        leases_score = [(preemptee,score) for preemptee,score in leases_score if score != -1]
        leases_score.sort(key=operator.itemgetter(1), reverse=True)
        return [preemptee for preemptee,score in leases_score]


    def sort_hosts(self, nodes, time, lease):
        """Sorts a list of hosts by their score
        
        Takes a list of hosts, determines their score, and sorts them in
        order of decreasing score (most desireable hosts first)
        
        See documentation of HostSelectionPolicy.get_host_score for more details.
        
        Arguments:
        nodes -- List of physical node (the integer identifier used in the slot table)
        time -- Time at which the lease might be scheduled
        lease -- Lease that is being scheduled.
        """        
        nodes_score = [(node, self.get_host_score(node, time, lease)) for node in nodes]
        nodes_score.sort(key=operator.itemgetter(1), reverse=True)
        return [node for node,score in nodes_score]
    
    
    def accept_lease(self, lease):
        """Lease admission function
        
        Returns True if the lease can be accepted, False if it should be rejected.
        
        Argument
        lease -- Lease request
        """        
        return self.admission.accept_lease(lease)
    
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        """Computes the lease preemptability score
        
        See documentation of PreemptabilityPolicy.get_lease_preemptability_score
        for more details.
        
        Arguments:
        preemptor -- Preemptor lease
        preemptee -- Preemptee lease
        time -- Time at which preemption would take place
        """                
        return self.preemption.get_lease_preemptability_score(preemptor, preemptee, time)


    def get_host_score(self, node, time, lease):
        """Computes the score of a host
        
        See documentation of HostSelectionPolicy.get_host_score for more details.
        
        Arguments:
        node -- Physical node (the integer identifier used in the slot table)
        time -- Time at which the lease might be scheduled
        lease -- Lease that is being scheduled.
        """               
        return self.host_selection.get_host_score(node, time, lease)
    
    

class LeaseAdmissionPolicy(object):
    """Lease Admission policy
    
    This is the parent class of lease admission policies. A lease admission
    policy determines whether a given lease request should be accepted or not
    by Haizea. Note that this is distinct from whether the lease can be
    scheduled or not (although this could certainly be a part of the
    policy); the policy simply decides whether the lease can be considered for
    scheduling or not. For example, a user could submit an AR lease that must
    start in 5 hours, but the policy could dictate that all ARs must be notified
    at least 24 hours in advance (and the lease would be rejected, regardless of
    whether there was resources available for it in 5 hours). Similarly, an
    AR lease could be requested 48 hours in advance, be accepted by the lease
    admission policy, but then be rejected by the scheduler if there are no
    resources available.
    
    """       
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """
        self.slottable = slottable
    
    
    def accept_lease(self, lease):
        """Lease admission function
        
        Returns True if the lease can be accepted, False if it should be rejected.
        
        Argument
        lease -- Lease request
        """        
        abstract()
    
    
    
class PreemptabilityPolicy(object):
    """Lease Preemptability policy
    
    This is the parent class of lease preemptability policies. This type of
    policy is used to determine whether a lease can be preempted by another
    lease at a given time. However, the policy doesn't return True or False but,
    rather, a "preemptability score" (see get_lease_preemptability_score for
    more details)
    
    """           
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        self.slottable = slottable
    
    
    def get_lease_preemptability_score(self, preemptor, preemptee, time):
        """Computes the lease preemptability score
        
        Given a lease that needs to preempt resources (the "preemptor"),
        another lease (the "preemptee") that may be preempted by it, and a time,
        this method determines the preemptability score of the preemptee or
        "how preemptable is the preemptee by the preemptor at the given time".
        The score can be the following:
        
        -1 : Cannot be preempted under any circumstances
        0.0 <= x <= 1.0: Lease can be preempted. The higher the score,
        the "more preemptable" it is (this is a relative measure; the score
        should be used to determine which of several leases is a better
        candidate for preemption)
        
        Arguments:
        preemptor -- Preemptor lease
        preemptee -- Preemptee lease
        time -- Time at which preemption would take place
        """             
        abstract()    


    def _get_aging_factor(self, lease, time):
        """Returns an aging factor for the preemptability score
        
        This is a convenience function that can be used to "age" a
        preemptability score (allowing leases that have been submitted
        long ago to avoid preemption). The method returns a factor
        between 0 and 1 that can be multiplied by the score, reducing
        the score based on the lease's "age".
        
        Currently, this method uses a hard-coded horizon of 31 days
        (any lease older than 7 days cannot be preempted, and leases
        less than 7 days are assigned a factor proportional to their age)
        
        Arguments:
        lease -- Lease that is going to be preempted
        time -- Time at which preemption would take place        
        """            
        # TODO: Make horizon configurable
        horizon = time - DateTimeDelta(7)
        if lease.submit_time <= horizon:
            return -1
        else:
            seconds = (time - lease.submit_time).seconds
            horizon_seconds = DateTimeDelta(31).seconds
            return float(horizon_seconds - seconds) / horizon_seconds        
        
        
class HostSelectionPolicy(object):
    """Host Selection policy
    
    This is the parent class of host selection policies. When mapping VMs
    to physical hosts, this policy determines what hosts are more desireable.
    For example, an energy-saving policy might value hosts that already have
    VMs running (to leave as many empty machines as possible, which could then
    be turned off), whereas another policy might prefer empty hosts to make
    sure that VMs are spread out across nodes.
    
    To do this, the policy will assign a score to each host. See the documentation
    for get_host_score for more details.
        
    """             
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        self.slottable = slottable
    
    
    def get_host_score(self, node, time, lease):
        """Computes the score of a host
        
        Given a physical host, a time, and a lease we would like to
        schedule at that time, this method returns a score indicating
        how desireable that host is for that lease at that time.
        The score can be between 0.0 and 1.0. The higher the score,
        the "more desireable" the physical host is (this is a relative measure; 
        the score should be used to determine which of several physical hosts
        is more desireable for this lease).
        
        Arguments:
        node -- Physical node (the integer identifier used in the slot table)
        time -- Time at which the lease might be scheduled
        lease -- Lease that is being scheduled.
        """               
        abstract()    
