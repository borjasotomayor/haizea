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

"""This module provides pluggable host selection policies. See the documentation
for haizea.core.schedule.policy.HostSelectionPolicy for more details on
host selection policies.
"""

from haizea.core.scheduler.policy import HostSelectionPolicy

class NoPolicy(HostSelectionPolicy):
    """A simple host selection policy: all hosts have the same score
    """
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """  
        HostSelectionPolicy.__init__(self, slottable)
    
    
    def get_host_score(self, node, time, lease):
        """Computes the score of a host
        
        See class documentation for details on what policy is implemented here.
        See documentation of HostSelectionPolicy.get_host_score for more details
        on this method.
        
        Arguments:
        node -- Physical node (the integer identifier used in the slot table)
        time -- Time at which the lease might be scheduled
        lease -- Lease that is being scheduled.
        """             
        return 1 
    
    

class GreedyPolicy(HostSelectionPolicy):
    """A greedy host selection policy.
    
    This policy scores hosts such that hosts with fewer leases already
    scheduled on them, with the highest capacity, and with fewest leases
    scheduled in the future are scored highest.
    
    """
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """  
        HostSelectionPolicy.__init__(self, slottable)
    
    def get_host_score(self, node, time, lease):
        """Computes the score of a host
        
        See class documentation for details on what policy is implemented here.
        See documentation of HostSelectionPolicy.get_host_score for more details
        on this method.
        
        Arguments:
        node -- Physical node (the integer identifier used in the slot table)
        time -- Time at which the lease might be scheduled
        lease -- Lease that is being scheduled.
        """                     
        aw = self.slottable.get_availability_window(time)

        leases_in_node_horizon = 4
        
        # 1st: We prefer nodes with fewer leases to preempt
        leases_in_node = len(aw.get_leases_at(node, time))
        if leases_in_node > leases_in_node_horizon:
            leases_in_node = leases_in_node_horizon
        
        # Nodes with fewer leases already scheduled in them get 
        # higher scores
        leases_in_node = (leases_in_node_horizon - leases_in_node) / float(leases_in_node_horizon)
        leases_in_node_score = leases_in_node


        # 2nd: we prefer nodes with the highest capacity
        avail = aw.get_availability(time, node)
        # TODO: normalize into a score
        high_capacity_score = 1.0
        
        # 3rd: we prefer nodes where the current capacity
        # doesn't change for the longest time.
        duration = aw.get_capacity_duration(node, time)
        if duration == None or duration>=lease.duration.requested:
            duration_score = 1.0
        else:
            duration_score = duration.seconds / float(lease.duration.requested.seconds)

        return 0.5 * leases_in_node_score + 0.25 * high_capacity_score + 0.25 * duration_score
      