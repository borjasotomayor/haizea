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

"""This module provides the base class for writing custom "mappers" and the
default greedy mapper used in Haizea. A mapper is a class with a single function
"map" that takes a set of requested resources (typically corresponding to
VMs) and maps them to physical nodes (if such a mapping exists).
"""

from haizea.common.utils import abstract
from haizea.core.scheduler.slottable import ResourceTuple, AvailabilityWindow
import haizea.common.constants as constants
import operator

# This dictionary provides a shorthand notation for any mappers
# included in this module (this shorthand notation can be used in
# the configuration file)
class_mappings = {"greedy": "haizea.core.scheduler.mapper.GreedyMapper"}

class Mapper(object):
    """Base class for mappers
    
    """
    
    def __init__(self, slottable, policy):
        """Constructor
        
        Arguments
        slottable -- A fully constructed SlotTable
        policy -- A fully constructed PolicyManager
        """
        self.slottable = slottable
        self.policy = policy
    
    
    def map(self, requested_resources, start, end, strictend, onlynodes = None):
        """The mapping function
        
        The mapping function takes a set of requested resources and maps
        them to physical resources (based on the availability 
        in the slot table) in a specified time interval. The mapper
        may return a mapping that only satisfies part of the specified
        time interval.
        
        Arguments:
        requested_resources -- A dictionary mapping lease nodes (integers) to
        ResourceTuples (representing the desired amount of resources for
        that lease node)
        start -- Starting time of the interval during which the resources
        are required
        end -- Ending time of the interval
        strictend -- If True, the only valid mappings are those that span
        the entire requested interval. If False, the mapper is allowed to
        return mappings that only span part of the interval (this reduced
        interval must always start at "start"; the earlier end time is
        returned as a return value)
        onlynodes -- List of physical nodes. Only look for a mapping in
        these nodes.
        
        Returns:
        mapping -- A dictionary mapping lease nodes to physical nodes
        maxend -- The end of the interval for which a mapping was found.
        As noted in argument "strictend", this return value might not
        be the same as "end"
        preempting -- Leases that would have to be preempted for the
        mapping to be valid.
        
        If no mapping is found, the three return values are set to None
        """
        abstract()


class GreedyMapper(Mapper):
    """Haizea's default greedy mapper
    
    Haizea uses a greedy algorithm to determine how VMs are mapped to
    physical resources at a specific point in time (determining that point
    in time, when using best-effort scheduling, is determined in the lease
    and VM scheduling classes). 
    
    The way the algorithm works is by, first, greedily ordering the
    physical nodes from "most desirable" to "least desirable". For example,
    a physical node with no leases scheduled on it in the future is preferable
    to one with leases (since this reduces the probability of having to
    preempt leases to obtain a mapping). This ordering, however, is done by the 
    policy engine (see the GreedyPolicy class in the host_selection module) so, 
    to be a truly greedy algorithm, this mapper must be used in conjunction with 
    the "greedy" host selection policy).
    
    Then, the algorithm traverses the list of nodes and tries to map as many
    lease nodes into each physical node before moving on to the next. If
    the list of physical nodes is exhausted without finding a mapping for all
    the lease nodes, then the algorithm tries to find a mapping by preempting
    other leases.
    
    Before doing this, the mapper must first determine what leases could be
    preempted. This decision is delegated to the policy engine, which returns
    a list of leases ordered from "most preemptable" to "least preemptable".
    The mapper attempts a mapping assuming that the first lease is going
    to be preempted, then assuming the first and the second, etc.
    
    If no mapping is found with preemption, then there is no mapping at the
    requested time.
    
    """
    
    def __init__(self, slottable, policy):
        """Constructor
        
        Arguments
        slottable -- A fully constructed SlotTable
        policy -- A fully constructed PolicyManager
        """        
        Mapper.__init__(self, slottable, policy)
        
    def map(self, lease, requested_resources, start, end, strictend, onlynodes=None):
        """The mapping function
        
        See documentation in Mapper for more details
        """        
        
        # Generate an availability window at time "start"
        aw = self.slottable.get_availability_window(start)

        nodes = aw.get_nodes_at(start)     
        if onlynodes != None:
            nodes = list(set(nodes) & onlynodes)

        # Get an ordered list of physical nodes
        pnodes = self.policy.sort_hosts(nodes, start, lease)
        
        # Get an ordered list of lease nodes
        vnodes = self.__sort_vnodes(requested_resources)
        
        # Get the leases that intersect with the requested interval.
        leases = aw.get_leases_until(end)
        # Ask the policy engine to sort the leases based on their
        # preemptability
        leases = self.policy.sort_leases(lease, leases, start)
        
        preemptable_leases = leases
        preempting = []
        
        # Try to find a mapping. Each iteration of this loop goes through
        # all the lease nodes and tries to find a mapping. The first
        # iteration assumes no leases can be preempted, and each successive
        # iteration assumes one more lease can be preempted.
        mapping = {}
        done = False
        while not done:
            # Start at the first lease node
            vnodes_pos = 0
            cur_vnode = vnodes[vnodes_pos]
            cur_vnode_capacity = requested_resources[cur_vnode]
            maxend = end 
            
            # Go through all the physical nodes.
            # In each iteration, we try to map as many lease nodes
            # as possible into the physical nodes.
            # "cur_vnode_capacity" holds the capacity of the vnode we are currently
            # trying to map. "need_to_map" is the amount of resources we are 
            # trying to map into the current physical node (which might be
            # more than one lease node).
            for pnode in pnodes:
                # need_to_map is initialized to the capacity of whatever
                # lease node we are trying to map now.
                need_to_map = self.slottable.create_empty_resource_tuple()
                need_to_map.incr(cur_vnode_capacity)
                avail=aw.get_availability_at_node(start, pnode, preempted_leases = preempting)
                
                # Try to fit as many lease nodes as we can into this physical node
                pnode_done = False
                while not pnode_done:
                    if avail.fits(need_to_map, until = maxend):
                        # In this case, we can fit "need_to_map" into the
                        # physical node.
                        mapping[cur_vnode] = pnode
                        vnodes_pos += 1
                        if vnodes_pos >= len(vnodes):
                            # No more lease nodes to map, we're done.
                            done = True
                            break
                        else:
                            # Advance to the next lease node, and add its
                            # capacity to need_to_map
                            cur_vnode = vnodes[vnodes_pos]
                            cur_vnode_capacity = requested_resources[cur_vnode]
                            need_to_map.incr(cur_vnode_capacity)
                    else:
                        # We couldn't fit the lease node. If we need to
                        # find a mapping that spans the entire requested
                        # interval, then we're done checking this physical node.
                        if strictend:
                            pnode_done = True
                        else:
                            # Otherwise, check what the longest interval
                            # we could fit in this physical node
                            latest = avail.latest_fit(need_to_map)
                            if latest == None:
                                pnode_done = True
                            else:
                                maxend = latest
                    
                if done:
                    break

            # If there's no more leases that we could preempt,
            # we're done.
            if len(preemptable_leases) == 0:
                done = True
            elif not done:
                # Otherwise, add another lease to the list of
                # leases we are preempting
                preempting.append(preemptable_leases.pop())

        if len(mapping) != len(requested_resources):
            # No mapping found
            return None, None, None
        else:
            return mapping, maxend, preempting

    def __sort_vnodes(self, requested_resources):
        """Sorts the lease nodes
        
        Greedily sorts the lease nodes so the mapping algorithm
        will first try to map those that require the highest
        capacity.
        """            
        
        # Find the maximum requested resources for each resource type
        max_res = self.slottable.create_empty_resource_tuple()
        for res in requested_resources.values():
            for i in range(len(res._res)):
                if res._res[i] > max_res._res[i]:
                    max_res._res[i] = res._res[i]
                    
        # Normalize the capacities of the lease nodes (divide each
        # requested amount of a resource type by the maximum amount)
        norm_res = {}
        for k,v in requested_resources.items():
            norm_capacity = 0
            for i in range(len(max_res._res)):
                if max_res._res[i] > 0:
                    norm_capacity += v._res[i] / float(max_res._res[i])
            norm_res[k] = norm_capacity
             
        vnodes = norm_res.items()
        vnodes.sort(key=operator.itemgetter(1), reverse = True)
        vnodes = [k for k,v in vnodes]
        return vnodes      
                    
