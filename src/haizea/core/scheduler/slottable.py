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

import haizea.common.constants as constants
from haizea.common.utils import xmlrpc_marshall_singlevalue
from math import floor
import bisect
import logging
from operator import attrgetter

"""This module provides an in-memory slot table data structure. 

A slot table is essentially just a collection of resource reservations.
See the documentation for ResourceReservation, SlotTable, and AvailabilityWindow
for additional implementation details.




"""


class ResourceTuple(object):
    """A resource tuple
    
    This class ...
    
    """    
    SINGLE_INSTANCE = 1
    MULTI_INSTANCE = 2
    
    def __init__(self, slottable, res):
        self.slottable = slottable
        self._res = res
        if self.slottable.has_multiinst:
            self.multiinst = dict([(i,[]) for i in range(self.slottable.rtuple_len, self.slottable.rtuple_nres)])

    def __getstate__(self):
        if self.slottable.has_multiinst:
            return (self._res, self.multiinst)
        else:
            return (self._res,)

    def __setstate__(self, state):
        self.slottable = None
        self._res = state[0]
        if len(state) == 2:
            self.multiinst = state[1]

    @classmethod
    def copy(cls, rt):
        rt2 = cls(rt.slottable, rt._res[:])
        if rt.slottable.has_multiinst:
            rt2.multiinst = dict([(i, l[:]) for (i,l) in rt.multiinst.items()])
        return rt2 
        
    def fits_in(self, res2):
        for i in xrange(self.slottable.rtuple_len):
            if self._res[i] > res2._res[i]:
                return False
        if self.slottable.has_multiinst:
            multiinst2 = dict([(i, l[:]) for (i,l) in res2.multiinst.items()])
            for (pos, l) in self.multiinst.items():
                insts = multiinst2[pos]
                for quantity in l:
                    fits = False
                    for i in range(len(insts)):
                        if quantity <= insts[i]:
                            fits = True
                            insts[i] -= quantity
                            break
                    if fits == False:
                        return False
        return True
    
    def any_less(self, res2):
        for i in xrange(self.slottable.rtuple_len):
            if self._res[i] < res2._res[i]:
                return True
        return False    
   
    def min(self, res2):
        for i in xrange(self.slottable.rtuple_len):
            self._res[i] = min(self._res[i], res2._res[i])
    
    def decr(self, res2):
        for slottype in xrange(self.slottable.rtuple_len):
            self._res[slottype] -= res2._res[slottype]
        if self.slottable.has_multiinst:
            for (pos, l) in res2.multiinst.items():
                insts = self.multiinst[pos]
                for quantity in l:
                    fits = False
                    for i in range(len(insts)):
                        if quantity <= insts[i]:
                            fits = True
                            insts[i] -= quantity
                            break
                    if fits == False:
                        raise Exception, "Can't decrease"
                    
    def incr(self, res2):
        for slottype in xrange(self.slottable.rtuple_len):
            self._res[slottype] += res2._res[slottype]
        if self.slottable.has_multiinst:
            for (pos, l) in res2.multiinst.items():
                self.multiinst[pos] += l[:]
        
    def get_by_type(self, restype):
        return self._res[self.slottable.rtuple_restype2pos[restype]]        
        
    def is_zero_or_less(self):
        return sum([v for v in self._res]) <= 0
    
    def __repr__(self):
        r=""
        for i, x in enumerate(self._res):
            r += "%s:%i " % (i, x)
        if self.slottable.has_multiinst:
            r+= `self.multiinst`
        return r

    def __eq__(self, res2):
        return self._res == res2._res

    def __cmp__(self, res2):
        return cmp(self._res, res2._res)

class ResourceReservation(object):
    """A resource reservation
    
    A resource reservation (or RR) is a data structure specifying that a certain 
    quantities of resources (represented as a ResourceTuple) are reserved across 
    several nodes (each node can have a different resource tuple; e.g., 1 CPU and 
    512 MB of memory in node 1 and 2 CPUs and 1024 MB of memory in node 2). An RR 
    has a specific start and end time for all the nodes. Thus, if some nodes are 
    reserved for an interval of time, and other nodes are reserved for a different 
    interval (even if these reservations are for the same lease), two separate RRs 
    would have to be added to the slot table.
    
    """    
    
    # Resource reservation states
    STATE_SCHEDULED = 0
    STATE_ACTIVE = 1
    STATE_DONE = 2

    # Mapping from state to a descriptive string
    state_str = {STATE_SCHEDULED : "Scheduled",
                 STATE_ACTIVE : "Active",
                 STATE_DONE : "Done"}
    
    def __init__(self, lease, start, end, res):
        self.lease = lease
        self.start = start
        self.end = end
        self.state = None
        self.resources_in_pnode = res # pnode -> ResourceTuple
                        
    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Start          : %s" % self.start)
        logger.log(loglevel, "End            : %s" % self.end)
        logger.log(loglevel, "State          : %s" % ResourceReservation.state_str[self.state])
        logger.log(loglevel, "Resources      : \n                         %s" % "\n                         ".join(["N%i: %s" %(i, x) for i, x in self.resources_in_pnode.items()])) 
                
    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        rr = {}                
        rr["start"] = xmlrpc_marshall_singlevalue(self.start)
        rr["end"] = xmlrpc_marshall_singlevalue(self.end)
        rr["state"] = self.state
        return rr

class Node(object):
    def __init__(self, capacity):
        self.capacity = ResourceTuple.copy(capacity)

        
class KeyValueWrapper(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        
    def __cmp__(self, other):
        return cmp(self.key, other.key)


class SlotTable(object):
    """Slot Table 
    
    The slot table is, by far, the most used data structure in Haizea, we need to
    optimize access to these reservations. In particular, we will often need to quickly
    access reservations starting or ending at a specific time (or in an interval of time).
    The current slot table implementation stores the RRs in two ordered lists: one
    by starting time and another by ending time. Access is done by binary search in O(log n)
    time. Insertion and removal require O(n) time, since lists are implemented internally
    as arrays in CPython. We could improve these times in the future by using a
    tree structure (which Python doesn't have natively, so we'd have to include
    our own tree implementation), although slot table accesses far outweight insertion
    and removal operations. The slot table is implemented with classes SlotTable,
    Node, NodeList, and KeyValueWrapper.
    
    """
    
    def __init__(self, resource_types):
        self.logger = logging.getLogger("SLOT")
        self.nodes = {}
        self.resource_types = resource_types
        self.reservations_by_start = []
        self.reservations_by_end = []
        self.__dirty()

        # Resource tuple fields
        res_singleinstance = [rt for rt,ninst in resource_types if ninst == ResourceTuple.SINGLE_INSTANCE]
        self.rtuple_len = len(res_singleinstance)
        self.rtuple_nres = len(resource_types)
        res_multiinstance = [(rt,ninst) for rt,ninst in resource_types if ninst == ResourceTuple.MULTI_INSTANCE]
        self.has_multiinst = len(res_multiinstance) > 0
        self.rtuple_restype2pos = dict([(rt,i) for (i,rt) in enumerate(res_singleinstance)])
        pos = self.rtuple_len
        for rt, ninst in res_multiinstance:
            self.rtuple_restype2pos[rt] = pos
            pos = pos + 1

    def add_node(self, node_id, resourcetuple):
        self.nodes[node_id] = Node(resourcetuple)

    def create_empty_resource_tuple(self):
        return ResourceTuple(self, [0] * self.rtuple_len)
    
    def create_resource_tuple_from_capacity(self, capacity):
        rt = ResourceTuple(self, [0] * self.rtuple_len)
        for restype in capacity.get_resource_types():
            pos = self.rtuple_restype2pos[restype]
            if pos < self.rtuple_len:
                rt._res[pos] = capacity.get_quantity(restype)
            else:
                ninst = capacity.ninstances[restype]
                for i in range(ninst):
                    rt.multiinst[pos].append(capacity.get_quantity_instance(restype, i))
                    
        return rt

    def is_empty(self):
        return (len(self.reservations_by_start) == 0)

    def is_full(self, time, restype):
        nodes = self.get_availability(time)
        avail = sum([node.capacity.get_by_type(restype) for node in nodes.values()])
        return (avail == 0)

    def get_total_capacity(self, restype):
        return sum([n.capacity.get_by_type(restype) for n in self.nodes.values()])        

    def get_reservations_at(self, time):
        item = KeyValueWrapper(time, None)
        startpos = bisect.bisect_right(self.reservations_by_start, item)
        bystart = set([x.value for x in self.reservations_by_start[:startpos]])
        endpos = bisect.bisect_right(self.reservations_by_end, item)
        byend = set([x.value for x in self.reservations_by_end[endpos:]])
        res = bystart & byend
        return list(res)
    
    def get_reservations_starting_between(self, start, end):
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_start, startitem)
        endpos = bisect.bisect_right(self.reservations_by_start, enditem)
        res = [x.value for x in self.reservations_by_start[startpos:endpos]]
        return res

    def get_reservations_starting_after(self, start):
        startitem = KeyValueWrapper(start, None)
        startpos = bisect.bisect_right(self.reservations_by_start, startitem)
        res = [x.value for x in self.reservations_by_start[startpos:]]
        return res

    def get_reservations_ending_after(self, end):
        startitem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_right(self.reservations_by_end, startitem)
        res = [x.value for x in self.reservations_by_end[startpos:]]
        return res

    def get_reservations_starting_on_or_after(self, start):
        startitem = KeyValueWrapper(start, None)
        startpos = bisect.bisect_left(self.reservations_by_start, startitem)
        res = [x.value for x in self.reservations_by_start[startpos:]]
        return res

    def get_reservations_ending_on_or_after(self, end):
        startitem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_end, startitem)
        res = [x.value for x in self.reservations_by_end[startpos:]]
        return res

    def get_reservations_ending_between(self, start, end):
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_end, startitem)
        endpos = bisect.bisect_right(self.reservations_by_end, enditem)
        res = [x.value for x in self.reservations_by_end[startpos:endpos]]
        return res
    
    def get_reservations_starting_at(self, time):
        return self.get_reservations_starting_between(time, time)

    def get_reservations_ending_at(self, time):
        return self.get_reservations_ending_between(time, time) 

    def get_reservations_after(self, time):
        bystart = set(self.get_reservations_starting_after(time))
        byend = set(self.get_reservations_ending_after(time))
        return list(bystart | byend)
    
    def get_reservations_on_or_after(self, time):
        bystart = set(self.get_reservations_starting_on_or_after(time))
        byend = set(self.get_reservations_ending_on_or_after(time))
        return list(bystart | byend)    

    def get_changepoints_after(self, after, until=None, nodes=None):
        changepoints = set()
        res = self.get_reservations_after(after)
        for rr in res:
            if nodes == None or (nodes != None and len(set(rr.resources_in_pnode.keys()) & set(nodes)) > 0):
                if rr.start > after:
                    changepoints.add(rr.start)
                if rr.end > after:
                    changepoints.add(rr.end)
        changepoints = list(changepoints)
        if until != None:
            changepoints =  [c for c in changepoints if c < until]
        changepoints.sort()
        return changepoints
    
    def add_reservation(self, rr):
        startitem = KeyValueWrapper(rr.start, rr)
        enditem = KeyValueWrapper(rr.end, rr)
        bisect.insort(self.reservations_by_start, startitem)
        bisect.insort(self.reservations_by_end, enditem)
        self.__dirty()

    # If the slot table keys are not modified (start / end time)
    # Just remove and reinsert.
    def update_reservation(self, rr):
        # TODO: Might be more efficient to resort lists
        self.remove_reservation(rr)
        self.add_reservation(rr)
        self.__dirty()

    # If the slot table keys are modified (start and/or end time)
    # provide the old keys (so we can remove it using
    # the m) and updated reservation
    def update_reservation_with_key_change(self, rr, old_start, old_end):
        # TODO: Might be more efficient to resort lists
        self.remove_reservation(rr, old_start, old_end)
        self.add_reservation(rr)
        self.__dirty()
        
    def remove_reservation(self, rr, start=None, end=None):
        if start == None:
            start = rr.start
        if end == None:
            end = rr.end
        posstart = self.__get_reservation_index(self.reservations_by_start, rr, start)
        posend = self.__get_reservation_index(self.reservations_by_end, rr, end)
        self.reservations_by_start.pop(posstart)
        self.reservations_by_end.pop(posend)
        self.__dirty()
        
    def get_availability(self, time, min_capacity=None):
        if not self.availabilitycache.has_key(time):
            self.__get_availability_cache_miss(time)
            # Cache miss
            
        nodes = self.availabilitycache[time]

        # Keep only those nodes with enough resources
        if min_capacity != None:
            newnodes = {}
            for n, node in nodes.items():
                if min_capacity.fits_in(node.capacity):
                    newnodes[n]=node
                else:
                    pass
            nodes = newnodes

        return nodes

    def get_next_reservations_in_nodes(self, time, nodes, rr_type=None, immediately_next = False):
        nodes = set(nodes)
        rrs_in_nodes = []
        earliest_end_time = {}
        rrs = self.get_reservations_starting_after(time)
        if rr_type != None:
            rrs = [rr for rr in rrs if isinstance(rr, rr_type)]
            
        # Filter the RRs by nodes
        for rr in rrs:
            rr_nodes = set(rr.resources_in_pnode.keys())
            if len(nodes & rr_nodes) > 0:
                rrs_in_nodes.append(rr)
                end = rr.end
                for n in rr_nodes:
                    if not earliest_end_time.has_key(n):
                        earliest_end_time[n] = end
                    else:
                        if end < earliest_end_time[n]:
                            earliest_end_time[n] = end
                            
        if immediately_next:
            # We only want to include the ones that are immediately
            # next. 
            rr_nodes_excl = set()
            for n in nodes:
                if earliest_end_time.has_key(n):
                    end = earliest_end_time[n]
                    rrs = [rr for rr in rrs_in_nodes if n in rr.resources_in_pnode.keys() and rr.start < end]
                    rr_nodes_excl.update(rrs)
            rrs_in_nodes = list(rr_nodes_excl)
        
        return rrs_in_nodes
    
    def get_next_changepoint(self, time):
        item = KeyValueWrapper(time, None)
        
        startpos = bisect.bisect_right(self.reservations_by_start, item)
        if startpos == len(self.reservations_by_start):
            time1 = None
        else:
            time1 = self.reservations_by_start[startpos].value.start
        
        endpos = bisect.bisect_right(self.reservations_by_end, item)
        if endpos == len(self.reservations_by_end):
            time2 = None
        else:
            time2 = self.reservations_by_end[endpos].value.end
        
        if time1==None and time2==None:
            return None
        elif time1==None:
            return time2
        elif time2==None:
            return time1
        else:
            return min(time1, time2)
        
    def get_availability_window(self, start):           
        if self.awcache == None or start < self.awcache_time or (start >= self.awcache_time and not self.awcache.changepoints.has_key(start)):
            self.__get_aw_cache_miss(start)
        return self.awcache

    def sanity_check(self):
        # Get checkpoints
        changepoints = set()
        for rr in [x.value for x in self.reservations_by_start]:
            changepoints.add(rr.start)
            changepoints.add(rr.end)
        changepoints = list(changepoints)
        changepoints.sort()
        
        offending_node = None
        offending_cp = None
        offending_capacity = None
        
        for cp in changepoints:
            avail = self.get_availability(cp)
            for node in avail:
                for resource in avail[node].capacity._res:
                    if resource < 0:
                        return False, node, cp, avail[node].capacity
                
        return True, None, None, None

    # ONLY for simulation
    def get_next_premature_end(self, after):
        from haizea.core.scheduler.vm_scheduler import VMResourceReservation
        # Inefficient, but ok since this query seldom happens
        res = [i.value for i in self.reservations_by_end if isinstance(i.value, VMResourceReservation) and i.value.prematureend > after]
        if len(res) > 0:
            prematureends = [r.prematureend for r in res]
            prematureends.sort()
            return prematureends[0]
        else:
            return None
    
    # ONLY for simulation
    def get_prematurely_ending_res(self, t):
        from haizea.core.scheduler.vm_scheduler import VMResourceReservation
        return [i.value for i in self.reservations_by_end if isinstance(i.value, VMResourceReservation) and i.value.prematureend == t]


    def __get_reservation_index(self, rlist, rr, key):
        item = KeyValueWrapper(key, None)
        pos = bisect.bisect_left(rlist, item)
        found = False
        while not found:
            if rlist[pos].value == rr:
                found = True
            else:
                pos += 1
        return pos
        
        
    def __get_availability_cache_miss(self, time):
        allnodes = set(self.nodes.keys())
        nodes = {} 
        reservations = self.get_reservations_at(time)

        # Find how much resources are available on each node
        for r in reservations:
            for node in r.resources_in_pnode:
                if not nodes.has_key(node):
                    n = self.nodes[node]
                    nodes[node] = Node(n.capacity)
                nodes[node].capacity.decr(r.resources_in_pnode[node])

        # For the remaining nodes, use a reference to the original node, not a copy
        missing = allnodes - set(nodes.keys())
        for node in missing:
            nodes[node] = self.nodes[node]                    
            
        self.availabilitycache[time] = nodes

    def __get_aw_cache_miss(self, time):
        self.awcache = AvailabilityWindow(self, time)
        self.awcache_time = time
        
    def __dirty(self):
        # You're a dirty, dirty slot table and you should be
        # ashamed of having outdated caches!
        self.availabilitycache = {}
        self.awcache_time = None
        self.awcache = None

    
    

        
class ChangepointAvail(object):
    def __init__(self):
        self.nodes = {}
        self.leases = set()
        
    def add_node(self, node, capacity):
        self.nodes[node] = ChangepointNodeAvail(capacity)

class ChangepointNodeAvail(object):
    def __init__(self, capacity):
        self.capacity = capacity     
        self.available = ResourceTuple.copy(capacity)
        self.leases = set()
        self.available_if_preempting = {}
        self.next_cp = None
        self.next_nodeavail = None

    def decr(self, capacity):
        self.available.decr(capacity)

    def add_lease(self, lease, capacity):
        if not lease in self.leases:
            self.leases.add(lease)
            self.available_if_preempting[lease] = ResourceTuple.copy(capacity)
        else:
            self.available_if_preempting[lease].incr(capacity)
        
    def get_avail_withpreemption(self, leases):
        avail = ResourceTuple.copy(self.capacity)
        for l in self.available_if_preempting:
            if not l in leases:
                avail.decr(self.available_if_preempting[l])
        return avail
        
class AvailEntry(object):
    def __init__(self, available, until):
        self.available = available
        self.until = until
    
class AvailabilityInNode(object):
    def __init__(self, avail_list):
        self.avail_list = avail_list
        
    def fits(self, capacity, until):
        for avail in self.avail_list:
            if avail.until == None or avail.until >= until:
                return capacity.fits_in(avail.available)

    def latest_fit(self, capacity):
        prev = None
        for avail in self.avail_list:
            if not capacity.fits_in(avail.available):
                return prev
            else:
                prev = avail.until

    def get_avail_at_end(self):
        return self.avail_list[-1]

class AvailabilityWindow(object):
    """An availability window
    
    A particularly important operation with the slot table is determining the
    "availability window" of resources starting at a given time. In a nutshell, 
    an availability window provides a convenient abstraction over the slot table, 
    with methods to answer questions like "If I want to start a least at time T, 
    are there enough resources available to start the lease?" "Will those resources 
    be available until time T+t?" "If not, what's the longest period of time those 
    resources will be available?"

    """
    def __init__(self, slottable, time):
        self.slottable = slottable
        self.logger = logging.getLogger("SLOTTABLE.WIN")
        self.time = time
        self.leases = set()

        self.cp_list = [self.time] + self.slottable.get_changepoints_after(time)

        # Create initial changepoint hash table
        self.changepoints = dict([(cp,ChangepointAvail()) for cp in self.cp_list])
 
        for cp in self.changepoints.values():
            for node_id, node in self.slottable.nodes.items():
                cp.add_node(node_id, node.capacity)
        
        rrs = self.slottable.get_reservations_after(time)
        rrs.sort(key=attrgetter("start"))
        pos = 0
        # Fill in rest of changepoint hash table
        
        for rr in rrs:
            # Ignore nil-duration reservations
            if rr.start == rr.end:
                continue
            
            while rr.start >= self.time and self.cp_list[pos] != rr.start:
                pos += 1
            lease = rr.lease

            self.leases.add(lease)
            
            if rr.start >= self.time:
                start_cp = self.changepoints[rr.start]
            else:
                start_cp = self.changepoints[self.time]

            start_cp.leases.add(lease)
            for node in rr.resources_in_pnode:
                start_cp.nodes[node].decr(rr.resources_in_pnode[node])
                start_cp.nodes[node].add_lease(lease, rr.resources_in_pnode[node])

            pos2 = pos + 1

            while self.cp_list[pos2] < rr.end:
                cp = self.changepoints[self.cp_list[pos2]]
                cp.leases.add(lease)
                for node in rr.resources_in_pnode:
                    cp.nodes[node].decr(rr.resources_in_pnode[node])
                    cp.nodes[node].add_lease(lease, rr.resources_in_pnode[node])
                    
                pos2 += 1
        
        prev_nodeavail = {}
        for node_id, node in self.changepoints[self.time].nodes.items():
            prev_nodeavail[node_id] = [node]
        
        # Link node entries
        for cp in self.cp_list[1:]:
            for node_id, node in self.changepoints[cp].nodes.items():
                prev_nodes = prev_nodeavail[node_id]
                if prev_nodes[-1].available == node.available and prev_nodes[-1].leases == node.leases:
                    prev_nodes.append(node)
                else:
                    for prev_node in prev_nodes:
                        prev_node.next_cp = cp
                        prev_node.next_nodeavail = node
                    prev_nodeavail[node_id] = [node]
                    

    def get_availability_at_node(self, time, node, preempted_leases = []):
        avails = []
        node = self.changepoints[time].nodes[node]
        prev_avail = None
        prev_node = None
        while node != None:
            if len(preempted_leases) == None:
                available = ResourceTuple.copy(node.available)
            else:
                available = node.get_avail_withpreemption(preempted_leases)

            if prev_avail != None and available.any_less(prev_avail.available):
                available.min(prev_avail.available)
                availentry = AvailEntry(available, None)
                avails.append(availentry)
                prev_avail.until = prev_node.next_cp
                prev_avail = availentry
            elif prev_avail == None:
                availentry = AvailEntry(available, None)
                avails.append(availentry)
                prev_avail = availentry
            
            prev_node = node
            node = node.next_nodeavail
            
        return AvailabilityInNode(avails)
    
    def get_nodes_at(self, time):
        return self.changepoints[time].nodes.keys()

    def get_leases_at(self, node, time):
        return self.changepoints[time].nodes[node].leases
    
    def get_availability_at(self, node, time):
        return self.changepoints[time].nodes[node].available
    
    def get_capacity_interval(self, node, time):
        next_cp = self.changepoints[time].nodes[node].next_cp
        if next_cp == None:
            return None
        else:
            return next_cp - time
        
    def get_leases_until(self, until):
        leases = set()
        for cp in self.cp_list:
            if until <= cp:
                break
            leases.update(self.changepoints[cp].leases)
        return list(leases)

