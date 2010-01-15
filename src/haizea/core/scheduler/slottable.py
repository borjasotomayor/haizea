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
import bisect
import logging
from operator import attrgetter

"""This module provides an in-memory slot table data structure. 

A slot table is essentially just a collection of resource reservations, and is
implemented using the classes ResourceTuple, ResourceReservation, Node, and
KeyValueWrapper, and SlotTable. See the documentation in these classes for
additional details.

This module also provides an "availability window" implementation, which provides
easier access to the contents of a slot table (by determining the availability
in each node starting at a given time). The availability window is implemented
in classes ChangepointAvail, ChangepointNodeAvail, AvailEntry, OngoingAvailability,
and AvailabilityWindow.

"""


class ResourceTuple(object):
    """A resource tuple
    
    This is an internal data structure used by the slot table. To
    manipulate "quantities of resources" in Haizea, use L{Capacity}
    instead.
    
    A resource tuple represents a quantity of resources. For example, 
    "50% of a CPU and 512 MB of memory" is a resource tuple with two
    components (CPU and memory). The purpose of having a class for this
    (instead of a simpler structure, like a list or dictionary) is to
    be able to perform certain basic operations, like determining whether
    one tuple "fits" in another (e.g., the previous tuple fits in
    "100% of CPU and 1024 MB of memory", but in "100% of CPU and 256 MB
    of memory".
    
    A resource tuple is tightly coupled to a particular slot table. So,
    if a slot table defines that each node has "CPUs, memory, and disk space",
    the resource tuples will depend on this definition (the specification
    of valid resources is not repeated in each resource tuple object).
    
    Resources in a resource tuple can be of two types: single instance and
    multi instance. Memory is an example of a single instance resource: there
    is only "one" memory in a node (with some capacity). CPUs are an example
    of a multi instance resource: there can be multiple CPUs in a single node,
    and each CPU can be used to satisfy a requirement for a CPU.
    
    """    
    SINGLE_INSTANCE = 1
    MULTI_INSTANCE = 2
    
    def __init__(self, slottable, single_instance, multi_instance = None):
        """Constructor. Should only be called from SlotTable.
        
        The constructor is not meant to be called directly and should only
        be called from SlotTable.
        
        The res parameter is a list with the quantities of each resource.
        The list starts with the single-instance resources, followed
        by the multi-instance resources. The slottable contains information
        about the layout of this list: 
        
         - The mapping of resource to position in the list is contained in attribute 
        rtuple_restype2pos of the slottable.
           - For single-instance resources, the position returned by this mapping contains
        the quantity. 
           - For multi-instance resources, the position returns the
        quantity of the first instance. The number of instances of a given resource
        is contained in attribute rtuple_nres of the slottable.
         - The number of single-instance resources is contained in attribute rtuple_len of 
        the slottable.
        
        @param slottable: Slot table
        @type slottable: L{SlotTable}
        @param single_instance: Quantities of single instance resources
        @type single_instance: C{list}
        @param multi_instance: Quantities of multi instance resources
        @type multi_instance: C{dict}
        """
        
        self.slottable = slottable
        """
        Slot table
        @type: SlotTable
        """
        
        self._single_instance = single_instance
        """
        Resource quantities
        @type: list
        """        
        
        self._multi_instance = multi_instance 
        """
        Resource quantities
        @type: dict
        """     

    @classmethod
    def copy(cls, rt):
        """Creates a deep copy of a resource tuple
        
        @param rt: Resource tuple to copy
        @type rt: L{ResourceTuple}
        @return: Copy of resource tuple
        @rtype: L{ResourceTuple}
        """        
        return cls(rt.slottable, rt._single_instance[:], dict([(pos,l[:]) for pos, l in rt._multi_instance.items()]))

        
    def fits_in(self, rt):
        """Determines if this resource tuple fits in a given resource tuple
        
        @param rt: Resource tuple
        @type rt: L{ResourceTuple}
        @return: True if this resource tuple fits in rt. False otherwise.
        @rtype: bool
        """        
        
        # For single-instance resources, this is simple: just check if all the values
        # in this resource tuple are smaller that the corresponding value in the
        # given resource tuple
        for i in xrange(self.slottable.rtuple_nsingle):
            if self._single_instance[i] > rt._single_instance[i]:
                return False
            
        # For multi-instance resources this is a bit more hairy, since there
        # are potentially multiple fittings. For example, if we have four CPUs
        # and one is 50% used, an additional 50% could be fit in any of the four
        # CPUs. Here we use a simple first-fit algorithm.
        if self.slottable.rtuple_has_multiinst:
            # Create copy of resource tuple's multi-instance resources. We'll
            # subtract from this to determine if there is a fir.
            _multi_instance2 = dict([(i, l[:]) for (i,l) in rt._multi_instance.items()])
            for (pos, l) in self._multi_instance.items():
                insts = _multi_instance2[pos]
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
    
    
    def incr(self, rt):
        """Increases the resource tuple with the amounts in a given resource tuple
        
        @param rt: Resource tuple
        @type rt: L{ResourceTuple}
        """        
        for slottype in xrange(self.slottable.rtuple_nsingle):
            self._single_instance[slottype] += rt._single_instance[slottype]
        if self.slottable.rtuple_has_multiinst:
            for (pos, l) in rt._multi_instance.items():
                self._multi_instance[pos] += l[:]       
    
    def decr(self, rt):
        """Decreases the resource tuple with the amounts in a given resource tuple
        
        Precondition: rt must be known to fit in the resource tuple (via fits_in)
        
        @param rt: Resource tuple
        @type rt: L{ResourceTuple}
        """        
        for slottype in xrange(self.slottable.rtuple_nsingle):
            self._single_instance[slottype] -= rt._single_instance[slottype]
            
        # Decreasing is trickier than increasing because instead of simply adding
        # more instances, we essentially have to fit the multi-instance resources
        # from the given resource tuple into the resource tuple. For consistency,
        # we use the same first-fit algorithm as in fits_in
        if self.slottable.rtuple_has_multiinst:
            for (pos, l) in rt._multi_instance.items():
                insts = self._multi_instance[pos]
                for quantity in l:
                    fits = False
                    for i in range(len(insts)):
                        if quantity <= insts[i]:
                            fits = True
                            insts[i] -= quantity
                            break
                    if fits == False:
                        # If the precondition is met, this shouldn't happen
                        raise Exception, "Can't decrease"
                    
 

    def get_by_type(self, restype):
        """Gets the amount of a given resource type.
        
        @param restype: Resource type
        @type restype: C{str}
        @return: For single-instance resources, returns the amount. For multi-instance 
        resources, returns the sum of all the instances.
        @rtype: int
        """               
        pos = self.slottable.rtuple_restype2pos[restype]
        if pos < self.slottable.rtuple_nsingle:
            return self._single_instance[pos]
        else:
            return sum(self._multi_instance[pos])
                                     
    
    def any_less(self, rt):
        """Determines if any amount of a resource is less than that in a given resource tuple
        
        In the case of multi-instance resources, this method will only work when both
        resource tuples have the same number of instances, and makes the comparison
        instance by instance. For example, if a CPU resource has two instances A and B:
        ___A__B_
        R1|75 50
        R2|50 75
        
        R2.any_less(R1) returns True. However:

        ___A__B_
        R1|75 50
        R2|75 50
        
        R2.any_less(R1) returns False, even though one instance (R2.B) is less than another (R1.A)
        
        @param rt: Resource tuple
        @type rt: L{ResourceTuple}
        @return: True if these is any resource such that its amount is less than that in rt.
        @rtype: int
        """             
        for i in xrange(self.slottable.rtuple_nsingle):
            if self._single_instance[i] < rt._single_instance[i]:
                return True
        
        if self.slottable.rtuple_has_multiinst:
            for (pos, l) in self._multi_instance.items():
                for i, x in l:
                    if l[i] < rt._multi_instance[pos][i]:
                        return True
                    
        return False    
   
    def min(self, rt):
        """Modifies the resource amounts to the minimum of the current amount and that in the given resource tuple
        
        As in any_less, for multi-instance resources this method will only work when both
        resource tuples have the same number of instances, and makes the change
        instance by instance.
        
        @param rt: Resource tuple
        @type rt: L{ResourceTuple}
        """               
        for i in xrange(self.slottable.rtuple_nsingle):
            self._single_instance[i] = min(self._single_instance[i], rt._single_instance[i])
            
        if self.slottable.rtuple_has_multiinst:
            for (pos, l) in self._multi_instance.items():
                for i, x in l:
                    l[i] = min(l[i], rt._multi_instance[pos][i])
                    
    
    def __repr__(self):
        """Creates a string representation of the resource tuple
        
        @return: String representation
        @rtype: C{str}
        """              
        r=""
        for i, x in enumerate(self._single_instance):
            r += "%s:%i " % (i, x)
        if self.slottable.rtuple_has_multiinst:
            r+= `self._multi_instance`
        return r

    def __eq__(self, rt):
        """Determines if the resource tuple is equal to a given resource tuple
        
        @return: True if they equal, False otherwise
        @rtype: C{str}
        """            
        return self._single_instance == rt._single_instance and self._multi_instance == rt._multi_instance
    
    def __getstate__(self):
        """Returns state necessary to unpickle a ResourceTuple object
        
        """        
        return (self._single_instance, self._multi_instance)

    def __setstate__(self, state):
        """Restores state when unpickling a ResourceTuple object
        
        After unpickling, the object still has to be bound to a slottable.
        """        
        self.slottable = None
        self._single_instance = state[0]
        self._multi_instance = state[1]    


class ResourceReservation(object):
    """A resource reservation
    
    A resource reservation (or RR) is a data structure representing resources
    (represented as a ResourceTuple) reserved across multiple physical nodes.
    (each node can have a different resource tuple; e.g., 1 CPU and 
    512 MB of memory in node 1 and 2 CPUs and 1024 MB of memory in node 2). An RR 
    has a specific start and end time for all the nodes. Thus, if some nodes are 
    reserved for an interval of time, and other nodes are reserved for a different 
    interval (even if these reservations are for the same lease), two separate RRs 
    would have to be added to the slot table.
    
    This class isn't used by itself but rather serves as the base class for 
    VM reservations, image transfer reservations, etc.
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
        """Constructor
        
        @param lease: Lease this resource reservation belongs to
        @type lease: L{Lease}
        @param start: Starting time of the reservation
        @type start: L{DateTime}
        @param end: Ending time of the reservation
        @type end: L{DateTime}
        @param res: A dictionary mapping physical node ids to ResourceTuple objects
        @type res: C{dict}
        """              
        self.lease = lease
        self.start = start
        self.end = end
        self.state = None
        self.resources_in_pnode = res # pnode -> ResourceTuple
                        
    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        """Prints the contents of the RR to the log
        
        @param loglevel: Log level
        @type loglevel: C{str}
        """              
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Start          : %s" % self.start)
        logger.log(loglevel, "End            : %s" % self.end)
        logger.log(loglevel, "State          : %s" % ResourceReservation.state_str[self.state])
        logger.log(loglevel, "Resources      : \n                         %s" % "\n                         ".join(["N%i: %s" %(i, x) for i, x in self.resources_in_pnode.items()])) 


class SlotTable(object):
    """Slot table 
    
    The slot table is one of the main data structures in Haizea (if not *the* main one). 
    It tracks the capacity of the physical nodes on which leases can be scheduled,
    contains the resource reservations of all the leases, and allows efficient access
    to them. 
    
    However, the information in the slot table is stored in a somewhat 'raw' format
    (a collection of L{ResourceReservation}s) which can be hard to use directly. So,
    one of the responsabilities of the slot table is to efficiently generate "availability
    windows", which are a more convenient abstraction over available resources. See
    AvailabilityWindow for more details. When writing a custom mapper, most read-only
    interactions with the slot table should be through availability windows, which can
    be obtained through the get_availability_window method of SlotTable.
    
    The slot table also depends on classes L{Node} and L{KeyValueWrapper}.
    
    Since querying resource reservations is the most frequent operation in Haizea, the
    slot table tries to optimize access to them as much as possible. In particular, 
    we will often need to quickly access reservations starting or ending at a specific 
    time (or in an interval of time). The current slot table implementation stores the RRs 
    in two ordered lists: one by starting time and another by ending time. Access is done by 
    binary search in O(log n) time using the C{bisect} module. Insertion and removal 
    require O(n) time, since lists are implemented internally as arrays in CPython. 
    We could improve these times in the future by using a tree structure (which Python 
    doesn't have natively, so we'd have to include our own tree implementation), although 
    slot table accesses far outweight insertion and removal operations. 
    
    """
    
    def __init__(self, resource_types):
        """Constructor
        
        The slot table will be initially empty, without any physical nodes. These have to be added
        with add_node.
        
        @param resource_types: A dictionary mapping resource types to ResourceTuple.SINGLE_INSTANCE or
        ResourceTuple.MULTI_INSTANCE (depending on whether the resource is single- or multi-instance)
        @type resource_types: C{dict}
        """              
        self.logger = logging.getLogger("SLOT")
        self.nodes = {}
        self.reservations_by_start = []
        self.reservations_by_end = []
        self.resource_types = resource_types
        self.availabilitycache = {}
        self.awcache_time = None
        self.awcache = None
        self.__dirty()

        # Resource tuple fields
        res_singleinstance = [rt for rt,ninst in resource_types if ninst == ResourceTuple.SINGLE_INSTANCE]
        res_multiinstance = [(rt,ninst) for rt,ninst in resource_types if ninst == ResourceTuple.MULTI_INSTANCE]
        self.rtuple_nsingle = len(res_singleinstance)
        self.rtuple_nmultiple = len(res_multiinstance)
        self.rtuple_has_multiinst = self.rtuple_nmultiple > 0
        self.rtuple_restype2pos = dict([(rt,i) for (i,rt) in enumerate(res_singleinstance)])
        pos = self.rtuple_nsingle
        for rt, ninst in res_multiinstance:
            self.rtuple_restype2pos[rt] = pos
            pos = pos + 1

    def add_node(self, node_id, resourcetuple):
        """Add a new physical node to the slot table
        
        @param node_id: Resource type
        @type node_id: C{int}
        @param resourcetuple: Resource type
        @type resourcetuple: L{ResourceTuple}
        """        
        self.nodes[node_id] = Node(resourcetuple)

    def create_empty_resource_tuple(self):
        """Create an empty resource tuple
        
        @return: Empty resource tuple, single-instance resources set to zero, multi-instance resources
        set to zero instances.
        @rtype: L{ResourceTuple}
        """        
        return ResourceTuple(self, [0] * self.rtuple_nsingle, dict((pos,[]) for pos in xrange(self.rtuple_nsingle, self.rtuple_nsingle+self.rtuple_nmultiple)))
    
    def create_resource_tuple_from_capacity(self, capacity):
        """Converts a L{Capacity} object to a L{ResourceTuple}
        
        @param capacity: Resource capacity
        @type capacity: L{Capacity}
        @return: Resource tuple
        @rtype: L{ResourceTuple}        
        """    
        single_instance = [0] * self.rtuple_nsingle
        multi_instance = {}
        for restype in capacity.get_resource_types():
            pos = self.rtuple_restype2pos[restype]
            ninst = capacity.ninstances[restype]
            if pos < self.rtuple_nsingle:
                single_instance[pos] = capacity.get_quantity(restype)
            else:
                multi_instance[pos] = []
                for i in range(ninst):
                    multi_instance[pos].append(capacity.get_quantity_instance(restype, i))

        rt = ResourceTuple(self, single_instance, multi_instance)
                    
        return rt

    def get_availability_window(self, start):  
        """Creates an availability window starting at a given time.
        
        @param start: Start of availability window.
        @type start: L{DateTime}
        @return: Availability window
        @rtype: L{AvailabilityWindow}        
        """      
        
        # If possible, we try to use the cached availability window, so we don't have to
        # recompute an entire availability window from scratch.
        # The way availability windows are currently implemented (see AvailabilityWindow
        # for details), an existing availability window can be used if the requested start 
        # time is after the existing start time *and* the requested start time is one of
        # the changepoints covered by the availability window.
        if self.awcache == None or start < self.awcache_time or (start >= self.awcache_time and not self.awcache.changepoints.has_key(start)):
            # If the cached version doesn't work, recompute the availability window
            self.__get_aw_cache_miss(start)
        return self.awcache
    
    def get_availability(self, time, min_capacity=None):
        """Computes the available resources on all nodes at a given time.
        
        @param time: Time at which to determine availability.
        @type time: L{DateTime}
        @param min_capacity: If not None, only include the nodes that have at least
        this minimum capacity.
        @type min_capacity: L{ResourceTuple}
        @return: A dictionary mapping physical node id to a L{Node} object (which
        contains the available capacity of that physical node at the specified time)
        @rtype: C{dict}        
        """        
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
    
    def is_empty(self):
        """Determines if the slot table is empty (has no reservations)
        
        @return: True if there are no reservations, False otherwise.
        @rtype: C{bool}        
        """        
        return (len(self.reservations_by_start) == 0)

    def is_full(self, time, restype):
        """Determines if a resource type is "full" at a specified time.
        
        A resource type is considered to be "full" if its available capacity is zero
        in all the physical nodes in the slot table.
        
        @param time: time at which to check for fullness.
        @type time: L{DateTime}        
        @param restype: Resource type
        @type restype: C{str}
        @return: True if the resource type is full, False otherwise.
        @rtype: C{bool}        
        """        
        nodes = self.get_availability(time)
        avail = sum([node.capacity.get_by_type(restype) for node in nodes.values()])
        return (avail == 0)

    def get_total_capacity(self, restype):
        """Determines the aggregate capacity of a given resource type across all nodes.
        
        @param restype: Resource type
        @type restype: C{str}
        @return: Total capacity
        @rtype: C{int}        
        """        
        return sum([n.capacity.get_by_type(restype) for n in self.nodes.values()])

    def add_reservation(self, rr):
        """Adds a L{ResourceReservation} to the slot table.
        
        @param rr: Resource reservation
        @type rr: L{ResourceReservation}
        """
        startitem = KeyValueWrapper(rr.start, rr)
        enditem = KeyValueWrapper(rr.end, rr)
        bisect.insort(self.reservations_by_start, startitem)
        bisect.insort(self.reservations_by_end, enditem)
        self.__dirty()


    def update_reservation(self, rr, old_start, old_end):
        """Update a L{ResourceReservation} to the slot table.

        Since the start and end time are used to index the reservations,
        the old times have to be provided so we can find the old reservation
        and make the changes.
        
        @param rr: Resource reservation with updated values (including potentially new start and/or end times)
        @type rr: L{ResourceReservation}
        @param old_start: Start time of reservation before update.
        @type old_start: L{DateTime}
        @param old_end: End time of reservation before update.
        @type old_end: L{DateTime}
        """        
        # TODO: Might be more efficient to resort lists
        self.__remove_reservation(rr, old_start, old_end)
        self.add_reservation(rr)
        self.__dirty()
        
        
    def remove_reservation(self, rr):
        """Remove a L{ResourceReservation} from the slot table.
    
        @param rr: Resource reservation
        @type rr: L{ResourceReservation}
        """        
        self.__remove_reservation(rr, rr.start, rr.end)


    def get_reservations_at(self, time):
        """Get all reservations at a specified time
    
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """             
        item = KeyValueWrapper(time, None)
        startpos = bisect.bisect_right(self.reservations_by_start, item)
        bystart = set([x.value for x in self.reservations_by_start[:startpos]])
        endpos = bisect.bisect_right(self.reservations_by_end, item)
        byend = set([x.value for x in self.reservations_by_end[endpos:]])
        res = bystart & byend
        return list(res)
    
    def get_reservations_starting_between(self, start, end):
        """Get all reservations starting in a specified interval.
        
        The interval is closed: it includes the starting time and the ending time.
    
        @param start: Start of interval
        @type start: L{DateTime}
        @param end: End of interval
        @type end: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """         
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_start, startitem)
        endpos = bisect.bisect_right(self.reservations_by_start, enditem)
        res = [x.value for x in self.reservations_by_start[startpos:endpos]]
        return res

    def get_reservations_ending_between(self, start, end):
        """Get all reservations ending in a specified interval.
        
        The interval is closed: it includes the starting time and the ending time.
    
        @param start: Start of interval
        @type start: L{DateTime}
        @param end: End of interval
        @type end: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """        
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_end, startitem)
        endpos = bisect.bisect_right(self.reservations_by_end, enditem)
        res = [x.value for x in self.reservations_by_end[startpos:endpos]]
        return res

    def get_reservations_starting_after(self, start):
        """Get all reservations starting after (but not on) a specified time
    
        @param start: Time
        @type start: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """             
        startitem = KeyValueWrapper(start, None)
        startpos = bisect.bisect_right(self.reservations_by_start, startitem)
        res = [x.value for x in self.reservations_by_start[startpos:]]
        return res

    def get_reservations_ending_after(self, end):
        """Get all reservations ending after (but not on) a specified time
    
        @param end: Time
        @type end: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """             
        startitem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_right(self.reservations_by_end, startitem)
        res = [x.value for x in self.reservations_by_end[startpos:]]
        return res

    def get_reservations_starting_on_or_after(self, start):
        """Get all reservations starting on or after a specified time
    
        @param start: Time
        @type start: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """             
        startitem = KeyValueWrapper(start, None)
        startpos = bisect.bisect_left(self.reservations_by_start, startitem)
        res = [x.value for x in self.reservations_by_start[startpos:]]
        return res

    def get_reservations_ending_on_or_after(self, end):
        """Get all reservations ending on or after a specified time
    
        @param end: Time
        @type end: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """      
        startitem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservations_by_end, startitem)
        res = [x.value for x in self.reservations_by_end[startpos:]]
        return res


    def get_reservations_starting_at(self, time):
        """Get all reservations starting at a specified time
    
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """        
        return self.get_reservations_starting_between(time, time)

    def get_reservations_ending_at(self, time):
        """Get all reservations ending at a specified time
    
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """        
        return self.get_reservations_ending_between(time, time) 

    def get_reservations_after(self, time):
        """Get all reservations that take place after (but not on) a
        specified time. i.e., all reservations starting or ending after that time.
    
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """            
        bystart = set(self.get_reservations_starting_after(time))
        byend = set(self.get_reservations_ending_after(time))
        return list(bystart | byend)
    
    def get_reservations_on_or_after(self, time):
        """Get all reservations that take place on or after a
        specified time. i.e., all reservations starting or ending after that time.
    
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """        
        bystart = set(self.get_reservations_starting_on_or_after(time))
        byend = set(self.get_reservations_ending_on_or_after(time))
        return list(bystart | byend)    

    def get_changepoints_after(self, after, until=None, nodes=None):
        """Get all the changepoints after a given time.
        
        A changepoint is any time anything is scheduled to change in the
        slottable (a reservation starting or ending). 
    
        @param after: Time
        @type after: L{DateTime}
        @param until: If not None, only include changepoints until this time.
        @type until: L{DateTime}
        @param nodes: If not None, only include changepoints affecting these nodes.
        @type nodes: C{list} of C{int}s
        @return: Changepoints
        @rtype: C{list} of L{DateTime}s
        """        
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
    
    def get_next_changepoint(self, time):
        """Get the first changepoint after a given time.
 
        @param time: Time
        @type time: L{DateTime}
        @return: Changepoints
        @rtype: L{DateTime}
        """        
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
        


    def get_next_premature_end(self, after):
        """Get the first premature end time after a given time. ONLY FOR SIMULATION.
        
        In simulation, some reservations can end prematurely, and this information
        is stored in the slot table (in real life, this information is not
        known a priori). 
 
        @param after: Time
        @type after: L{DateTime}
        @return: Next premature end
        @rtype: L{DateTime}
        """              
        from haizea.core.scheduler.vm_scheduler import VMResourceReservation
        # Inefficient, but ok since this query seldom happens
        res = [i.value for i in self.reservations_by_end if isinstance(i.value, VMResourceReservation) and i.value.prematureend > after]
        if len(res) > 0:
            prematureends = [r.prematureend for r in res]
            prematureends.sort()
            return prematureends[0]
        else:
            return None
    
    
    def get_prematurely_ending_res(self, time):
        """Gets all the L{ResourceReservation}s that are set to end prematurely at a given time. ONLY FOR SIMULATION
        
        @param time: Time
        @type time: L{DateTime}
        @return: Resource reservations
        @rtype: C{list} of L{ResourceReservation}s
        """           
        from haizea.core.scheduler.vm_scheduler import VMResourceReservation
        return [i.value for i in self.reservations_by_end if isinstance(i.value, VMResourceReservation) and i.value.prematureend == time]

    def save(self, leases = []):
        self.reservations_by_start2 = self.reservations_by_start[:]
        self.reservations_by_end2 = self.reservations_by_end[:]
        
        self.orig_vmrrs = dict([(l,l.vm_rrs[:]) for l in leases])
        self.orig_vmrrs_data = {}
        for orig_vmrr in self.orig_vmrrs.values():
            for vmrr in orig_vmrr:
                self.orig_vmrrs_data[vmrr] = (vmrr.start, vmrr.end, vmrr.prematureend, vmrr.pre_rrs[:], vmrr.post_rrs[:])


    def restore(self):
        self.reservations_by_start = self.reservations_by_start2
        self.reservations_by_end = self.reservations_by_end2
        
        for l in self.orig_vmrrs:
            l.vm_rrs = self.orig_vmrrs[l]
            for vm_rr in l.vm_rrs:
                vm_rr.start = self.orig_vmrrs_data[vm_rr][0]
                vm_rr.end = self.orig_vmrrs_data[vm_rr][1]
                vm_rr.prematureend = self.orig_vmrrs_data[vm_rr][2]
                vm_rr.pre_rrs = self.orig_vmrrs_data[vm_rr][3]
                vm_rr.post_rrs = self.orig_vmrrs_data[vm_rr][4]
   
        self.__dirty()


    def __remove_reservation(self, rr, start=None, end=None):
        """Remove a L{ResourceReservation} from the slot table.
    
        @param rr: Resource reservation
        @type rr: L{ResourceReservation}
        @param start: Start time under which the reservation is indexed, in cases where the RR
        has changed (this parameter is only used when calling this method from update_reservation)
        @type start: L{DateTime}
        @param end: Same as start, but for the end time for the RR.
        @type end: L{DateTime}
        """        
        if start == None:
            start = rr.start
        if end == None:
            end = rr.end
        posstart = self.__get_reservation_index(self.reservations_by_start, rr, start)
        posend = self.__get_reservation_index(self.reservations_by_end, rr, end)
        del self.reservations_by_start[posstart]
        del self.reservations_by_end[posend]
        self.__dirty()


    def __get_availability_cache_miss(self, time):
        """Computes availability at a given time, and caches it.
        
        Called when get_availability can't use availabilities in the cache.
        
        @param time: Time at which to determine availability.
        @type time: L{DateTime}
        """        
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
        """Computes availability window at a given time, and caches it.
        
        Called when get_availability_window can't use the cached availability window.
        
        @param time: Start of availability window.
        @type time: L{DateTime}
        """        
        self.awcache = AvailabilityWindow(self, time)
        self.awcache_time = time
        
    def __dirty(self):
        """Empties the caches.
        
        Should be called whenever the caches become dirty (e.g., when a reservation
        is added to the slot table).
        
        """        
        # You're a dirty, dirty slot table and you should be
        # ashamed of having outdated caches!
        self.availabilitycache = {}
        self.awcache_time = None
        self.awcache = None

    def __get_reservation_index(self, rlist, rr, time):
        """Find the index of a resource reservation in one of the internal reservation lists
    
        @param rlist: Resource reservation
        @type rlist: C{list} of L{ResourceReservation}s
        @param rr: Resource reservation to look up
        @type rr: L{ResourceReservation}
        @param time: time the reservation is indexed under
        @type time: L{DateTime}
        """        
        item = KeyValueWrapper(time, None)
        pos = bisect.bisect_left(rlist, item)
        found = False
        while not found:
            if rlist[pos].value == rr:
                found = True
            else:
                pos += 1
        return pos

    def sanity_check(self, only_at = None):
        """Verifies the slot table is consistent. Used by unit tests.
        
        @return: Returns a tuple, the first item being True if the slot table
        is in a consistent state, and False otherwise. If the slot table is not
        in a consistent state, the remaining values in the tuple are the
        offending node, the offending changepoint, and the available resources
        in the node at the changepoint.
        @rtype: (C{bool}, 
        """        
        # Get checkpoints
        if only_at != None:
            changepoints = [only_at]
        else:
            changepoints = set()
            for rr in [x.value for x in self.reservations_by_start]:
                changepoints.add(rr.start)
                changepoints.add(rr.end)
            changepoints = list(changepoints)
            changepoints.sort()
      
        for cp in changepoints:
            avail = self.get_availability(cp)
            for node in avail:
                for resource in avail[node].capacity._single_instance:
                    if resource < 0:
                        return False, node, cp, avail[node].capacity
                
        return True, None, None, None
    
# TODO: We don't need a class for this anymore, but removing it requires making a lot of
# changes in SlotTable.
class Node(object):
    """A physical node in the slot table."""       
            
    def __init__(self, capacity):
        """Constructor
        
        @param capacity: Capacity of the node
        @type capacity: L{ResourceTuple}
        """        
        self.capacity = ResourceTuple.copy(capacity)

        
class KeyValueWrapper(object):
    """A wrapper around L{ResourceReservations} so we can use the bisect module
    to manage ordered lists of reservations."""   
    
    def __init__(self, key, value):
        """Constructor
        
        @param key: Time under which the reservation should be indexed
        @type key: L{DateTime}
        @param value: Resource reservation
        @type value: L{ResourceReservation}
        """           
        self.key = key
        self.value = value
        
    def __cmp__(self, other):
        return cmp(self.key, other.key)    
    

class AvailabilityWindow(object):
    """An availability window
    
    A particularly important operation with the slot table is determining the
    "availability window" of resources starting at a given time. In a nutshell, 
    an availability window provides a convenient abstraction over the slot table, 
    with methods to answer questions like "If I want to start a least at time T, 
    are there enough resources available to start the lease?" "Will those resources 
    be available until time T+t?" "If not, what's the longest period of time those 
    resources will be available?" etc.
    
    AvailabilityWindow objects are not meant to be created directly, and should be
    created through the SlotTable's get_availability_window method.

    """
    def __init__(self, slottable, time):
        """Constructor
        
        An availability window starts at a specific time, provided to the constructor.
        
        @param slottable: Slot table the availability window is based upon.
        @type slottable: L{SlotTable}
        @param time: Starting time of the availability window.
        @type time: L{DateTime}
        """            
        self.slottable = slottable
        self.logger = logging.getLogger("SLOTTABLE.WIN")
        self.time = time
        self.leases = set()

        self.cp_list = [self.time] + self.slottable.get_changepoints_after(time)

        # The availability window is stored using a sparse data structure that
        # allows quick access to information related to a specific changepoint in
        # the slottable.
        #
        # All this information is contained in the 'changepoints' attribute:
        #  - The 'changepoints' attribute is a dictionary mapping changepoints 
        #    to ChangepointAvail objects.
        #  - A ChangepointAvail contains information about availability in a
        #    changepoint. More specifically, it contains ChangepointNodeAvail
        #
        # We also have an ordered list of changepoints in cp_list

        # Create initial changepoint dictionary
        self.changepoints = dict([(cp,ChangepointAvail()) for cp in self.cp_list])
 
        # Add the nodes to each ChangepointAvail object
        for cp in self.changepoints.values():
            for node_id, node in self.slottable.nodes.items():
                cp.add_node(node_id, node.capacity)
        
        # Get reservations that will affect the availability window.
        rrs = self.slottable.get_reservations_after(time)
        rrs.sort(key=attrgetter("start"))
        
        # This is an index into cp_list. We start at the first changepoint.
        pos = 0

        # Fill in rest of the availability window.
        # For each reservation, we go through each changepoint the reservation
        # passes through, and we reduce the availability at that changepoint.
        # Note that the RRs are ordered by starting time.
        for rr in rrs:
            # Ignore nil-duration reservations
            if rr.start == rr.end:
                continue
            
            # Advance pos to the changepoint corresponding to the RR's starting time.
            while rr.start >= self.time and self.cp_list[pos] != rr.start:
                pos += 1
                
            # Add the lease to the set of leases included in the availability window
            lease = rr.lease
            self.leases.add(lease)
            
            # Get the ChangepointAvail object for the starting changepoint. Note
            # that the RRs starting time might be before the start of the availability
            # window, in which case we just take the first ChangepointAvail.
            if rr.start >= self.time:
                start_cp = self.changepoints[rr.start]
            else:
                start_cp = self.changepoints[self.time]

            # Add the RR's lease to the ChangepointAvail object
            start_cp.leases.add(lease)
            
            # Decrease the availability at each node
            for node in rr.resources_in_pnode:
                start_cp.nodes[node].decr(rr.resources_in_pnode[node])
                start_cp.nodes[node].add_lease(lease, rr.resources_in_pnode[node])

            # Process the other changepoints covered by this RR.
            pos2 = pos + 1

            while self.cp_list[pos2] < rr.end:
                cp = self.changepoints[self.cp_list[pos2]]
                cp.leases.add(lease)
                for node in rr.resources_in_pnode:
                    cp.nodes[node].decr(rr.resources_in_pnode[node])
                    cp.nodes[node].add_lease(lease, rr.resources_in_pnode[node])
                    
                pos2 += 1
        
        
        # We link the ChangepointNodeAvail objects so each includes a 'pointer' to
        # the next changepoint in that node and the corresponding next 
        # ChangepointNodeAvail

        prev_nodeavail = {}
        for node_id, node in self.changepoints[self.time].nodes.items():
            prev_nodeavail[node_id] = [node]
        
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
                    
                    
    def get_availability(self, time, node):
        """Determines the available capacity at a given time and node
        
        @param time: Time
        @type time: L{DateTime}
        @param node: Node id
        @type node: C{int}
        @return: Available capacity
        @rtype: L{ResourceTuple}
        """                 
        return self.changepoints[time].nodes[node].available                    


    def get_ongoing_availability(self, time, node, preempted_leases = []):
        """Determines the available capacity from a given time onwards.
        
        This method returns an L{OngoingAvailability} object (see that class's
        documentation for more details)
        
        @param time: Time
        @type time: L{DateTime}
        @param node: Node id
        @type node: C{int}
        @param preempted_leases: List of leases that can be preempted.
        @type preempted_leases: C{list} of L{Lease}s
        @return: Ongoing availability (see L{OngoingAvailability} documentation for more details)
        @rtype: L{OngoingAvailability}
        """                 
        return OngoingAvailability(self.changepoints[time].nodes[node], preempted_leases)
    

    def get_nodes_at(self, time):
        """Get all the nodes at a given time.
        
        @param time: Time
        @type time: L{DateTime}
        @return: Node ids
        @rtype: C{list} of C{int}
        """        
        return self.changepoints[time].nodes.keys()

    def get_leases_at(self, node, time):
        """Get leases scheduled on a node at a given time.
        
        @param node: Node id
        @type node: C{int}
        @param time: Time
        @type time: L{DateTime}
        """             
        return self.changepoints[time].nodes[node].leases
        
    def get_leases_between(self, from_time, until_time):
        """Get all the leases scheduled in an interval.
        
        This interval is semi-closed: It includes the start time but not the
        end time of the interval.
        
        @param from_time: Start of interval
        @type from_time: L{DateTime}
        @param until_time: End of interval
        @type until_time: L{DateTime}
        @return: Leases
        @rtype: C{list} of L{Lease}s
        """              
        leases = set()
        for cp in self.cp_list:
            if cp < from_time:
                continue
            if cp >= until_time:
                break
            leases.update(self.changepoints[cp].leases)
        return list(leases)

    def get_capacity_duration(self, node, time):
        """Determine how much longer the capacity in a node will
        last, starting at a given time.
        
        @param node: Node id
        @type node: C{int}
        @param time: Time
        @type time: L{DateTime}
        @return: Duration the capacity will last. If it will last indefinitely,
        None is returned.
        @rtype: L{DateTimeDelta}        
        """        
        next_cp = self.changepoints[time].nodes[node].next_cp
        if next_cp == None:
            return None
        else:
            return next_cp - time
        

class OngoingAvailability(object):
    """Information about ongoing availability in a node
    
    An OngoingAvailability object contains information not just about 
    the availability starting at a given time, but also how that availability 
    diminishes over time. Thus, it the object to use when determining
    if, starting at a given time, it is possible to fit some capacity
    up to a certain time (with or without preempting other leases).
    
    Typically, you will want to create an OngoingAvailability object using
    the get_ongoing_availability method in L{AvailabilityWindow}
    """
    
    def __init__(self, node, preempted_leases):
        """Constructor
        
        @param node: Node and time from which to start determing availability, represented
        by a valid L{ChangepointNodeAvail} object from the L{AvailabilityWindow}.
        @type node: L{ChangepointNodeAvail}
        @param preempted_leases: List of leases that can be preempted.
        @type preempted_leases: C{list} of L{Lease}s
        """           
        avails = []
        prev_avail = None
        prev_node = None
        
        # Traverse the list of ChangepointNodeAvails
        while node != None:
            if len(preempted_leases) == 0:
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
        
        self.avail_list = avails
        
        
    def fits(self, capacity, until):
        """Determine if there is enough capacity until a given time.
        
        @param capacity: Capacity
        @type capacity: L{ResourceTuple}
        @param until: Time
        @type until: L{DateTime}
        @return: True if the given capacity can fit until the given time. False otherwise.
        @rtype: C{bool}     
        """        
        for avail in self.avail_list:
            if avail.until == None or avail.until >= until:
                return capacity.fits_in(avail.available)

    def latest_fit(self, capacity):
        """Determine for how long we can fit a given capacity.
        
        @param capacity: Capacity
        @type capacity: L{ResourceTuple}
        @return: The latest time at which the given capacity fits in the node.
        @rtype: L{DateTime} 
        """             
        prev = None
        for avail in self.avail_list:
            if not capacity.fits_in(avail.available):
                return prev
            else:
                prev = avail.until


# TODO: Document these classes too. These are pretty simple and only used internally by
# Haizea (there's no good reason why someone writing a mapper or a policy would have to
# use them), so for now they should be pretty self-explanatory.

class AvailEntry(object):
    def __init__(self, available, until):
        self.available = available
        self.until = until
        
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
        

    


