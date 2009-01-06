# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
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

from mx.DateTime import ISO, TimeDelta
from operator import attrgetter, itemgetter
import haizea.common.constants as constants
from math import ceil, floor
import bisect
import copy
import logging

class SlotFittingException(Exception):
    pass

class CriticalSlotFittingException(Exception):
    pass


class Node(object):
    def __init__(self, capacity, capacitywithpreemption, resourcepoolnode):
        self.capacity = ResourceTuple.copy(capacity)
        if capacitywithpreemption == None:
            self.capacitywithpreemption = None
        else:
            self.capacitywithpreemption = ResourceTuple.copy(capacitywithpreemption)
        self.resourcepoolnode = resourcepoolnode
        
    @classmethod
    def from_resourcepool_node(cls, node):
        capacity = node.get_capacity()
        return cls(capacity, capacity, node)

class NodeList(object):
    def __init__(self):
        self.nodelist = []

    def add(self, node):
        self.nodelist.append(node)
        
    def __getitem__(self, n):
        return self.nodelist[n-1]

    def copy(self):
        nodelist = NodeList()
        for n in self.nodelist:
            nodelist.add(Node(n.capacity, n.capacitywithpreemption, n.resourcepoolnode))
        return nodelist
   
    def toDict(self):
        nodelist = self.copy()
        return dict([(i+1, v) for i, v in enumerate(nodelist)])
        
class KeyValueWrapper(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        
    def __cmp__(self, other):
        return cmp(self.key, other.key)

class ResourceReservation(object):
    
    # Resource reservation states
    STATE_SCHEDULED = 0
    STATE_ACTIVE = 1
    STATE_DONE = 2

    state_str = {STATE_SCHEDULED : "Scheduled",
                 STATE_ACTIVE : "Active",
                 STATE_DONE : "Done"}
    
    def __init__(self, lease, start, end, res):
        self.lease = lease
        self.start = start
        self.end = end
        self.state = None
        self.resources_in_pnode = res
        self.logger = logging.getLogger("LEASES")
                        
    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Start          : %s" % self.start)
        self.logger.log(loglevel, "End            : %s" % self.end)
        self.logger.log(loglevel, "State          : %s" % ResourceReservation.state_str[self.state])
        self.logger.log(loglevel, "Resources      : \n                         %s" % "\n                         ".join(["N%i: %s" %(i, x) for i, x in self.resources_in_pnode.items()])) 
                
    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        rr = {}                
        rr["start"] = xmlrpc_marshall_singlevalue(self.start)
        rr["end"] = xmlrpc_marshall_singlevalue(self.end)
        rr["state"] = self.state
        return rr

class ResourceTuple(object):
    def __init__(self, res):
        self._res = res
        
    @classmethod
    def from_list(cls, l):
        return cls(l[:])

    @classmethod
    def copy(cls, rt):
        return cls(rt._res[:])
    
    @classmethod
    def set_resource_types(cls, resourcetypes):
        cls.type2pos = dict([(x[0], i) for i, x in enumerate(resourcetypes)])
        cls.descriptions = dict([(i, x[2]) for i, x in enumerate(resourcetypes)])
        cls.tuplelength = len(resourcetypes)

    @classmethod
    def create_empty(cls):
        return cls([0 for x in range(cls.tuplelength)])
        
    def fits_in(self, res2):
        fits = True
        for i in xrange(len(self._res)):
            if self._res[i] > res2._res[i]:
                fits = False
                break
        return fits
    
    def get_num_fits_in(self, res2):
        canfit = 10000 # Arbitrarily large
        for i in xrange(len(self._res)):
            if self._res[i] != 0:
                f = res2._res[i] / self._res[i]
                if f < canfit:
                    canfit = f
        return int(floor(canfit))
    
    def decr(self, res2):
        for slottype in xrange(len(self._res)):
            self._res[slottype] -= res2._res[slottype]

    def incr(self, res2):
        for slottype in xrange(len(self._res)):
            self._res[slottype] += res2._res[slottype]
        
    def get_by_type(self, resourcetype):
        return self._res[self.type2pos[resourcetype]]

    def set_by_type(self, resourcetype, value):
        self._res[self.type2pos[resourcetype]] = value        
        
    def is_zero_or_less(self):
        return sum([v for v in self._res]) <= 0
    
    def __repr__(self):
        r=""
        for i, x in enumerate(self._res):
            r += "%s:%.2f " % (self.descriptions[i], x)
        return r


class SlotTable(object):
    def __init__(self):
        self.logger = logging.getLogger("SLOT")
        self.nodes = NodeList()
        self.reservations = []
        self.reservationsByStart = []
        self.reservationsByEnd = []
        self.availabilitycache = {}
        self.changepointcache = None
        
        self.availabilitywindow = AvailabilityWindow(self)

    def add_node(self, resourcepoolnode):
        self.nodes.add(Node.from_resourcepool_node(resourcepoolnode))

    def is_empty(self):
        return (len(self.reservationsByStart) == 0)

    def dirty(self):
        # You're a dirty, dirty slot table and you should be
        # ashamed of having outdated caches!
        self.availabilitycache = {}
        self.changepointcache = None
        
    def getAvailabilityCacheMiss(self, time):
        allnodes = set([i+1 for i in range(len(self.nodes.nodelist))])
        onlynodes = None       
        nodes = {} 
        reservations = self.getReservationsAt(time)
        # Find how much resources are available on each node
        canpreempt = True
        for r in reservations:
            for node in r.resources_in_pnode:
                if onlynodes == None or (onlynodes != None and node in onlynodes):
                    if not nodes.has_key(node):
                        n = self.nodes[node]
                        if canpreempt:
                            nodes[node] = Node(n.capacity, n.capacitywithpreemption, n.resourcepoolnode)
                        else:
                            nodes[node] = Node(n.capacity, None, n.resourcepoolnode)
                    nodes[node].capacity.decr(r.resources_in_pnode[node])
                    if canpreempt and not r.is_preemptible:
                        nodes[node].capacitywithpreemption.decr(r.resources_in_pnode[node])

        # For the remaining nodes, use a reference to the original node, not a copy
        if onlynodes == None:
            missing = allnodes - set(nodes.keys())
        else:
            missing = onlynodes - set(nodes.keys())
            
        for node in missing:
            nodes[node] = self.nodes[node]                    
            
        self.availabilitycache[time] = nodes

    def getAvailability(self, time, resreq=None, onlynodes=None, canpreempt=False):
        if not self.availabilitycache.has_key(time):
            self.getAvailabilityCacheMiss(time)
            # Cache miss
            
        nodes = self.availabilitycache[time]

        if onlynodes != None:
            onlynodes = set(onlynodes)
            nodes = dict([(n,node) for n,node in nodes.items() if n in onlynodes])

        # Keep only those nodes with enough resources
        if resreq != None:
            newnodes = {}
            for n, node in nodes.items():
                if not resreq.fits_in(node.capacity) or (canpreempt and not resreq.fits_in(node.capacitywithpreemption)):
                    pass
                else:
                    newnodes[n]=node
            nodes = newnodes

        return nodes
    
    def get_total_capacity(self, restype = constants.RES_CPU):
        return sum([n.capacity.get_by_type(restype) for n in self.nodes.nodelist])        

    def getReservationsAt(self, time):
        item = KeyValueWrapper(time, None)
        startpos = bisect.bisect_right(self.reservationsByStart, item)
        bystart = set([x.value for x in self.reservationsByStart[:startpos]])
        endpos = bisect.bisect_right(self.reservationsByEnd, item)
        byend = set([x.value for x in self.reservationsByEnd[endpos:]])
        res = bystart & byend
        return list(res)
    
    def get_reservations_starting_between(self, start, end):
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservationsByStart, startitem)
        endpos = bisect.bisect_right(self.reservationsByStart, enditem)
        res = [x.value for x in self.reservationsByStart[startpos:endpos]]
        return res

    def get_reservations_starting_after(self, start):
        startitem = KeyValueWrapper(start, None)
        startpos = bisect.bisect_left(self.reservationsByStart, startitem)
        res = [x.value for x in self.reservationsByStart[startpos:]]
        return res

    def get_reservations_ending_after(self, end):
        startitem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservationsByEnd, startitem)
        res = [x.value for x in self.reservationsByEnd[startpos:]]
        return res

    def get_reservations_ending_between(self, start, end):
        startitem = KeyValueWrapper(start, None)
        enditem = KeyValueWrapper(end, None)
        startpos = bisect.bisect_left(self.reservationsByEnd, startitem)
        endpos = bisect.bisect_right(self.reservationsByEnd, enditem)
        res = [x.value for x in self.reservationsByEnd[startpos:endpos]]
        return res
    
    def get_reservations_starting_at(self, time):
        return self.get_reservations_starting_between(time, time)

    def get_reservations_ending_at(self, time):
        return self.get_reservations_ending_between(time, time)
    
    # ONLY for simulation
    def getNextPrematureEnd(self, after):
        from haizea.resourcemanager.scheduler.vm_scheduler import VMResourceReservation
        # Inefficient, but ok since this query seldom happens
        res = [i.value for i in self.reservationsByEnd if isinstance(i.value, VMResourceReservation) and i.value.prematureend > after]
        if len(res) > 0:
            prematureends = [r.prematureend for r in res]
            prematureends.sort()
            return prematureends[0]
        else:
            return None
    
    # ONLY for simulation
    def getPrematurelyEndingRes(self, t):
        from haizea.resourcemanager.scheduler.vm_scheduler import VMResourceReservation
        return [i.value for i in self.reservationsByEnd if isinstance(i.value, VMResourceReservation) and i.value.prematureend == t]

    
    def get_reservations_starting_or_ending_after(self, after):
        item = KeyValueWrapper(after, None)
        startpos = bisect.bisect_right(self.reservationsByStart, item)
        bystart = set([x.value for x in self.reservationsByStart[:startpos]])
        endpos = bisect.bisect_right(self.reservationsByEnd, item)
        byend = set([x.value for x in self.reservationsByEnd[endpos:]])
        res = bystart | byend
        return list(res)    
    
    def addReservation(self, rr):
        startitem = KeyValueWrapper(rr.start, rr)
        enditem = KeyValueWrapper(rr.end, rr)
        bisect.insort(self.reservationsByStart, startitem)
        bisect.insort(self.reservationsByEnd, enditem)
        self.dirty()

    # If the slot table keys are not modified (start / end time)
    # Just remove and reinsert.
    def updateReservation(self, rr):
        # TODO: Might be more efficient to resort lists
        self.removeReservation(rr)
        self.addReservation(rr)
        self.dirty()

    # If the slot table keys are modified (start and/or end time)
    # provide the old keys (so we can remove it using
    # the m) and updated reservation
    def update_reservation_with_key_change(self, rr, old_start, old_end):
        # TODO: Might be more efficient to resort lists
        self.removeReservation(rr, old_start, old_end)
        self.addReservation(rr)
        self.dirty()


    def getIndexOfReservation(self, rlist, rr, key):
        item = KeyValueWrapper(key, None)
        pos = bisect.bisect_left(rlist, item)
        found = False
        while not found:
            if rlist[pos].value == rr:
                found = True
            else:
                pos += 1
        return pos

    def removeReservation(self, rr, start=None, end=None):
        if start == None:
            start = rr.start
        if end == None:
            end = rr.start
        posstart = self.getIndexOfReservation(self.reservationsByStart, rr, start)
        posend = self.getIndexOfReservation(self.reservationsByEnd, rr, end)
        self.reservationsByStart.pop(posstart)
        self.reservationsByEnd.pop(posend)
        self.dirty()

    
    def findChangePointsAfter(self, after, until=None, nodes=None):
        changepoints = set()
        res = self.get_reservations_starting_or_ending_after(after)
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
    
    def peekNextChangePoint(self, time):
        if self.changepointcache == None:
            # Cache is empty
            changepoints = self.findChangePointsAfter(time)
            changepoints.reverse()
            self.changepointcache = changepoints
        if len(self.changepointcache) == 0:
            return None
        else:
            return self.changepointcache[-1]
    
    def getNextChangePoint(self, time):
        p = self.peekNextChangePoint(time)
        if p != None:
            self.changepointcache.pop()
        return p
        
    def isFull(self, time):
        nodes = self.getAvailability(time)
        avail = sum([node.capacity.get_by_type(constants.RES_CPU) for node in nodes.values()])
        return (avail == 0)
    
    def get_next_reservations_in_nodes(self, time, nodes, rr_type=None, immediately_next = False):
        nodes = set(nodes)
        rrs_in_nodes = []
        earliest_end_time = {}
        rrs = self.get_reservations_starting_after(time)
        if rr_type != None:
            rrs = [rr for rr in rrs if isinstance(rr, rr_type)]
            
        # Filter the RRs by nodes
        for r in rrs:
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

class AvailEntry(object):
    def __init__(self, time, avail, availpreempt, resreq):
        self.time = time
        self.avail = avail
        self.availpreempt = availpreempt
        
        if avail == None and availpreempt == None:
            self.canfit = 0
            self.canfitpreempt = 0
        else:
            self.canfit = resreq.get_num_fits_in(avail)
            if availpreempt == None:
                self.canfitpreempt = 0
            else:
                self.canfitpreempt = resreq.get_num_fits_in(availpreempt)
        
    def getCanfit(self, canpreempt):
        if canpreempt:
            return self.canfitpreempt
        else:
            return self.canfit


class AvailabilityWindow(object):
    def __init__(self, slottable):
        self.slottable = slottable
        self.logger = logging.getLogger("SLOTTABLE.WIN")
        self.time = None
        self.resreq = None
        self.onlynodes = None
        self.avail = None
        
    # Create avail structure
    def initWindow(self, time, resreq, onlynodes = None, canpreempt=False):
        self.time = time
        self.resreq = resreq
        self.onlynodes = onlynodes
        self.avail = {}

        # Availability at initial time
        availatstart = self.slottable.getAvailability(self.time, self.resreq, self.onlynodes, canpreempt)
        for node in availatstart:
            capacity = availatstart[node].capacity
            if canpreempt:
                capacitywithpreemption = availatstart[node].capacitywithpreemption
            else:
                capacitywithpreemption = None
            self.avail[node] = [AvailEntry(self.time, capacity, capacitywithpreemption, self.resreq)]
        
        # Determine the availability at the subsequent change points
        nodes = set(availatstart.keys())
        res = self.slottable.get_reservations_starting_after(self.time)
        changepoints = set()
        for rr in res:
            if nodes == None or (nodes != None and len(set(rr.resources_in_pnode.keys()) & set(nodes)) > 0):
                changepoints.add(rr.start)
        changepoints = list(changepoints)
        changepoints.sort()
        for p in changepoints:
            availatpoint = self.slottable.getAvailability(p, self.resreq, nodes, canpreempt)
            newnodes = set(availatpoint.keys())
            
            # Add entries for nodes that have no resources available
            # (for, at least, one VM)
            fullnodes = nodes - newnodes
            for node in fullnodes:
                self.avail[node].append(AvailEntry(p, None, None, None))
                nodes.remove(node)
                
            # For the rest, only interested if the available resources
            # Decrease in the window
            for node in newnodes:
                capacity = availatpoint[node].capacity
                fits = self.resreq.get_num_fits_in(capacity)
                if canpreempt:
                    capacitywithpreemption = availatpoint[node].capacitywithpreemption
                    fitswithpreemption = self.resreq.get_num_fits_in(capacitywithpreemption)
                prevavail = self.avail[node][-1]
                if not canpreempt and prevavail.getCanfit(canpreempt=False) > fits:
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                elif canpreempt and (prevavail.getCanfit(canpreempt=False) > fits or prevavail.getCanfit(canpreempt=True) > fitswithpreemption):
                    self.avail[node].append(AvailEntry(p, capacity, capacitywithpreemption, self.resreq))
                  
    
    def fitAtStart(self, nodes = None, canpreempt = False):
        if nodes != None:
            avail = [v for (k, v) in self.avail.items() if k in nodes]
        else:
            avail = self.avail.values()
        if canpreempt:
            return sum([e[0].canfitpreempt for e in avail])
        else:
            return sum([e[0].canfit for e in avail])
        
    # TODO: Also return the amount of resources that would have to be
    # preempted in each physnode
    def findPhysNodesForVMs(self, numnodes, maxend, strictend=False, canpreempt=False):
        # Returns the physical nodes that can run all VMs, and the
        # time at which the VMs must end
        canfit = dict([(n, v[0].getCanfit(canpreempt)) for (n, v) in self.avail.items()])
        entries = []
        for n in self.avail.keys():
            entries += [(n, e) for e in self.avail[n][1:]]
        getTime = lambda x: x[1].time
        entries.sort(key=getTime)
        if strictend:
            end = None
        else:
            end = maxend
        for e in entries:
            physnode = e[0]
            entry = e[1]
       
            if entry.time >= maxend:
                # Can run to its maximum duration
                break
            else:
                diff = canfit[physnode] - entry.getCanfit(canpreempt)
                totalcanfit = sum([n for n in canfit.values()]) - diff
                if totalcanfit < numnodes and not strictend:
                    # Not enough resources. Must end here
                    end = entry.time
                    break
                else:
                    # Update canfit
                    canfit[physnode] = entry.getCanfit(canpreempt)

        # Filter out nodes where we can't fit any vms
        canfit = dict([(n, v) for (n, v) in canfit.items() if v > 0])
        
        return end, canfit
            
                    
    def printContents(self, nodes = None, withpreemption = False):
        if self.logger.getEffectiveLevel() == constants.LOGLEVEL_VDEBUG:
            if nodes == None:
                physnodes = self.avail.keys()
            else:
                physnodes = [k for k in self.avail.keys() if k in nodes]
            physnodes.sort()
            if withpreemption:
                p = "(with preemption)"
            else:
                p = "(without preemption)"
            self.logger.vdebug("AVAILABILITY WINDOW (time=%s, nodes=%s) %s"%(self.time, nodes, p))
            for n in physnodes:
                contents = "Node %i --- " % n
                for x in self.avail[n]:
                    contents += "[ %s " % x.time
                    contents += "{ "
                    if x.avail == None and x.availpreempt == None:
                        contents += "END "
                    else:
                        if withpreemption:
                            res = x.availpreempt
                            canfit = x.canfitpreempt
                        else:
                            res = x.avail
                            canfit = x.canfit
                        contents += "%s" % res
                    contents += "} (Fits: %i) ]  " % canfit
                self.logger.vdebug(contents)
                

                
                          
                          
            


        
        