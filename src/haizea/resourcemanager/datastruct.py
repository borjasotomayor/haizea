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

"""This module provides the fundamental data structures (besides the slot table,
which is in a module of its own) used by Haizea. The module provides four types
of structures:

* Lease data structures
  * Lease: Base class for leases
  * ARLease: Advance reservation lease
  * BestEffortLease: Best-effort lease
  * ImmediateLease: Immediate lease
* Resource reservation (RR) structures:
  * ResourceReservationBase: Base class for RRs in the slot table
  * VMResourceReservation: RR representing one or more VMs
  * SuspensionResourceReservation: RR representing a lease suspension
  * ResumptionResourceReservation: RR representing a lease resumption
* Lease containers
  * Queue: Your run-of-the-mill queue
  * LeaseTable: Provides easy access to leases in the system
* Miscellaneous structures
  * ResourceTuple: A tuple representing a resource usage or requirement
  * Timestamp: A wrapper around requested/scheduled/actual timestamps
  * Duration: A wrapper around requested/accumulated/actual durations
"""

from haizea.common.constants import RES_MEM, MIGRATE_NONE, MIGRATE_MEM, MIGRATE_MEMDISK, LOGLEVEL_VDEBUG
from haizea.common.utils import round_datetime_delta, get_lease_id, pretty_nodemap, estimate_transfer_time, xmlrpc_marshall_singlevalue

from operator import attrgetter
from mx.DateTime import TimeDelta
from math import floor

import logging


#-------------------------------------------------------------------#
#                                                                   #
#                     LEASE DATA STRUCTURES                         #
#                                                                   #
#-------------------------------------------------------------------#


class Lease(object):
    # Lease states
    STATE_NEW = 0
    STATE_PENDING = 1
    STATE_REJECTED = 2
    STATE_SCHEDULED = 3
    STATE_QUEUED = 4
    STATE_CANCELLED = 5
    STATE_PREPARING = 6
    STATE_READY = 7
    STATE_ACTIVE = 8
    STATE_SUSPENDING = 9
    STATE_SUSPENDED = 10
    STATE_MIGRATING = 11
    STATE_RESUMING = 12
    STATE_RESUMED_READY = 13
    STATE_DONE = 14
    STATE_FAIL = 15
    
    state_str = {STATE_NEW : "New",
                 STATE_PENDING : "Pending",
                 STATE_REJECTED : "Rejected",
                 STATE_SCHEDULED : "Scheduled",
                 STATE_QUEUED : "Queued",
                 STATE_CANCELLED : "Cancelled",
                 STATE_PREPARING : "Preparing",
                 STATE_READY : "Ready",
                 STATE_ACTIVE : "Active",
                 STATE_SUSPENDING : "Suspending",
                 STATE_SUSPENDED : "Suspended",
                 STATE_MIGRATING : "Migrating",
                 STATE_RESUMING : "Resuming",
                 STATE_RESUMED_READY: "Resumed-Ready",
                 STATE_DONE : "Done",
                 STATE_FAIL : "Fail"}
    
    def __init__(self, submit_time, start, duration, diskimage_id, 
                 diskimage_size, numnodes, requested_resources, preemptible):
        # Lease ID (read only)
        self.id = get_lease_id()
        
        # Request attributes (read only)
        self.submit_time = submit_time
        self.start = start
        self.duration = duration
        self.end = None
        self.diskimage_id = diskimage_id
        self.diskimage_size = diskimage_size
        # TODO: The following assumes homogeneous nodes. Should be modified
        # to account for heterogeneous nodes.
        self.numnodes = numnodes
        self.requested_resources = requested_resources
        self.preemptible = preemptible

        # Bookkeeping attributes
        # (keep track of the lease's state, resource reservations, etc.)
        self.state = Lease.STATE_NEW
        self.diskimagemap = {}
        self.memimagemap = {}
        self.deployment_rrs = []
        self.vm_rrs = []

        # Enactment information. Should only be manipulated by enactment module
        self.enactment_info = None
        self.vnode_enactment_info = dict([(n+1, None) for n in range(numnodes)])
        
        self.logger = logging.getLogger("LEASES")
        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Lease ID       : %i" % self.id)
        self.logger.log(loglevel, "Submission time: %s" % self.submit_time)
        self.logger.log(loglevel, "Duration       : %s" % self.duration)
        self.logger.log(loglevel, "State          : %s" % Lease.state_str[self.state])
        self.logger.log(loglevel, "Disk image     : %s" % self.diskimage_id)
        self.logger.log(loglevel, "Disk image size: %s" % self.diskimage_size)
        self.logger.log(loglevel, "Num nodes      : %s" % self.numnodes)
        self.logger.log(loglevel, "Resource req   : %s" % self.requested_resources)
        self.logger.log(loglevel, "Disk image map : %s" % pretty_nodemap(self.diskimagemap))
        self.logger.log(loglevel, "Mem image map  : %s" % pretty_nodemap(self.memimagemap))

    def print_rrs(self, loglevel=LOGLEVEL_VDEBUG):
        if len(self.deployment_rrs) > 0:
            self.logger.log(loglevel, "DEPLOYMENT RESOURCE RESERVATIONS")
            self.logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            for r in self.deployment_rrs:
                r.print_contents(loglevel)
                self.logger.log(loglevel, "##")
        self.logger.log(loglevel, "VM RESOURCE RESERVATIONS")
        self.logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~~~~")
        for r in self.vm_rrs:
            r.print_contents(loglevel)
            self.logger.log(loglevel, "##")

    def get_endtime(self):
        vmrr = self.get_last_vmrr()
        return vmrr.end

    def append_vmrr(self, vmrr):
        self.vm_rrs.append(vmrr)
        
    def append_deployrr(self, vmrr):
        self.deployment_rrs.append(vmrr)

    def get_last_vmrr(self):
        return self.vm_rrs[-1]

    def update_vmrr(self, rrold, rrnew):
        self.vm_rrs[self.vm_rrs.index(rrold)] = rrnew
    
    def remove_vmrr(self, vmrr):
        if not vmrr in self.vm_rrs:
            raise Exception, "Tried to remove an VM RR not contained in this lease"
        else:
            self.vm_rrs.remove(vmrr)

    def clear_rrs(self):
        self.deployment_rrs = []
        self.vm_rrs = []
        
    def add_boot_overhead(self, t):
        self.duration.incr(t)        

    def add_runtime_overhead(self, percent):
        self.duration.incr_by_percent(percent)
        
    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        l = {}
        l["id"] = self.id
        l["submit_time"] = xmlrpc_marshall_singlevalue(self.submit_time)
        l["start_req"] = xmlrpc_marshall_singlevalue(self.start.requested)
        l["start_sched"] = xmlrpc_marshall_singlevalue(self.start.scheduled)
        l["start_actual"] = xmlrpc_marshall_singlevalue(self.start.actual)
        l["duration_req"] = xmlrpc_marshall_singlevalue(self.duration.requested)
        l["duration_acc"] = xmlrpc_marshall_singlevalue(self.duration.accumulated)
        l["duration_actual"] = xmlrpc_marshall_singlevalue(self.duration.actual)
        l["end"] = xmlrpc_marshall_singlevalue(self.end)
        l["diskimage_id"] = self.diskimage_id
        l["diskimage_size"] = self.diskimage_size
        l["numnodes"] = self.numnodes
        l["resources"] = `self.requested_resources`
        l["preemptible"] = self.preemptible
        l["state"] = self.state
        l["vm_rrs"] = [vmrr.xmlrpc_marshall() for vmrr in self.vm_rrs]
                
        return l
        
        
class ARLease(Lease):
    def __init__(self, submit_time, start, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # AR-specific parameters:
                 realdur = None):
        start = Timestamp(start)
        duration = Duration(duration)
        if realdur != duration.requested:
            duration.known = realdur # ONLY for simulation
        Lease.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)
        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "__________________________________________________")
        Lease.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : AR")
        self.logger.log(loglevel, "Start time     : %s" % self.start)
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------")
    
    def xmlrpc_marshall(self):
        l = Lease.xmlrpc_marshall(self)
        l["type"] = "AR"
        return l

        
class BestEffortLease(Lease):
    def __init__(self, submit_time, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # BE-specific parameters:
                 realdur = None):
        start = Timestamp(None) # i.e., start on a best-effort basis
        duration = Duration(duration)
        if realdur != duration.requested:
            duration.known = realdur # ONLY for simulation
        # When the images will be available
        self.imagesavail = None        
        Lease.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "__________________________________________________")
        Lease.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : BEST-EFFORT")
        self.logger.log(loglevel, "Images Avail @ : %s" % self.imagesavail)
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------")
        
    def get_waiting_time(self):
        return self.start.actual - self.submit_time
        
    def get_slowdown(self, bound=10):
        time_on_dedicated = self.duration.original
        time_on_loaded = self.end - self.submit_time
        bound = TimeDelta(seconds=bound)
        if time_on_dedicated < bound:
            time_on_dedicated = bound
        return time_on_loaded / time_on_dedicated

    def xmlrpc_marshall(self):
        l = Lease.xmlrpc_marshall(self)
        l["type"] = "BE"
        return l


class ImmediateLease(Lease):
    def __init__(self, submit_time, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # Immediate-specific parameters:
                 realdur = None):
        start = Timestamp(None) # i.e., start on a best-effort basis
        duration = Duration(duration)
        duration.known = realdur # ONLY for simulation
        Lease.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "__________________________________________________")
        Lease.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : IMMEDIATE")
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------")

    def xmlrpc_marshall(self):
        l = Lease.xmlrpc_marshall(self)
        l["type"] = "IM"
        return l


#-------------------------------------------------------------------#
#                                                                   #
#                        RESOURCE RESERVATION                       #
#                          DATA STRUCTURES                          #
#                                                                   #
#-------------------------------------------------------------------#

        
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
                        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
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
                
class VMResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, nodes, res, backfill_reservation):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.nodes = nodes # { vnode -> pnode }
        self.backfill_reservation = backfill_reservation
        self.pre_rrs = []
        self.post_rrs = []

        # ONLY for simulation
        self.__update_prematureend()

    def update_start(self, time):
        self.start = time
        # ONLY for simulation
        self.__update_prematureend()

    def update_end(self, time):
        self.end = time
        # ONLY for simulation
        self.__update_prematureend()
        
    # ONLY for simulation
    def __update_prematureend(self):
        if self.lease.duration.known != None:
            remdur = self.lease.duration.get_remaining_known_duration()
            rrdur = self.end - self.start
            if remdur < rrdur:
                self.prematureend = self.start + remdur
            else:
                self.prematureend = None
        else:
            self.prematureend = None 

    def get_final_end(self):
        if len(self.post_rrs) == 0:
            return self.end
        else:
            return self.post_rrs[-1].end

    def is_suspending(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], SuspensionResourceReservation)

    def is_shutting_down(self):
        return len(self.post_rrs) > 0 and isinstance(self.post_rrs[0], ShutdownResourceReservation)

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        for resmrr in self.pre_rrs:
            resmrr.print_contents(loglevel)
            self.logger.log(loglevel, "--")
        self.logger.log(loglevel, "Type           : VM")
        self.logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes))
        if self.prematureend != None:
            self.logger.log(loglevel, "Premature end  : %s" % self.prematureend)
        ResourceReservation.print_contents(self, loglevel)
        for susprr in self.post_rrs:
            self.logger.log(loglevel, "--")
            susprr.print_contents(loglevel)
        
    def is_preemptible(self):
        return self.lease.preemptible

    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "VM"
        rr["nodes"] = self.nodes.items()
        return rr

        
class SuspensionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : SUSPEND")
        self.logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)
        
    def is_first(self):
        return (self == self.vmrr.post_rrs[0])

    def is_last(self):
        return (self == self.vmrr.post_rrs[-1])
        
    # TODO: Suspension RRs should be preemptible, but preempting a suspension RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a suspension RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "SUSP"
        return rr
        
class ResumptionResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : RESUME")
        self.logger.log(loglevel, "Vnodes         : %s" % self.vnodes)
        ResourceReservation.print_contents(self, loglevel)

    def is_first(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[0])

    def is_last(self):
        resm_rrs = [r for r in self.vmrr.pre_rrs if isinstance(r, ResumptionResourceReservation)]
        return (self == resm_rrs[-1])

    # TODO: Resumption RRs should be preemptible, but preempting a resumption RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a resumption RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "RESM"
        return rr
    
class ShutdownResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vnodes, vmrr):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.vnodes = vnodes

    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : SHUTDOWN")
        ResourceReservation.print_contents(self, loglevel)
        
    def is_preemptible(self):
        return True        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservation.xmlrpc_marshall(self)
        rr["type"] = "SHTD"
        return rr

class MigrationResourceReservation(ResourceReservation):
    def __init__(self, lease, start, end, res, vmrr, transfers):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.vmrr = vmrr
        self.transfers = transfers
        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Type           : MIGRATE")
        self.logger.log(loglevel, "Transfers      : %s" % self.transfers)
        ResourceReservation.print_contents(self, loglevel)        

    def is_preemptible(self):
        return False        

#-------------------------------------------------------------------#
#                                                                   #
#                         LEASE CONTAINERS                          #
#                                                                   #
#-------------------------------------------------------------------#
        
class Queue(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.__q = []
        
    def is_empty(self):
        return len(self.__q)==0
    
    def enqueue(self, r):
        self.__q.append(r)
    
    def dequeue(self):
        return self.__q.pop(0)
    
    def enqueue_in_order(self, r):
        self.__q.append(r)
        self.__q.sort(key=attrgetter("submit_time"))

    def length(self):
        return len(self.__q)
    
    def has_lease(self, lease_id):
        return (1 == len([l for l in self.__q if l.id == lease_id]))
    
    def get_lease(self, lease_id):
        return [l for l in self.__q if l.id == lease_id][0]
    
    def remove_lease(self, lease):
        self.__q.remove(lease)
    
    def __iter__(self):
        return iter(self.__q)
        
class LeaseTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.entries = {}
        
    def has_lease(self, lease_id):
        return self.entries.has_key(lease_id)
        
    def get_lease(self, lease_id):
        return self.entries[lease_id]
    
    def is_empty(self):
        return len(self.entries)==0
    
    def remove(self, lease):
        del self.entries[lease.id]
        
    def add(self, lease):
        self.entries[lease.id] = lease
        
    def get_leases(self, type=None):
        if type==None:
            return self.entries.values()
        else:
            return [e for e in self.entries.values() if isinstance(e, type)]

    def get_leases_by_state(self, state):
        return [e for e in self.entries.values() if e.state == state]
    
#-------------------------------------------------------------------#
#                                                                   #
#             MISCELLANEOUS DATA STRUCTURES CONTAINERS              #
#                                                                   #
#-------------------------------------------------------------------#    
    
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

class Timestamp(object):
    def __init__(self, requested):
        self.requested = requested
        self.scheduled = None
        self.actual = None

    def __repr__(self):
        return "REQ: %s  |  SCH: %s  |  ACT: %s" % (self.requested, self.scheduled, self.actual)
        
class Duration(object):
    def __init__(self, requested, known=None):
        self.original = requested
        self.requested = requested
        self.accumulated = TimeDelta()
        self.actual = None
        # The following is ONLY used in simulation
        self.known = known
        
    def incr(self, t):
        self.requested += t
        if self.known != None:
            self.known += t
            
    def incr_by_percent(self, pct):
        factor = 1 + float(pct)/100
        self.requested = round_datetime_delta(self.requested * factor)
        if self.known != None:
            self.requested = round_datetime_delta(self.known * factor)
        
    def accumulate_duration(self, t):
        self.accumulated += t
            
    def get_remaining_duration(self):
        return self.requested - self.accumulated

    # ONLY in simulation
    def get_remaining_known_duration(self):
        return self.known - self.accumulated
            
    def __repr__(self):
        return "REQ: %s  |  ACC: %s  |  ACT: %s  |  KNW: %s" % (self.requested, self.accumulated, self.actual, self.known)
    
