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
  * LeaseBase: Base class for leases
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

from haizea.common.constants import state_str, rstate_str, DS, RES_STATE_SCHEDULED, RES_STATE_ACTIVE, RES_MEM, MIGRATE_NONE, MIGRATE_MEM, MIGRATE_MEMDISK
from haizea.common.utils import roundDateTimeDelta, get_lease_id, pretty_nodemap, estimate_transfer_time, xmlrpc_marshall_singlevalue

from operator import attrgetter
from mx.DateTime import TimeDelta
from math import floor


#-------------------------------------------------------------------#
#                                                                   #
#                     LEASE DATA STRUCTURES                         #
#                                                                   #
#-------------------------------------------------------------------#


class LeaseBase(object):
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
        self.state = None
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
        # Enactment information. Should only be manipulated by enactment module
        self.enactment_info = None
        self.vnode_enactment_info = None

    # TODO: Remove the link to the scheduler, and pass all necessary information
    # as parameters to methods.
    def set_scheduler(self, scheduler):
        self.scheduler = scheduler
        self.logger = scheduler.rm.logger
        
    def print_contents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "Lease ID       : %i" % self.id, DS)
        self.logger.log(loglevel, "Submission time: %s" % self.submit_time, DS)
        self.logger.log(loglevel, "Duration       : %s" % self.duration, DS)
        self.logger.log(loglevel, "State          : %s" % state_str(self.state), DS)
        self.logger.log(loglevel, "VM image       : %s" % self.diskimage_id, DS)
        self.logger.log(loglevel, "VM image size  : %s" % self.diskimage_size, DS)
        self.logger.log(loglevel, "Num nodes      : %s" % self.numnodes, DS)
        self.logger.log(loglevel, "Resource req   : %s" % self.requested_resources, DS)
        self.logger.log(loglevel, "VM image map   : %s" % pretty_nodemap(self.vmimagemap), DS)
        self.logger.log(loglevel, "Mem image map  : %s" % pretty_nodemap(self.memimagemap), DS)

    def print_rrs(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "RESOURCE RESERVATIONS", DS)
        self.logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~", DS)
        for r in self.rr:
            r.print_contents(loglevel)
            self.logger.log(loglevel, "##", DS)
            
        
    def has_starting_reservations(self, time):
        return len(self.get_starting_reservations(time)) > 0

    def has_ending_reservations(self, time):
        return len(self.get_ending_reservations(time)) > 0

    def get_starting_reservations(self, time):
        return [r for r in self.rr if r.start <= time and r.state == RES_STATE_SCHEDULED]

    def get_ending_reservations(self, time):
        return [r for r in self.rr if r.end <= time and r.state == RES_STATE_ACTIVE]

    def get_active_reservations(self, time):
        return [r for r in self.rr if r.start <= time and time <= r.end and r.state == RES_STATE_ACTIVE]

    def get_scheduled_reservations(self):
        return [r for r in self.rr if r.state == RES_STATE_SCHEDULED]

    def get_endtime(self):
        vmrr, resrr = self.get_last_vmrr()
        return vmrr.end

    
    def append_rr(self, rr):
        self.rr.append(rr)

    def next_rrs(self, rr):
        return self.rr[self.rr.index(rr)+1:]

    def prev_rr(self, rr):
        return self.rr[self.rr.index(rr)-1]

    def get_last_vmrr(self):
        if isinstance(self.rr[-1],VMResourceReservation):
            return (self.rr[-1], None)
        elif isinstance(self.rr[-1],SuspensionResourceReservation):
            return (self.rr[-2], self.rr[-1])

    def replace_rr(self, rrold, rrnew):
        self.rr[self.rr.index(rrold)] = rrnew
    
    def remove_rr(self, rr):
        if not rr in self.rr:
            raise Exception, "Tried to remove an RR not contained in this lease"
        else:
            self.rr.remove(rr)

    def clear_rrs(self):
        self.rr = []
        
        
    def add_boot_overhead(self, t):
        self.duration.incr(t)        

    def add_runtime_overhead(self, percent):
        self.duration.incr_by_percent(percent)      
        
        
    def estimate_suspend_resume_time(self, rate):
        time = float(self.requested_resources.get_by_type(RES_MEM)) / rate
        time = roundDateTimeDelta(TimeDelta(seconds = time))
        return time
    
    # TODO: Factor out into deployment modules
    def estimate_image_transfer_time(self, bandwidth):
        forceTransferTime = self.scheduler.rm.config.get("force-imagetransfer-time")
        if forceTransferTime != None:
            return forceTransferTime
        else:      
            return estimate_transfer_time(self.diskimage_size, bandwidth)
        
    def estimate_migration_time(self, bandwidth):
        whattomigrate = self.scheduler.rm.config.get("what-to-migrate")
        if whattomigrate == MIGRATE_NONE:
            return TimeDelta(seconds=0)
        else:
            if whattomigrate == MIGRATE_MEM:
                mbtotransfer = self.requested_resources.get_by_type(RES_MEM)
            elif whattomigrate == MIGRATE_MEMDISK:
                mbtotransfer = self.diskimage_size + self.requested_resources.get_by_type(RES_MEM)
            return estimate_transfer_time(mbtotransfer, bandwidth)
        
    # TODO: This whole function has to be rethought
    def get_suspend_threshold(self, initial, suspendrate, migrating=False, bandwidth=None):
        threshold = self.scheduler.rm.config.get("force-suspend-threshold")
        if threshold != None:
            # If there is a hard-coded threshold, use that
            return threshold
        else:
            # deploytime needs to be factored out of here
#            transfertype = self.scheduler.rm.config.getTransferType()
#            if transfertype == TRANSFER_NONE:
#                deploytime = TimeDelta(seconds=0)
#            else: 
#                deploytime = self.estimateImageTransferTime()
            # The threshold will be a multiple of the overhead
            if not initial:
                # Overestimating, just in case (taking into account that the lease may be
                # resumed, but also suspended again)
                if migrating:
                    threshold = self.estimate_suspend_resume_time(suspendrate) * 2
                    #threshold = self.estimate_migration_time(bandwidth) + self.estimate_suspend_resume_time(suspendrate) * 2
                else:
                    threshold = self.estimate_suspend_resume_time(suspendrate) * 2
            else:
                #threshold = self.scheduler.rm.config.getBootOverhead() + deploytime + self.estimateSuspendResumeTime(suspendrate)
                threshold = self.scheduler.rm.config.get("bootshutdown-overhead") + self.estimate_suspend_resume_time(suspendrate)
            factor = self.scheduler.rm.config.get("suspend-threshold-factor") + 1
            return roundDateTimeDelta(threshold * factor)
        
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
        l["rr"] = [rr.xmlrpc_marshall() for rr in self.rr]
        return l
        
        
class ARLease(LeaseBase):
    def __init__(self, submit_time, start, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # AR-specific parameters:
                 realdur = None):
        start = Timestamp(start)
        duration = Duration(duration)
        duration.known = realdur # ONLY for simulation
        LeaseBase.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)
        
    def print_contents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : AR", DS)
        self.logger.log(loglevel, "Start time     : %s" % self.start, DS)
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)
    
    def xmlrpc_marshall(self):
        l = LeaseBase.xmlrpc_marshall(self)
        l["type"] = "AR"
        return l

        
class BestEffortLease(LeaseBase):
    def __init__(self, submit_time, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # BE-specific parameters:
                 realdur = None):
        start = Timestamp(None) # i.e., start on a best-effort basis
        duration = Duration(duration)
        duration.known = realdur # ONLY for simulation
        # When the images will be available
        self.imagesavail = None        
        LeaseBase.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)

    def print_contents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : BEST-EFFORT", DS)
        self.logger.log(loglevel, "Images Avail @ : %s" % self.imagesavail, DS)
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)
        
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
        l = LeaseBase.xmlrpc_marshall(self)
        l["type"] = "BE"
        return l


class ImmediateLease(LeaseBase):
    def __init__(self, submit_time, duration, diskimage_id, 
                 diskimage_size, numnodes, resreq, preemptible,
                 # Immediate-specific parameters:
                 realdur = None):
        start = Timestamp(None) # i.e., start on a best-effort basis
        duration = Duration(duration)
        duration.known = realdur # ONLY for simulation
        LeaseBase.__init__(self, submit_time, start, duration, diskimage_id,
                           diskimage_size, numnodes, resreq, preemptible)

    def print_contents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "__________________________________________________", DS)
        LeaseBase.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : IMMEDIATE", DS)
        self.print_rrs(loglevel)
        self.logger.log(loglevel, "--------------------------------------------------", DS)

    def xmlrpc_marshall(self):
        l = LeaseBase.xmlrpc_marshall(self)
        l["type"] = "IM"
        return l


#-------------------------------------------------------------------#
#                                                                   #
#                        RESOURCE RESERVATION                       #
#                          DATA STRUCTURES                          #
#                                                                   #
#-------------------------------------------------------------------#

        
class ResourceReservationBase(object):
    def __init__(self, lease, start, end, res):
        self.lease = lease
        self.start = start
        self.end = end
        self.state = None
        self.resources_in_pnode = res
        self.logger = lease.scheduler.rm.logger
                        
    def print_contents(self, loglevel="EXTREMEDEBUG"):
        self.logger.log(loglevel, "Start          : %s" % self.start, DS)
        self.logger.log(loglevel, "End            : %s" % self.end, DS)
        self.logger.log(loglevel, "State          : %s" % rstate_str(self.state), DS)
        self.logger.log(loglevel, "Resources      : \n%s" % "\n".join(["N%i: %s" %(i, x) for i, x in self.resources_in_pnode.items()]), DS) 
                
    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        rr = {}                
        rr["start"] = xmlrpc_marshall_singlevalue(self.start)
        rr["end"] = xmlrpc_marshall_singlevalue(self.end)
        rr["state"] = self.state
        return rr
                
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, nodes, res, oncomplete, backfill_reservation):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes
        self.oncomplete = oncomplete
        self.backfill_reservation = backfill_reservation

        # ONLY for simulation
        if lease.duration.known != None:
            remdur = lease.duration.get_remaining_known_duration()
            rrdur = self.end - self.start
            if remdur < rrdur:
                self.prematureend = self.start + remdur
            else:
                self.prematureend = None
        else:
            self.prematureend = None 

    def print_contents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.print_contents(self, loglevel)
        if self.prematureend != None:
            self.logger.log(loglevel, "Premature end  : %s" % self.prematureend, DS)
        self.logger.log(loglevel, "Type           : VM", DS)
        self.logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes), DS)
        self.logger.log(loglevel, "On Complete    : %s" % self.oncomplete, DS)
        
    def is_preemptible(self):
        return self.lease.preemptible

    def xmlrpc_marshall(self):
        rr = ResourceReservationBase.xmlrpc_marshall(self)
        rr["type"] = "VM"
        rr["nodes"] = self.nodes.items()
        return rr

        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, res, nodes):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes

    def print_contents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : SUSPEND", DS)
        self.logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes), DS)
        
    # TODO: Suspension RRs should be preemptible, but preempting a suspension RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a suspension RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservationBase.xmlrpc_marshall(self)
        rr["type"] = "SUSP"
        rr["nodes"] = self.nodes.items()
        return rr
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, lease, start, end, res, nodes):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.nodes = nodes

    def print_contents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : RESUME", DS)
        self.logger.log(loglevel, "Nodes          : %s" % pretty_nodemap(self.nodes), DS)

    # TODO: Suspension RRs should be preemptible, but preempting a suspension RR
    # has wider implications (with a non-trivial handling). For now, we leave them 
    # as non-preemptible, since the probability of preempting a suspension RR is slim.
    def is_preemptible(self):
        return False        
        
    def xmlrpc_marshall(self):
        rr = ResourceReservationBase.xmlrpc_marshall(self)
        rr["type"] = "RESM"
        rr["nodes"] = self.nodes.items()
        return rr

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
    
    # TODO: Should be moved to slottable module
    def getNextLeasesScheduledInNodes(self, time, nodes):
        nodes = set(nodes)
        leases = []
        earliestEndTime = {}
        for l in self.entries.values():
            start = l.rr[-1].start
            nodes2 = set(l.rr[-1].nodes.values())
            if len(nodes & nodes2) > 0 and start > time:
                leases.append(l)
                end = l.rr[-1].end
                for n in nodes2:
                    if not earliestEndTime.has_key(n):
                        earliestEndTime[n] = end
                    else:
                        if end < earliestEndTime[n]:
                            earliestEndTime[n] = end
        leases2 = set()
        for n in nodes:
            if earliestEndTime.has_key(n):
                end = earliestEndTime[n]
                l = [l for l in leases if n in l.rr[-1].nodes.values() and l.rr[-1].start < end]
                leases2.update(l)
        return list(leases2)
    
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
        self.requested = roundDateTimeDelta(self.requested * factor)
        if self.known != None:
            self.requested = roundDateTimeDelta(self.known * factor)
        
    def accumulate_duration(self, t):
        self.accumulated += t
            
    def get_remaining_duration(self):
        return self.requested - self.accumulated

    # ONLY in simulation
    def get_remaining_known_duration(self):
        return self.known - self.accumulated
            
    def __repr__(self):
        return "REQ: %s  |  ACC: %s  |  ACT: %s  |  KNW: %s" % (self.requested, self.accumulated, self.actual, self.known)
    
