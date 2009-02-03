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

"""This module provides the lease data structures, and a couple of auxiliary
data structures.

* Lease data structures
  * Lease: Base class for leases
  * ARLease: Advance reservation lease
  * BestEffortLease: Best-effort lease
  * ImmediateLease: Immediate lease
* Miscellaneous structures
  * Timestamp: A wrapper around requested/scheduled/actual timestamps
  * Duration: A wrapper around requested/accumulated/actual durations
"""

from haizea.common.constants import RES_MEM, MIGRATE_NONE, MIGRATE_MEM, MIGRATE_MEMDISK, LOGLEVEL_VDEBUG
from haizea.common.utils import StateMachine, round_datetime_delta, get_lease_id, pretty_nodemap, estimate_transfer_time, xmlrpc_marshall_singlevalue

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
    STATE_SUSPENDED_PENDING = 10
    STATE_SUSPENDED_QUEUED = 11
    STATE_SUSPENDED_SCHEDULED = 12
    STATE_MIGRATING = 13
    STATE_RESUMING = 14
    STATE_RESUMED_READY = 15
    STATE_DONE = 16
    STATE_FAIL = 17
    
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
                 STATE_SUSPENDED_PENDING : "Suspended-Pending",
                 STATE_SUSPENDED_QUEUED : "Suspended-Queued",
                 STATE_SUSPENDED_SCHEDULED : "Suspended-Scheduled",
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
        self.state = LeaseStateMachine()
        self.diskimagemap = {}
        self.memimagemap = {}
        self.deployment_rrs = []
        self.vm_rrs = []

        # Enactment information. Should only be manipulated by enactment module
        self.enactment_info = None
        self.vnode_enactment_info = dict([(n+1, None) for n in range(numnodes)])
        
        self.logger = logging.getLogger("LEASES")
        
    def get_state(self):
        return self.state.get_state()
    
    def set_state(self, state):
        self.state.change_state(state)
        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        self.logger.log(loglevel, "Lease ID       : %i" % self.id)
        self.logger.log(loglevel, "Submission time: %s" % self.submit_time)
        self.logger.log(loglevel, "Duration       : %s" % self.duration)
        self.logger.log(loglevel, "State          : %s" % Lease.state_str[self.get_state()])
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
        l["state"] = self.get_state()
        l["vm_rrs"] = [vmrr.xmlrpc_marshall() for vmrr in self.vm_rrs]
                
        return l
        
class LeaseStateMachine(StateMachine):
    initial_state = Lease.STATE_NEW
    transitions = {Lease.STATE_NEW:                 [(Lease.STATE_PENDING,    "")],
                   
                   Lease.STATE_PENDING:             [(Lease.STATE_SCHEDULED,  ""),
                                                     (Lease.STATE_QUEUED,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_REJECTED,   "")],
                                                     
                   Lease.STATE_SCHEDULED:           [(Lease.STATE_PREPARING,  ""),
                                                     (Lease.STATE_READY,      ""),
                                                     (Lease.STATE_CANCELLED,  "")],
                                                     
                   Lease.STATE_QUEUED:              [(Lease.STATE_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  "")],
                                                     
                   Lease.STATE_PREPARING:           [(Lease.STATE_READY,      ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_READY:               [(Lease.STATE_ACTIVE,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_ACTIVE:              [(Lease.STATE_SUSPENDING, ""),
                                                     (Lease.STATE_DONE,       ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_SUSPENDING:          [(Lease.STATE_SUSPENDED_PENDING,  ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_SUSPENDED_PENDING:   [(Lease.STATE_SUSPENDED_QUEUED,     ""),
                                                     (Lease.STATE_SUSPENDED_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_SUSPENDED_QUEUED:    [(Lease.STATE_SUSPENDED_QUEUED,     ""),
                                                     (Lease.STATE_SUSPENDED_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_SUSPENDED_SCHEDULED: [(Lease.STATE_MIGRATING,  ""),
                                                     (Lease.STATE_RESUMING,   ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_MIGRATING:           [(Lease.STATE_SUSPENDED_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_RESUMING:            [(Lease.STATE_RESUMED_READY, ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_RESUMED_READY:       [(Lease.STATE_ACTIVE,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                   
                   # Final states
                   Lease.STATE_DONE:          [],
                   Lease.STATE_CANCELLED:     [],
                   Lease.STATE_FAIL:          [],
                   Lease.STATE_REJECTED:      [],
                   }
    
    def __init__(self):
        StateMachine.__init__(self, LeaseStateMachine.initial_state, LeaseStateMachine.transitions, Lease.state_str)
        
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
    
