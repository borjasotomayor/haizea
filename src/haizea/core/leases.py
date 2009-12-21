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

"""This module provides the lease data structures:

* Lease: Represents a lease
* LeaseStateMachine: A state machine to keep track of a lease's state
* Capacity: Used to represent a quantity of resources
* Timestamp: An exact moment in time
* Duration: A duration
* SoftwareEnvironment, UnmanagedSoftwareEnvironment, DiskImageSoftwareEnvironment:
  Used to represent a lease's required software environment.
* LeaseWorkload: Represents a collection of lease requests submitted
  in a specific order.
* Site: Represents the site with leasable resources.
* Nodes: Represents a collection of machines ("nodes"). This is used
  both when specifying a site and when specifying the machines
  needed by a leases.
"""

from haizea.common.constants import LOGLEVEL_VDEBUG
from haizea.common.utils import StateMachine, round_datetime_delta, get_lease_id
from haizea.core.scheduler.slottable import ResourceReservation

from mx.DateTime import DateTime, TimeDelta, Parser

import logging

try:
    import xml.etree.ElementTree as ET
except ImportError:
    # Compatibility with Python <=2.4
    import elementtree.ElementTree as ET 




class Lease(object):
    """A resource lease
    
    This is one of the main data structures used in Haizea. A lease
    is "a negotiated and renegotiable agreement between a resource 
    provider and a resource consumer, where the former agrees to make 
    a set of resources available to the latter, based on a set of 
    lease terms presented by the resource consumer". All the gory
    details on what this means can be found on the Haizea website
    and on the Haizea publications.
    
    See the __init__ method for a description of the information that
    is contained in a lease.
    
    """
    
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
    STATE_REJECTED_BY_USER = 18
    
    # String representation of lease states
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
                 STATE_FAIL : "Fail",
                 STATE_REJECTED_BY_USER : "Rejected by user"}
    
    # Lease types
    BEST_EFFORT = 1
    ADVANCE_RESERVATION = 2
    IMMEDIATE = 3
    DEADLINE = 4
    UNKNOWN = -1
    
    # String representation of lease types    
    type_str = {BEST_EFFORT: "Best-effort",
                ADVANCE_RESERVATION: "AR",
                IMMEDIATE: "Immediate",
                DEADLINE: "Deadline",
                UNKNOWN: "Unknown"}
    
    def __init__(self, lease_id, submit_time, user_id, requested_resources, start, duration, 
                 deadline, preemptible, software, state, extras = {}):
        """Constructs a lease.
        
        The arguments are the fundamental attributes of a lease.
        The attributes that are not specified by the arguments are
        the lease ID (which is an autoincremented integer), the
        lease state (a lease always starts out in state "NEW").
        A lease also has several bookkeeping attributes that are
        only meant to be consumed by other Haizea objects.
        
        Arguments:
        id -- Unique identifier for the lease. If None, one
        will be provided.
        submit_time -- The time at which the lease was submitted
        requested_resources -- A dictionary (int -> Capacity) mapping
          each requested node to a capacity (i.e., the amount of
          resources requested for that node)
        start -- A Timestamp object containing the requested time.
        duration -- A Duration object containing the requested duration.
        deadline -- A Timestamp object containing the deadline by which
          this lease must be completed.
        preemptible -- A boolean indicating whether this lease can be
          preempted or not.
        software -- A SoftwareEnvironment object specifying the
          software environment required by the lease.
        extras -- Extra attributes. Haizea will ignore them, but they
          may be used by pluggable modules.
        """        
        # Lease ID (read only)
        self.id = lease_id
        
        # Lease attributes
        self.submit_time = submit_time
        self.user_id = user_id
        self.requested_resources = requested_resources
        self.start = start
        self.duration = duration
        self.deadline = deadline
        self.preemptible = preemptible
        self.software = software
        self.price = None
        self.extras = extras

        # Bookkeeping attributes:

        # Lease state
        if state == None:
            state = Lease.STATE_NEW
        self.state = LeaseStateMachine(initial_state = state)

        # End of lease (recorded when the lease ends)
        self.end = None
        
        # Number of nodes requested in the lease
        self.numnodes = len(requested_resources)
        
        # The following two lists contain all the resource reservations
        # (or RRs) associated to this lease. These two lists are
        # basically the link between the lease and Haizea's slot table.
        
        # The preparation RRs are reservations that have to be
        # completed before a lease can first transition into a
        # READY state (e.g., image transfers)
        self.preparation_rrs = []
        # The VM RRs are reservations for the VMs that implement
        # the lease.
        self.vm_rrs = []

        # Enactment information. Should only be manipulated by enactment module
        self.enactment_info = None
        self.vnode_enactment_info = dict([(n, None) for n in self.requested_resources.keys()])
        
        
    @classmethod
    def create_new(cls, submit_time, user_id, requested_resources, start, duration, 
                 deadline, preemptible, software):
        lease_id = get_lease_id()
        state = Lease.STATE_NEW
        return cls(lease_id, submit_time, user_id, requested_resources, start, duration, 
                 deadline, preemptible, software, state)
        
    @classmethod
    def create_new_from_xml_element(cls, element):
        lease = cls.from_xml_element(element)
        if lease.id == None:
            lease.id = get_lease_id()
        lease.state = LeaseStateMachine(initial_state = Lease.STATE_NEW)
        return lease

    @classmethod
    def from_xml_file(cls, xml_file):
        """Constructs a lease from an XML file.
        
        See the Haizea documentation for details on the
        lease XML format.
        
        Argument:
        xml_file -- XML file containing the lease in XML format.
        """        
        return cls.from_xml_element(ET.parse(xml_file).getroot())

    @classmethod
    def from_xml_string(cls, xml_str):
        """Constructs a lease from an XML string.
        
        See the Haizea documentation for details on the
        lease XML format.
        
        Argument:
        xml_str -- String containing the lease in XML format.
        """        
        return cls.from_xml_element(ET.fromstring(xml_str))
        
    @classmethod
    def from_xml_element(cls, element):
        """Constructs a lease from an ElementTree element.
        
        See the Haizea documentation for details on the
        lease XML format.
        
        Argument:
        element -- Element object containing a "<lease>" element.
        """        
        
        lease_id = element.get("id")
        
        if lease_id == None:
            lease_id = None
        else:
            lease_id = int(lease_id)

        user_id = element.get("user")
        if user_id == None:
            user_id = None
        else:
            user_id = int(user_id)

        state = element.get("state")
        if state == None:
            state = None
        else:
            state = int(state)

        
        submit_time = element.get("submit-time")
        if submit_time == None:
            submit_time = None
        else:
            submit_time = Parser.DateTimeFromString(submit_time)
        
        nodes = Nodes.from_xml_element(element.find("nodes"))
        
        requested_resources = nodes.get_all_nodes()
        
        start = element.find("start")
        if len(start.getchildren()) == 0:
            start = Timestamp(Timestamp.UNSPECIFIED)
        else:
            child = start[0]
            if child.tag == "now":
                start = Timestamp(Timestamp.NOW)
            elif child.tag == "exact":
                start = Timestamp(Parser.DateTimeFromString(child.get("time")))
        
        duration = Duration(Parser.DateTimeDeltaFromString(element.find("duration").get("time")))

        deadline = element.find("deadline")
        
        if deadline != None:
            deadline = Parser.DateTimeFromString(deadline.get("time"))
        
        extra = element.find("extra")
        extras = {}
        if extra != None:
            for attr in extra:
                extras[attr.get("name")] = attr.get("value")

        
        preemptible = element.get("preemptible").capitalize()
        if preemptible == "True":
            preemptible = True
        elif preemptible == "False":
            preemptible = False
        
        software = element.find("software")
        
        if software.find("none") != None:
            software = UnmanagedSoftwareEnvironment()
        elif software.find("disk-image") != None:
            disk_image = software.find("disk-image")
            image_id = disk_image.get("id")
            image_size = int(disk_image.get("size"))
            software = DiskImageSoftwareEnvironment(image_id, image_size)
        
        return Lease(lease_id, submit_time, user_id, requested_resources, start, duration, 
                     deadline, preemptible, software, state, extras)


    def to_xml(self):
        """Returns an ElementTree XML representation of the lease
        
        See the Haizea documentation for details on the
        lease XML format.
        
        """        
        lease = ET.Element("lease")
        if self.id != None:
            lease.set("id", str(self.id))
        lease.set("state", str(self.get_state()))
        lease.set("preemptible", str(self.preemptible))
        if self.submit_time != None:
            lease.set("submit-time", str(self.submit_time))
        
        capacities = {}
        for capacity in self.requested_resources.values():
            key = capacity
            for c in capacities:
                if capacity == c:
                    key = c
                    break
            numnodes = capacities.setdefault(key, 0)
            capacities[key] += 1
        
        nodes = Nodes([(numnodes,c) for c,numnodes in capacities.items()])
        lease.append(nodes.to_xml())
        
        start = ET.SubElement(lease, "start")
        if self.start.requested == Timestamp.UNSPECIFIED:
            pass # empty start element
        elif self.start.requested == Timestamp.NOW:
            ET.SubElement(start, "now") #empty now element
        else:
            exact = ET.SubElement(start, "exact")
            exact.set("time", str(self.start.requested))
            
        duration = ET.SubElement(lease, "duration")
        duration.set("time", str(self.duration.requested))
        
        software = ET.SubElement(lease, "software")
        if isinstance(self.software, UnmanagedSoftwareEnvironment):
            ET.SubElement(software, "none")
        elif isinstance(self.software, DiskImageSoftwareEnvironment):
            imagetransfer = ET.SubElement(software, "disk-image")
            imagetransfer.set("id", self.software.image_id)
            imagetransfer.set("size", str(self.software.image_size))
            
        return lease

    def to_xml_string(self):
        """Returns a string XML representation of the lease
        
        See the Haizea documentation for details on the
        lease XML format.
        
        """   
        return ET.tostring(self.to_xml())

    def get_type(self):
        """Determines the type of lease
        
        Based on the lease's attributes, determines the lease's type.
        Can return Lease.BEST_EFFORT, Lease.ADVANCE_RESERVATION, or
        Lease.IMMEDIATE
        
        """
        if self.start.requested == Timestamp.UNSPECIFIED:
            return Lease.BEST_EFFORT
        elif self.start.requested == Timestamp.NOW:
            return Lease.IMMEDIATE            
        else:
            if self.deadline == None:
                return Lease.ADVANCE_RESERVATION
            else:
                return Lease.DEADLINE
        
    def get_state(self):
        """Returns the lease's state.
                
        """        
        return self.state.get_state()
    
    def set_state(self, state):
        """Changes the lease's state.
                
        The state machine will throw an exception if the 
        requested transition is illegal.
        
        Argument:
        state -- The new state
        """        
        self.state.change_state(state)
        
    def print_contents(self, loglevel=LOGLEVEL_VDEBUG):
        """Prints the lease's attributes to the log.
                
        Argument:
        loglevel -- The loglevel at which to print the information
        """           
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "__________________________________________________")
        logger.log(loglevel, "Lease ID       : %i" % self.id)
        logger.log(loglevel, "Type           : %s" % Lease.type_str[self.get_type()])
        logger.log(loglevel, "Submission time: %s" % self.submit_time)
        logger.log(loglevel, "Start          : %s" % self.start)
        logger.log(loglevel, "Duration       : %s" % self.duration)
        logger.log(loglevel, "Deadline       : %s" % self.deadline)
        logger.log(loglevel, "State          : %s" % Lease.state_str[self.get_state()])
        logger.log(loglevel, "Resource req   : %s" % self.requested_resources)
        logger.log(loglevel, "Software       : %s" % self.software)
        logger.log(loglevel, "Price          : %s" % self.price)
        logger.log(loglevel, "Extras         : %s" % self.extras)
        self.print_rrs(loglevel)
        logger.log(loglevel, "--------------------------------------------------")

    def print_rrs(self, loglevel=LOGLEVEL_VDEBUG):
        """Prints the lease's resource reservations to the log.
                
        Argument:
        loglevel -- The loglevel at which to print the information
        """            
        logger = logging.getLogger("LEASES")  
        if len(self.preparation_rrs) > 0:
            logger.log(loglevel, "DEPLOYMENT RESOURCE RESERVATIONS")
            logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            for r in self.preparation_rrs:
                r.print_contents(loglevel)
                logger.log(loglevel, "##")
        logger.log(loglevel, "VM RESOURCE RESERVATIONS")
        logger.log(loglevel, "~~~~~~~~~~~~~~~~~~~~~~~~")
        for r in self.vm_rrs:
            r.print_contents(loglevel)
            logger.log(loglevel, "##")

    def get_active_vmrrs(self, time):
        """Returns the active VM resource reservations at a given time
                
        Argument:
        time -- Time to look for active reservations
        """        
        return [r for r in self.vm_rrs if r.start <= time and time <= r.end and r.state == ResourceReservation.STATE_ACTIVE]

    def get_scheduled_reservations(self):
        """Returns all scheduled reservations
                
        """           
        return [r for r in self.preparation_rrs + self.vm_rrs if r.state == ResourceReservation.STATE_SCHEDULED]

    def get_last_vmrr(self):
        """Returns the last VM reservation for this lease.
                        
        """            
        return self.vm_rrs[-1]    

    def get_endtime(self):
        """Returns the time at which the last VM reservation 
        for this lease ends.
        
        Note that this is not necessarily the time at which the lease
        will end, just the time at which the last currently scheduled
        VM will end.
                
        """        
        vmrr = self.get_last_vmrr()
        return vmrr.end
    
    def append_vmrr(self, vmrr):
        """Adds a VM resource reservation to the lease.
        
        Argument:
        vmrr -- The VM RR to add.
        """             
        self.vm_rrs.append(vmrr)
        
    def remove_vmrr(self, vmrr):
        """Removes a VM resource reservation from the lease.
        
        Argument:
        vmrr -- The VM RR to remove.
        """           
        if not vmrr in self.vm_rrs:
            raise Exception, "Tried to remove an VM RR not contained in this lease"
        else:
            self.vm_rrs.remove(vmrr)
                    
    def append_preparationrr(self, preparation_rr):
        """Adds a preparation resource reservation to the lease.
        
        Argument:
        preparation_rr -- The preparation RR to add.
        """             
        self.preparation_rrs.append(preparation_rr)
        
    def remove_preparationrr(self, preparation_rr):
        """Removes a preparation resource reservation from the lease.
        
        Argument:
        preparation_rr -- The preparation RR to remove.
        """        
        if not preparation_rr in self.preparation_rrs:
            raise Exception, "Tried to remove a preparation RR not contained in this lease"
        else:
            self.preparation_rrs.remove(preparation_rr)        

    def clear_rrs(self):
        """Removes all resource reservations for this lease
        (both preparation and VM)
        
        """            
        del self.preparation_rrs 
        del self.vm_rrs 
        self.preparation_rrs = []
        self.vm_rrs = []

    def get_waiting_time(self):
        """Gets the waiting time for this lease.
        
        The waiting time is the difference between the submission
        time and the time at which the lease start. This method
        mostly makes sense for best-effort leases, where the
        starting time is determined by Haizea.
        
        """          
        return self.start.actual - self.submit_time
        
    def get_slowdown(self, bound=10):
        """Determines the bounded slowdown for this lease.
        
        Slowdown is a normalized measure of how much time a
        request takes to make it through a queue (thus, like
        get_waiting_time, the slowdown makes sense mostly for
        best-effort leases). Slowdown is equal to the time the
        lease took to run on a loaded system (i.e., a system where
        it had to compete with other leases for resources)
        divided by the time it would take if it just had the
        system all to itself (i.e., starts running immediately
        without having to wait in a queue and without the
        possibility of being preempted).
        
        "Bounded" slowdown is one where leases with very short
        durations are rounded up to a bound, to prevent the
        metric to be affected by reasonable but disproportionate
        waiting times (e.g., a 5-second lease with a 15 second
        waiting time -an arguably reasonable waiting time- has a 
        slowdown of 4, the same as 10 hour lease having to wait 
        30 hours for resources).
        
        Argument:
        bound -- The bound, specified in seconds.
        All leases with a duration less than this
        parameter are rounded up to the bound.
        """          
        time_on_dedicated = self.duration.original
        time_on_loaded = self.end - self.submit_time
        bound = TimeDelta(seconds=bound)
        if time_on_dedicated < bound:
            time_on_dedicated = bound
        return time_on_loaded / time_on_dedicated
        
    def add_boot_overhead(self, t):
        """Adds a boot overhead to the lease.
        
        Increments the requested duration to account for the fact 
        that some time will be spent booting up the resources.
        
        Argument:
        t -- Time to add
        """          
        self.duration.incr(t)        

    def add_runtime_overhead(self, percent):
        """Adds a runtime overhead to the lease.
        
        This method is mostly meant for simulations. Since VMs
        run slower than physical hardware, this increments the
        duration of a lease by a percent to observe the effect
        of having all the leases run slower on account of
        running on a VM.
        
        Note: the whole "runtime overhead" problem is becoming
        increasingly moot as people have lost their aversion to
        VMs thanks to the cloud computing craze. Anecdotal evidence
        suggests that most people don't care that VMs will run
        X % slower (compared to a physical machine) because they
        know full well that what they're getting is a virtual
        machine (the same way a user of an HPC system would know
        that he/she's getting processors with speed X as opposed to
        those on some other site, with speed X*0.10)
        
        Argument:
        percent -- Runtime overhead (in percent of requested
        duration) to add to the lease.
        """            
        self.duration.incr_by_percent(percent)
            
        
class LeaseStateMachine(StateMachine):
    """A lease state machine
    
    A child of StateMachine, this class simply specifies the valid
    states and transitions for a lease (the actual state machine code
    is in StateMachine).
    
    See the Haizea documentation for a description of states and
    valid transitions.
    
    """
    transitions = {Lease.STATE_NEW:                 [(Lease.STATE_PENDING,    "")],
                   
                   Lease.STATE_PENDING:             [(Lease.STATE_SCHEDULED,  ""),
                                                     (Lease.STATE_QUEUED,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_REJECTED,   ""),
                                                     (Lease.STATE_REJECTED_BY_USER,   "")],
                                                     
                   Lease.STATE_SCHEDULED:           [(Lease.STATE_PREPARING,  ""),
                                                     (Lease.STATE_QUEUED,     ""),
                                                     (Lease.STATE_PENDING,    ""),
                                                     (Lease.STATE_READY,      ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_QUEUED:              [(Lease.STATE_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  "")],
                                                     
                   Lease.STATE_PREPARING:           [(Lease.STATE_READY,      ""),
                                                     (Lease.STATE_PENDING,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_READY:               [(Lease.STATE_ACTIVE,     ""),
                                                     (Lease.STATE_QUEUED,     ""),
                                                     (Lease.STATE_PENDING,     ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_ACTIVE:              [(Lease.STATE_SUSPENDING, ""),
                                                     (Lease.STATE_QUEUED,     ""),
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
                                                     
                   Lease.STATE_SUSPENDED_QUEUED:    [(Lease.STATE_SUSPENDED_SCHEDULED,  ""),
                                                     (Lease.STATE_CANCELLED,  ""),
                                                     (Lease.STATE_FAIL,       "")],
                                                     
                   Lease.STATE_SUSPENDED_SCHEDULED: [(Lease.STATE_SUSPENDED_QUEUED,     ""),
                                                     (Lease.STATE_SUSPENDED_PENDING,  ""),
                                                     (Lease.STATE_MIGRATING,  ""),
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
    
    def __init__(self, initial_state):
        StateMachine.__init__(self, initial_state, LeaseStateMachine.transitions, Lease.state_str)


class Capacity(object):
    """A quantity of resources
    
    This class is used to represent a quantity of resources, such
    as those required by a lease. For example, if a lease needs a
    single node with 1 CPU and 1024 MB of memory, a single Capacity
    object would be used containing that information. 
    
    Resources in a Capacity object can be multi-instance, meaning
    that several instances of the same type of resources can be
    specified. For example, if a node requires 2 CPUs, then this is
    represented as two instances of the same type of resource. Most
    resources, however, will be "single instance" (e.g., a physical
    node only has "one" memory).
    
    Note: This class is similar, but distinct from, the ResourceTuple
    class in the slottable module. The ResourceTuple class can contain
    the same information, but uses a different internal representation
    (which is optimized for long-running simulations) and is tightly
    coupled to the SlotTable class. The Capacity and ResourceTuple
    classes are kept separate so that the slottable module remains
    independent from the rest of Haizea (in case we want to switch
    to a different slottable implementation in the future).
    
    """        
    def __init__(self, types):
        """Constructs an empty Capacity object.
        
        All resource types are initially set to be single-instance,
        with a quantity of 0 for each resource.
        
        Argument:
        types -- List of resource types. e.g., ["CPU", "Memory"]
        """          
        self.ninstances = dict([(res_type, 1) for res_type in types])
        self.quantity = dict([(res_type, [0]) for res_type in types])
        
    def get_ninstances(self, res_type):
        """Gets the number of instances for a resource type
                
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        """               
        return self.ninstances[res_type]
           
    def get_quantity(self, res_type):
        """Gets the quantity of a single-instance resource
                
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        """               
        return self.get_quantity_instance(res_type, 1)
    
    def get_quantity_instance(self, res_type, instance):
        """Gets the quantity of a specific instance of a 
        multi-instance resource.
                        
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        instance -- The instance. Note that instances are numbered
        from 1.
        """               
        return self.quantity[res_type][instance-1]

    def set_quantity(self, res_type, amount):
        """Sets the quantity of a single-instance resource
                
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        amount -- The amount to set the resource to.
        """            
        self.set_quantity_instance(res_type, 1, amount)
    
    def set_quantity_instance(self, res_type, instance, amount):
        """Sets the quantity of a specific instance of a 
        multi-instance resource.
                        
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        instance -- The instance. Note that instances are numbered
        from 1.
        amount -- The amount to set the instance of the resource to.
        """        
        self.quantity[res_type][instance-1] = amount
    
    def set_ninstances(self, res_type, ninstances):
        """Changes the number of instances of a resource type.
                        
        Note that changing the number of instances will initialize
        all the instances' amounts to zero. This method should
        only be called right after constructing a Capacity object.
        
        Argument:
        type -- The type of resource (using the same name passed
        when constructing the Capacity object)
        ninstance -- The number of instances
        """                
        self.ninstances[res_type] = ninstances
        self.quantity[res_type] = [0] * ninstances
       
    def get_resource_types(self):
        """Returns the types of resources in this capacity.
                        
        """            
        return self.quantity.keys()
    
    def __eq__(self, other):
        """Tests if two capacities are the same
                        
        """        
        for res_type in self.quantity:
            if not other.quantity.has_key(res_type):
                return False
            if self.ninstances[res_type] != other.ninstances[res_type]:
                return False
            if self.quantity[res_type] != other.quantity[res_type]:
                return False
        return True

    def __ne__(self, other):
        """Tests if two capacities are not the same
                        
        """        
        return not self == other
            
    def __repr__(self):
        """Returns a string representation of the Capacity"""
        return "  |  ".join("%s: %i" % (res_type,q[0]) for res_type, q in self.quantity.items())
            

class Timestamp(object):
    """An exact point in time.
    
    This class is just a wrapper around three DateTimes. When
    dealing with timestamps in Haizea (such as the requested
    starting time for a lease), we want to keep track not just
    of the requested timestamp, but also the scheduled timestamp
    (which could differ from the requested one) and the
    actual timestamp (which could differ from the scheduled one).
    """        
    
    UNSPECIFIED = "Unspecified"
    NOW = "Now"
    
    def __init__(self, requested):
        """Constructor
                        
        Argument:
        requested -- The requested timestamp
        """        
        self.requested = requested
        self.scheduled = None
        self.actual = None

    def __repr__(self):
        """Returns a string representation of the Duration"""
        return "REQ: %s  |  SCH: %s  |  ACT: %s" % (self.requested, self.scheduled, self.actual)
        
class Duration(object):
    """A duration
    
    This class is just a wrapper around five DateTimes. When
    dealing with durations in Haizea (such as the requested
    duration for a lease), we want to keep track of the following:
    
    - The requested duration
    - The accumulated duration (when the entire duration of
    the lease can't be scheduled without interrumption, this
    keeps track of how much duration has been fulfilled so far)
    - The actual duration (which might not be the same as the
    requested duration)
    
    For the purposes of simulation, we also want to keep track
    of the "original" duration (since the requested duration
    can be modified to simulate certain overheads) and the
    "known" duration (when simulating lease workloads, this is
    the actual duration of the lease, which is known a posteriori).
    """  
    
    def __init__(self, requested, known=None):
        """Constructor
                        
        Argument:
        requested -- The requested duration
        known -- The known duration (ONLY in simulation)
        """              
        self.original = requested
        self.requested = requested
        self.accumulated = TimeDelta()
        self.actual = None
        # The following is ONLY used in simulation
        self.known = known
        
    def incr(self, t):
        """Increments the requested duration by an amount.
                        
        Argument:
        t -- The time to add to the requested duration.
        """               
        self.requested += t
        if self.known != None:
            self.known += t
            
    def incr_by_percent(self, pct):
        """Increments the requested duration by a percentage.
                        
        Argument:
        pct -- The percentage of the requested duration to add.
        """          
        factor = 1 + float(pct)/100
        self.requested = round_datetime_delta(self.requested * factor)
        if self.known != None:
            self.requested = round_datetime_delta(self.known * factor)
        
    def accumulate_duration(self, t):
        """Increments the accumulated duration by an amount.
                        
        Argument:
        t -- The time to add to the accumulated duration.
        """        
        self.accumulated += t
            
    def get_remaining_duration(self):
        """Returns the amount of time required to fulfil the entire
        requested duration of the lease.
                        
        """         
        return self.requested - self.accumulated

    def get_remaining_known_duration(self):
        """Returns the amount of time required to fulfil the entire
        known duration of the lease.
              
        ONLY for simulations.
        """           
        return self.known - self.accumulated
            
    def __repr__(self):
        """Returns a string representation of the Duration"""
        return "REQ: %s  |  ACC: %s  |  ACT: %s  |  KNW: %s" % (self.requested, self.accumulated, self.actual, self.known)
    
class SoftwareEnvironment(object):
    """The base class for a lease's software environment"""
    
    def __init__(self):
        """Constructor.
        
        Does nothing."""
        pass

class UnmanagedSoftwareEnvironment(SoftwareEnvironment):
    """Represents an "unmanaged" software environment.
    
    When a lease has an unmanaged software environment,
    Haizea does not need to perform any actions to prepare
    a lease's software environment (it assumes that this
    task is carried out by an external entity, and that
    software environments can be assumed to be ready
    when a lease has to start; e.g., if VM disk images are
    predeployed on all physical nodes)."""
    
    def __init__(self):
        """Constructor.
        
        Does nothing."""        
        SoftwareEnvironment.__init__(self)

class DiskImageSoftwareEnvironment(SoftwareEnvironment):
    """Reprents a software environment encapsulated in a disk image.
    
    When a lease's software environment is contained in a disk image,
    this disk image must be deployed to the physical nodes the lease
    is mapped to before the lease can start. This means that the
    preparation for this lease must be handled by a preparation
    scheduler (see documentation in lease_scheduler) capable of
    handling a DiskImageSoftwareEnvironment.
    """
    def __init__(self, image_id, image_size):
        """Constructor.
        
        Arguments:
        image_id -- A unique identifier for the disk image required
        by the lease.
        image_size -- The size, in MB, of the disk image. """         
        self.image_id = image_id
        self.image_size = image_size
        SoftwareEnvironment.__init__(self)


    
class LeaseWorkload(object):
    """Reprents a sequence of lease requests.
    
    A lease workload is a sequence of lease requests with a specific
    arrival time for each lease. This class is currently only used
    to load LWF (Lease Workload File) files. See the Haizea documentation 
    for details on the LWF format.
    """    
    def __init__(self, leases):
        """Constructor.
        
        Arguments:
        leases -- An ordered list (by arrival time) of leases in the workload
        """                 
        self.leases = leases
        

    def get_leases(self):
        """Returns the leases in the workload.
        
        """  
        return self.leases
    
    @classmethod
    def from_xml_file(cls, xml_file, inittime = DateTime(0)):
        """Constructs a lease workload from an XML file.
        
        See the Haizea documentation for details on the
        lease workload XML format.
        
        Argument:
        xml_file -- XML file containing the lease in XML format.
        inittime -- The starting time of the lease workload. All relative
        times in the XML file will be converted to absolute times by
        adding them to inittime. If inittime is not specified, it will
        arbitrarily be 0000/01/01 00:00:00.
        """        
        return cls.__from_xml_element(ET.parse(xml_file).getroot(), inittime)

    @classmethod
    def __from_xml_element(cls, element, inittime):
        """Constructs a lease from an ElementTree element.
        
        See the Haizea documentation for details on the
        lease XML format.
        
        Argument:
        element -- Element object containing a "<lease-workload>" element.
        inittime -- The starting time of the lease workload. All relative
        times in the XML file will be converted to absolute times by
        adding them to inittime.  
        """                
        reqs = element.findall("lease-requests/lease-request")
        leases = []
        for r in reqs:
            lease = r.find("lease")
            # Add time lease is submitted
            submittime = inittime + Parser.DateTimeDeltaFromString(r.get("arrival"))
            lease.set("submit-time", str(submittime))
            
            # If an exact starting time is specified, add the init time
            exact = lease.find("start/exact")
            if exact != None:
                start = inittime + Parser.DateTimeDeltaFromString(exact.get("time"))
                exact.set("time", str(start))

            # If a deadline is specified, add the init time
            deadline = lease.find("deadline")
            if deadline != None:
                t = inittime + Parser.DateTimeDeltaFromString(deadline.get("time"))
                deadline.set("time", str(t))
                
            lease = Lease.create_new_from_xml_element(lease)
            
            realduration = r.find("realduration")
            if realduration != None:
                lease.duration.known = Parser.DateTimeDeltaFromString(realduration.get("time"))

            leases.append(lease)
            
        return cls(leases)
    
class LeaseAnnotation(object):
    """Reprents a lease annotation.
    
    ...
    """    
    def __init__(self, lease_id, start, deadline, software, extras):
        """Constructor.
        
        Arguments:
        ...
        """                 
        self.lease_id = lease_id
        self.start = start
        self.deadline = deadline
        self.software = software
        self.extras = extras
        
    
    @classmethod
    def from_xml_file(cls, xml_file):
        """...
        
        ...
        
        Argument:
        xml_file -- XML file containing the lease in XML format.
        """        
        return cls.__from_xml_element(ET.parse(xml_file).getroot())

    @classmethod
    def from_xml_element(cls, element):
        """...
        
        ...
        
        Argument:
        element -- Element object containing a "<lease-annotation>" element.
        """                
        lease_id = element.get("id")
      
        start = element.find("start")
        if start != None:
            if len(start.getchildren()) == 0:
                start = Timestamp(Timestamp.UNSPECIFIED)
            else:
                child = start[0]
                if child.tag == "now":
                    start = Timestamp(Timestamp.NOW)
                elif child.tag == "exact":
                    start = Timestamp(Parser.DateTimeDeltaFromString(child.get("time")))
        
        deadline = element.find("deadline")
        
        if deadline != None:
            deadline = Parser.DateTimeDeltaFromString(deadline.get("time"))
        
        extra = element.find("extra")
        extras = {}
        if extra != None:
            for attr in extra:
                extras[attr.get("name")] = attr.get("value")

        
        software = element.find("software")

        if software != None:
            if software.find("none") != None:
                software = UnmanagedSoftwareEnvironment()
            elif software.find("disk-image") != None:
                disk_image = software.find("disk-image")
                image_id = disk_image.get("id")
                image_size = int(disk_image.get("size"))
                software = DiskImageSoftwareEnvironment(image_id, image_size)
        
        return cls(lease_id, start, deadline, software, extras)
 
    
    def to_xml(self):
        """Returns an ElementTree XML representation of the lease annotation
        
        ...
        
        """        
        annotation = ET.Element("lease-annotation")
        annotation.set("id", str(self.lease_id))
        
        start = ET.SubElement(annotation, "start")
        if self.start.requested == Timestamp.UNSPECIFIED:
            pass # empty start element
        elif self.start.requested == Timestamp.NOW:
            ET.SubElement(start, "now") #empty now element
        else:
            exact = ET.SubElement(start, "exact")
            exact.set("time", "+" + str(self.start.requested))
            
        if self.deadline != None:
            deadline = ET.SubElement(annotation, "deadline")
            deadline.set("time", "+" + str(self.deadline))
        
        if self.software != None:
            software = ET.SubElement(annotation, "software")
            if isinstance(self.software, UnmanagedSoftwareEnvironment):
                ET.SubElement(software, "none")
            elif isinstance(self.software, DiskImageSoftwareEnvironment):
                imagetransfer = ET.SubElement(software, "disk-image")
                imagetransfer.set("id", self.software.image_id)
                imagetransfer.set("size", str(self.software.image_size))
            
        if len(self.extras) > 0:
            extras = ET.SubElement(annotation, "extra")
            for name, value in self.extras.items():
                attr = ET.SubElement(extras, "attr")
                attr.set("name", name)
                attr.set("value", value)
            
        return annotation

    def to_xml_string(self):
        """Returns a string XML representation of the lease annotation
        
        ...
        
        """   
        return ET.tostring(self.to_xml())    
    
class LeaseAnnotations(object):
    """Reprents a sequence of lease annotations.
    
    ...
    """    
    def __init__(self, annotations, attributes):
        """Constructor.
        
        Arguments:
        annotations -- A dictionary of annotations
        """                 
        self.annotations = annotations
        self.attributes = attributes
    
    def apply_to_leases(self, leases):
        """Apply annotations to a workload
        
        """
        for lease in [l for l in leases if self.has_annotation(l.id)]:
            annotation = self.get_annotation(lease.id)
            
            if annotation.start != None:
                if annotation.start.requested in (Timestamp.NOW, Timestamp.UNSPECIFIED):
                    lease.start.requested = annotation.start.requested
                else:
                    lease.start.requested = lease.submit_time + annotation.start.requested

            if annotation.deadline != None:
                lease.deadline = lease.submit_time + annotation.deadline

            if annotation.software != None:
                lease.software = annotation.software

            if annotation.extras != None:
                lease.extras.update(annotation.extras)

    def get_annotation(self, lease_id):
        """...
        
        """  
        return self.annotations[lease_id]

    def has_annotation(self, lease_id):
        """...
        
        """  
        return self.annotations.has_key(lease_id)
    
    @classmethod
    def from_xml_file(cls, xml_file):
        """...
        
        ...
        
        Argument:
        xml_file -- XML file containing the lease in XML format.
        """        
        return cls.__from_xml_element(ET.parse(xml_file).getroot())

    @classmethod
    def __from_xml_element(cls, element):
        """...
        
        ...
        
        Argument:
        element -- Element object containing a "<lease-annotations>" element.
        """                
        annotation_elems = element.findall("lease-annotation")
        annotations = {}
        for annotation_elem in annotation_elems:
            lease_id = int(annotation_elem.get("id"))
            annotations[lease_id] = LeaseAnnotation.from_xml_element(annotation_elem)
            
        attributes = {}
        attributes_elem = element.find("attributes")
        if attributes_elem != None:
            for attr_elem in attributes_elem:
                attributes[attr_elem.get("name")] = attr_elem.get("value")
            
        return cls(annotations, attributes)    
    
    def to_xml(self):
        """Returns an ElementTree XML representation of the lease
        
        See the Haizea documentation for details on the
        lease XML format.
        
        """        
        annotations = ET.Element("lease-annotations")

        attributes = ET.SubElement(annotations, "attributes")
        for name, value in self.attributes.items():
            attr_elem = ET.SubElement(attributes, "attr")
            attr_elem.set("name", name)
            attr_elem.set("value", value)

        for annotation in self.annotations.values():
            annotations.append(annotation.to_xml())
            
        return annotations

    def to_xml_string(self):
        """Returns a string XML representation of the lease
        
        See the Haizea documentation for details on the
        lease XML format.
        
        """   
        return ET.tostring(self.to_xml())    
        
class Site(object):
    """Represents a site containing machines ("nodes").
    
    This class is used to load site descriptions in XML format or
    using a "resources string". Site descriptions can appear in two places:
    in a LWF file (where the site required for the lease workload is
    embedded in the LWF file) or in the Haizea configuration file. In both
    cases, the site description is only used in simulation (in OpenNebula mode,
    the available nodes and resources are obtained by querying OpenNebula). 
    
    Note that this class is distinct from the ResourcePool class, even though
    both are used to represent "collections of nodes". The Site class is used
    purely as a convenient way to load site information from an XML file
    and to manipulate that information elsewhere in Haizea, while the
    ResourcePool class is responsible for sending enactment commands
    to nodes, monitoring nodes, etc.
    """        
    def __init__(self, nodes, resource_types, attr_types):
        """Constructor.
        
        Arguments:
        nodes -- A Nodes object
        resource_types -- A list of valid resource types in this site.
        attr_types -- A list of valid attribute types in this site
        """             
        self.nodes = nodes
        self.resource_types = resource_types
        self.attr_types = attr_types
        
    @classmethod
    def from_xml_file(cls, xml_file):
        """Constructs a site from an XML file.
        
        See the Haizea documentation for details on the
        site XML format.
        
        Argument:
        xml_file -- XML file containing the site in XML format.
        """                
        return cls.__from_xml_element(ET.parse(xml_file).getroot())        

    @classmethod
    def from_lwf_file(cls, lwf_file):
        """Constructs a site from an LWF file.
        
        LWF files can have site information embedded in them. This method
        loads this site information from an LWF file. See the Haizea 
        documentation for details on the LWF format.
        
        Argument:
        lwf_file -- LWF file.
        """                
        return cls.__from_xml_element(ET.parse(lwf_file).getroot().find("site"))        
        
    @classmethod
    def __from_xml_element(cls, element):     
        """Constructs a site from an ElementTree element.
        
        See the Haizea documentation for details on the
        site XML format.
        
        Argument:
        element -- Element object containing a "<site>" element.
        """     
        resource_types = element.find("resource-types")
        resource_types = resource_types.get("names").split()
       
        # TODO: Attributes
        attrs = []
        
        nodes = Nodes.from_xml_element(element.find("nodes"))

        # Validate nodes
        for node_set in nodes.node_sets:
            capacity = node_set[1]
            for resource_type in capacity.get_resource_types():
                if resource_type not in resource_types:
                    # TODO: Raise something more meaningful
                    raise Exception

        return cls(nodes, resource_types, attrs)
    
    @classmethod
    def from_resources_string(cls, resource_str):
        """Constructs a site from a "resources string"
        
        A "resources string" is a shorthand way of specifying a site
        with homogeneous resources and no attributes. The format is:
        
        <numnodes> <resource_type>:<resource_quantity>[,<resource_type>:<resource_quantity>]*
        
        For example: 4 CPU:100,Memory:1024
        
        Argument:
        resource_str -- resources string
        """    

        resource_str = resource_str.split()
        numnodes = int(resource_str[0])
        resources = resource_str[1:]
        res = {}
        
        for r in resources:
            res_type, amount = r.split(":")
            res[res_type] = int(amount)
            
        capacity = Capacity(res.keys())
        for (res_type, amount) in res.items():
            capacity.set_quantity(res_type, amount)
        
        nodes = Nodes([(numnodes,capacity)])

        return cls(nodes, res.keys(), [])
            
    def add_resource(self, name, amounts):
        """Adds a new resource to all nodes in the site.
                
        Argument:
        name -- Name of the resource type
        amounts -- A list with the amounts of the resource to add to each
        node. If the resource is single-instance, then this will just
        be a list with a single element. If multi-instance, each element
        of the list represent the amount of an instance of the resource.
        """            
        self.resource_types.append(name)
        self.nodes.add_resource(name, amounts)
    
    def get_resource_types(self):
        """Returns the resource types in this site.
        
        This method returns a list, each item being a pair with
        1. the name of the resource type and 2. the maximum number of
        instances for that resource type across all nodes.
                
        """               
        max_ninstances = dict((rt, 1) for rt in self.resource_types)
        for node_set in self.nodes.node_sets:
            capacity = node_set[1]
            for resource_type in capacity.get_resource_types():
                if capacity.ninstances[resource_type] > max_ninstances[resource_type]:
                    max_ninstances[resource_type] = capacity.ninstances[resource_type]
                    
        max_ninstances = [(rt,max_ninstances[rt]) for rt in self.resource_types]

        return max_ninstances
    


class Nodes(object):
    """Represents a collection of machines ("nodes")
    
    This class is used to load descriptions of nodes from an XML
    file. These nodes can appear in two places: in a site description
    (which, in turn, is loaded by the Site class) or in a lease's
    resource requirements (describing what nodes, with what resources,
    are required by the lease).
    
    Nodes are stored as one or more "node sets". Each node set has nodes
    with the exact same resources. So, for example, a lease requiring 100
    nodes (all identical, except 50 have 1024MB of memory and the other 50
    have 512MB of memory) doesn't need to enumerate all 100 nodes. Instead,
    it just has to describe the two "node sets" (indicating that there are
    50 nodes of one type and 50 of the other). See the Haizea documentation
    for more details on the XML format.
    
    Like the Site class, this class is distinct from the ResourcePool class, even
    though they both represent a "collection of nodes". See the 
    Site class documentation for more details.
    """            
    def __init__(self, node_sets):
        """Constructor.
        
        Arguments:
        node_sets -- A list of (n,c) pairs (where n is the number of nodes
        in the set and c is a Capacity object; all nodes in the set have
        capacity c).
        """                 
        self.node_sets = node_sets

    @classmethod
    def from_xml_element(cls, nodes_element):
        """Constructs a node collection from an ElementTree element.
        
        See the Haizea documentation for details on the
        <nodes> XML format.
        
        Argument:
        element -- Element object containing a "<nodes>" element.
        """           
        nodesets = []
        nodesets_elems = nodes_element.findall("node-set")
        for nodeset_elem in nodesets_elems:
            r = Capacity([])
            resources = nodeset_elem.findall("res")
            for i, res in enumerate(resources):
                res_type = res.get("type")
                if len(res.getchildren()) == 0:
                    amount = int(res.get("amount"))
                    r.set_ninstances(res_type, 1)
                    r.set_quantity(res_type, amount)
                else:
                    instances = res.findall("instance")
                    r.set_ninstances(type, len(instances))
                    for i, instance in enumerate(instances):
                        amount = int(instance.get("amount"))
                        r.set_quantity_instance(type, i+1, amount)
                                     
            numnodes = int(nodeset_elem.get("numnodes"))

            nodesets.append((numnodes,r))
            
        return cls(nodesets)
    
    def to_xml(self):
        """Returns an ElementTree XML representation of the nodes
        
        See the Haizea documentation for details on the
        lease XML format.
        
        """   
        nodes = ET.Element("nodes")
        for (numnodes, capacity) in self.node_sets:
            nodeset = ET.SubElement(nodes, "node-set")
            nodeset.set("numnodes", str(numnodes))
            for res_type in capacity.get_resource_types():
                res = ET.SubElement(nodeset, "res")
                res.set("type", res_type)
                ninstances = capacity.get_ninstances(res_type)
                if ninstances == 1:
                    res.set("amount", str(capacity.get_quantity(res_type)))                
            
        return nodes
    
    def get_all_nodes(self):
        """Returns a dictionary mapping individual nodes to capacities
        
        """              
        nodes = {}
        nodenum = 1
        for node_set in self.node_sets:
            numnodes = node_set[0]
            r = node_set[1]
            for i in range(numnodes):
                nodes[nodenum] = r
                nodenum += 1     
        return nodes   
                
    def add_resource(self, name, amounts):
        """Adds a new resource to all the nodes
                
        Argument:
        name -- Name of the resource type
        amounts -- A list with the amounts of the resource to add to each
        node. If the resource is single-instance, then this will just
        be a list with a single element. If multi-instance, each element
        of the list represent the amount of an instance of the resource.
        """              
        for node_set in self.node_sets:
            r = node_set[1]
            r.set_ninstances(name, len(amounts))
            for ninstance, amount in enumerate(amounts):
                r.set_quantity_instance(name, ninstance+1, amount)

