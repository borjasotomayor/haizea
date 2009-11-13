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
from haizea.core.leases import Lease, Capacity, Timestamp, Duration, UnmanagedSoftwareEnvironment
from haizea.core.frontends import RequestFrontend
from haizea.common.utils import UNIX2DateTime, round_datetime, get_config, OpenNebulaXMLRPCClientSingleton
from haizea.common.opennebula_xmlrpc import OpenNebulaVM
from mx.DateTime import DateTimeDelta, ISO

import operator
import logging

class OpenNebulaHaizeaVM(object):
    HAIZEA_PARAM = "HAIZEA"
    HAIZEA_START = "START"
    HAIZEA_START_NOW = "now"
    HAIZEA_START_BESTEFFORT = "best_effort"
    HAIZEA_DURATION = "DURATION"
    HAIZEA_DURATION_UNLIMITED = "unlimited"
    HAIZEA_PREEMPTIBLE = "PREEMPTIBLE"
    HAIZEA_PREEMPTIBLE_YES = "yes"
    HAIZEA_PREEMPTIBLE_NO = "no"
    HAIZEA_GROUP = "GROUP"
  
    
    def __init__(self, opennebula_vm):                        
        # If there is no HAIZEA parameter, the default is to treat the
        # request as an immediate request with unlimited duration
        if not opennebula_vm.template.has_key(OpenNebulaHaizeaVM.HAIZEA_PARAM):
            self.start = OpenNebulaHaizeaVM.HAIZEA_START_NOW
            self.duration = OpenNebulaHaizeaVM.HAIZEA_DURATION_UNLIMITED
            self.preemptible = OpenNebulaHaizeaVM.HAIZEA_PREEMPTIBLE_NO
            self.group = None
        else:
            self.start = opennebula_vm.template[OpenNebulaHaizeaVM.HAIZEA_PARAM][OpenNebulaHaizeaVM.HAIZEA_START]
            self.duration = opennebula_vm.template[OpenNebulaHaizeaVM.HAIZEA_PARAM][OpenNebulaHaizeaVM.HAIZEA_DURATION]
            self.preemptible = opennebula_vm.template[OpenNebulaHaizeaVM.HAIZEA_PARAM][OpenNebulaHaizeaVM.HAIZEA_PREEMPTIBLE]
            if opennebula_vm.template[OpenNebulaHaizeaVM.HAIZEA_PARAM].has_key(OpenNebulaHaizeaVM.HAIZEA_GROUP):
                self.group = opennebula_vm.template[OpenNebulaHaizeaVM.HAIZEA_PARAM][OpenNebulaHaizeaVM.HAIZEA_GROUP]
            else:
                self.group = None
                
        self.submit_time = UNIX2DateTime(opennebula_vm.stime)
                
        # Create Timestamp object
        if self.start == OpenNebulaHaizeaVM.HAIZEA_START_NOW:
            self.start = Timestamp(Timestamp.NOW)
        elif self.start == OpenNebulaHaizeaVM.HAIZEA_START_BESTEFFORT:
            self.start = Timestamp(Timestamp.UNSPECIFIED)
        elif self.start[0] == "+":
            # Relative time
            self.start = Timestamp(round_datetime(self.submit_time + ISO.ParseTime(self.start[1:])))
        else:
            self.start = Timestamp(ISO.ParseDateTime(self.start))
            
        # Create Duration object
        if self.duration == OpenNebulaHaizeaVM.HAIZEA_DURATION_UNLIMITED:
            # This is an interim solution (make it run for a century).
            # TODO: Integrate concept of unlimited duration in the lease datastruct
            self.duration = Duration(DateTimeDelta(36500))
        else:
            self.duration = Duration(ISO.ParseTimeDelta(self.duration))
            

        self.preemptible = (self.preemptible == OpenNebulaHaizeaVM.HAIZEA_PREEMPTIBLE_YES)

    
        self.capacity = Capacity([constants.RES_CPU, constants.RES_MEM, constants.RES_DISK])
        
        # CPU
        # CPUs in VMs are not reported the same as in hosts.
        # THere are two template values: CPU and VCPU.
        # CPU reports the percentage of the CPU needed by the VM.
        # VCPU, which is optional, reports how many CPUs are needed.
        cpu = int(float(opennebula_vm.template["CPU"]) * 100)
        if opennebula_vm.template.has_key("VCPU"):
            ncpu = int(opennebula_vm.template["VCPU"])
        else:
            ncpu = 1
        self.capacity.set_ninstances(constants.RES_CPU, ncpu)
        for i in range(ncpu):
            self.capacity.set_quantity_instance(constants.RES_CPU, i+1, cpu)            
        
        # Memory. Unlike hosts, memory is reported directly in MBs
        self.capacity.set_quantity(constants.RES_MEM, int(opennebula_vm.template["MEMORY"]))

        self.one_id = opennebula_vm.id
        
    
class OpenNebulaFrontend(RequestFrontend):    
    
    def __init__(self):
        self.processed = []
        self.logger = logging.getLogger("ONEREQ")
        self.rpc = OpenNebulaXMLRPCClientSingleton().client

    def load(self, manager):
        pass
        
    def get_accumulated_requests(self):
        vms = self.rpc.vmpool_info()

        # Extract the pending OpenNebula VMs
        pending_vms = [] 
        for vm in vms:
            if not vm.id  in self.processed and vm.state == OpenNebulaVM.STATE_PENDING:
                vm_detailed = self.rpc.vm_info(vm.id)        
                pending_vms.append(OpenNebulaHaizeaVM(vm_detailed))
                self.processed.append(vm.id)
            
        grouped = [vm for vm in pending_vms if vm.group != None]
        not_grouped = [vm for vm in pending_vms if vm.group == None]
        
        # Extract VM groups
        group_ids = set([vm.group for vm in grouped])
        groups = {}
        for group_id in group_ids:
            groups[group_id] = [vm for vm in grouped if vm.group == group_id]
            
        lease_requests = []
        for group_id, opennebula_vms in groups.items():
            lease_requests.append(self.__ONEreqs_to_lease(opennebula_vms, group_id))

        for opennebula_vm in not_grouped:
            lease_requests.append(self.__ONEreqs_to_lease([opennebula_vm]))
        
        lease_requests.sort(key=operator.attrgetter("submit_time"))
        return lease_requests

    def exists_more_requests(self):
        return True

    
    def __ONEreqs_to_lease(self, opennebula_vms, group_id=None):
        # The vm_with_params is used to extract the HAIZEA parameters.
        # (i.e., lease-wide attributes)
        vm_with_params = opennebula_vms[0]

        # Per-lease attributes
        start = vm_with_params.start
        duration = vm_with_params.duration
        preemptible = vm_with_params.preemptible
        submit_time = vm_with_params.submit_time

        # Per-vnode attributes
        requested_resources = dict([(i+1,vm.capacity) for i, vm in enumerate(opennebula_vms)])

        lease = Lease.create_new(submit_time = submit_time, 
                                 requested_resources = requested_resources, 
                                 start = start, 
                                 duration = duration, 
                                 deadline = None,
                                 preemptible = preemptible, 
                                 software = UnmanagedSoftwareEnvironment())
     
        lease.enactment_info = group_id
        lease.vnode_enactment_info = dict([(i+1,vm.one_id) for i, vm in enumerate(opennebula_vms)])
        return lease

