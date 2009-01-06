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

import haizea.common.constants as constants
from haizea.resourcemanager.frontends import RequestFrontend
from haizea.resourcemanager.leases import ARLease, BestEffortLease, ImmediateLease
from haizea.resourcemanager.scheduler.slottable import ResourceTuple
from haizea.common.utils import UNIX2DateTime, round_datetime, get_config, get_clock

from pysqlite2 import dbapi2 as sqlite
from mx.DateTime import DateTimeDelta, TimeDelta, ISO

import operator
import logging

class OpenNebulaVM(object):
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
    
    ONE_CPU="CPU"
    ONE_MEMORY="MEMORY"
    ONE_DISK="DISK"
    ONE_DISK_SOURCE="SOURCE"    
    
    def __init__(self, db_entry, attrs):
        self.db_entry = db_entry
        self.attrs = attrs
        
        # If there is no HAIZEA parameter, the default is to treat the
        # request as an immediate request with unlimited duration
        if not attrs.has_key(OpenNebulaVM.HAIZEA_PARAM):
            self.haizea_param = {OpenNebulaVM.HAIZEA_START: OpenNebulaVM.HAIZEA_START_NOW,
                            OpenNebulaVM.HAIZEA_DURATION: OpenNebulaVM.HAIZEA_DURATION_UNLIMITED,
                            OpenNebulaVM.HAIZEA_PREEMPTIBLE: OpenNebulaVM.HAIZEA_PREEMPTIBLE_NO}
        else:
            self.haizea_param = self.__get_vector_value(attrs[OpenNebulaVM.HAIZEA_PARAM])

        
    def is_grouped(self):
        return self.haizea_param.has_key(OpenNebulaVM.HAIZEA_GROUP)
    
    def has_haizea_params(self):
        return self.haizea_param.has_key(OpenNebulaVM.HAIZEA_START)
    
    def get_start(self):
        start = self.haizea_param[OpenNebulaVM.HAIZEA_START]
        if start == OpenNebulaVM.HAIZEA_START_NOW or start == OpenNebulaVM.HAIZEA_START_BESTEFFORT:
            pass # Leave start as it is
        elif start[0] == "+":
            # Relative time
            # The following is just for testing:
            now = get_clock().get_time()
            start = round_datetime(now + ISO.ParseTime(start[1:]))
            # Should be:
            #start = round_datetime(self.get_submit_time() + ISO.ParseTime(start[1:]))

        else:
            start = ISO.ParseDateTime(start)
    
        return start
    
    def get_duration(self):
        duration = self.haizea_param[OpenNebulaVM.HAIZEA_DURATION]
        if duration == OpenNebulaVM.HAIZEA_DURATION_UNLIMITED:
            # This is an interim solution (make it run for a century).
            # TODO: Integrate concept of unlimited duration in the lease datastruct
            duration = DateTimeDelta(36500)
        else:
            duration = ISO.ParseTimeDelta(duration)
        return duration
    
    def get_preemptible(self):
        preemptible = self.haizea_param[OpenNebulaVM.HAIZEA_PREEMPTIBLE]
        return (preemptible == OpenNebulaVM.HAIZEA_PREEMPTIBLE_YES)

    def get_group(self):
        return self.haizea_param[OpenNebulaVM.HAIZEA_GROUP]

    def get_submit_time(self):
        return UNIX2DateTime(self.db_entry["stime"])
    
    def get_diskimage(self):
        disk = self.__get_vector_value(self.attrs[OpenNebulaVM.ONE_DISK])
        diskimage = disk[OpenNebulaVM.ONE_DISK_SOURCE]
        return diskimage
    
    def get_diskimage_size(self):
        return 0 # OpenNebula doesn't provide this
    
    def get_resource_requirements(self):
        resreq = ResourceTuple.create_empty()
        resreq.set_by_type(constants.RES_CPU, float(self.attrs[OpenNebulaVM.ONE_CPU]))
        resreq.set_by_type(constants.RES_MEM, int(self.attrs[OpenNebulaVM.ONE_MEMORY]))
        return resreq    

    def get_oneid(self):
        return int(self.db_entry["oid"])

    def __get_vector_value(self, value):
        return dict([n.split("=") for n in value.split(",")])
        
    
class OpenNebulaFrontend(RequestFrontend):    
    
    def __init__(self, rm):
        self.rm = rm
        self.processed = []
        self.logger = logging.getLogger("ONEREQ")
        config = get_config()

        self.conn = sqlite.connect(config.get("one.db"))
        self.conn.row_factory = sqlite.Row
        
    def get_accumulated_requests(self):
        cur = self.conn.cursor()
        processed = ",".join([`p` for p in self.processed])
        cur.execute("select * from vmpool where state=1 and oid not in (%s)" % processed)
        db_opennebula_vms = cur.fetchall()
        
        # Extract the pending OpenNebula VMs
        opennebula_vms = [] # (ONE VM, ONE VM template attributes, ONE Haizea parameter)
        for vm in db_opennebula_vms:
            cur.execute("select * from vm_template where id=%i" % vm["oid"])
            template = cur.fetchall()
            attrs = dict([(r["name"], r["value"]) for r in template])
            self.processed.append(vm["oid"])
            
            opennebula_vms.append(OpenNebulaVM(vm, attrs))
            
        grouped = [vm for vm in opennebula_vms if vm.is_grouped()]
        not_grouped = [vm for vm in opennebula_vms if not vm.is_grouped()]
        
        # Extract VM groups
        group_ids = set([vm.get_group() for vm in grouped])
        groups = {}
        for group_id in group_ids:
            groups[group_id] = [vm for vm in grouped if vm.get_group() == group_id]
            
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
        vm_with_params = self.__get_vm_with_params(opennebula_vms)

        # Per-lease attributes
        start = vm_with_params.get_start()
        duration = vm_with_params.get_duration()
        preemptible = vm_with_params.get_preemptible()
        numnodes = len(opennebula_vms)

        # Per-vnode attributes
        # Since Haizea currently assumes homogeneous nodes in
        # the lease, we will also use vm_with_params to extract the 
        # resource requirements.
        # TODO: When Haizea is modified to support heterogeneous nodes,
        # extract the resource requirements from each individual VM.
        submit_time = vm_with_params.get_submit_time()
        diskimage = vm_with_params.get_diskimage()
        diskimagesize = vm_with_params.get_diskimage_size()
        resource_requirements = vm_with_params.get_resource_requirements()

        if start == OpenNebulaVM.HAIZEA_START_NOW:
            lease = ImmediateLease(submit_time, duration, diskimage, diskimagesize, numnodes, resource_requirements, preemptible)
        elif start  == OpenNebulaVM.HAIZEA_START_BESTEFFORT:
            lease = BestEffortLease(submit_time, duration, diskimage, diskimagesize, numnodes, resource_requirements, preemptible)
        else:
            lease = ARLease(submit_time, start, duration, diskimage, diskimagesize, numnodes, resource_requirements, preemptible)
     
        lease.enactment_info = group_id
        lease.vnode_enactment_info = dict([(i+1,vm.get_oneid()) for i, vm in enumerate(opennebula_vms)])
        return lease
        
    def __get_vm_with_params(self, opennebula_vms):
        # One of the ONE VMs has the start, duration, and preemptible
        # parameters. Find it.
        vm_with_params = [vm for vm in opennebula_vms if vm.has_haizea_params()]
        # There should only be one
        # TODO: This is the ONE user's responsibility, but we should validate it
        # nonetheless.
        return vm_with_params[0]
