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
from haizea.resourcemanager.frontends.base import RequestFrontend
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease, ImmediateLease, ResourceTuple
from haizea.common.utils import UNIX2DateTime
from pysqlite2 import dbapi2 as sqlite
from mx.DateTime import DateTimeDelta, TimeDelta, ISO
from haizea.common.utils import roundDateTime
import operator

HAIZEA_PARAM = "HAIZEA"
HAIZEA_START = "START"
HAIZEA_START_NOW = "now"
HAIZEA_START_BESTEFFORT = "best_effort"
HAIZEA_DURATION = "DURATION"
HAIZEA_DURATION_UNLIMITED = "unlimited"
HAIZEA_PREEMPTIBLE = "PREEMPTIBLE"
HAIZEA_PREEMPTIBLE_YES = "yes"
HAIZEA_PREEMPTIBLE_NO = "no"

ONE_CPU="CPU"
ONE_MEMORY="MEMORY"
ONE_DISK="DISK"
ONE_DISK_SOURCE="SOURCE"

class OpenNebulaFrontend(RequestFrontend):
    def __init__(self, rm):
        self.rm = rm
        self.processed = []
        self.logger = self.logger
        config = self.rm.config

        self.conn = sqlite.connect(config.get("one.db"))
        self.conn.row_factory = sqlite.Row
        
    def getAccumulatedRequests(self):
        cur = self.conn.cursor()
        processed = ",".join([`p` for p in self.processed])
        cur.execute("select * from vmpool where state=1 and oid not in (%s)" % processed)
        openNebulaReqs = cur.fetchall()
        requests = []
        for req in openNebulaReqs:
            cur.execute("select * from vm_template where id=%i" % req["oid"])
            template = cur.fetchall()
            attrs = dict([(r["name"], r["value"]) for r in template])
            self.processed.append(req["oid"])
            requests.append(self.ONEreq2lease(req, attrs))
        requests.sort(key=operator.attrgetter("submit_time"))
        return requests

    def existsMoreRequests(self):
        return True
    
    def ONEreq2lease(self, req, attrs):
        # If there is no HAIZEA parameter, the default is to treat the
        # request as an immediate request with unlimited duration
        if not attrs.has_key(HAIZEA_PARAM):
            haizea_param = {HAIZEA_START: HAIZEA_START_NOW,
                            HAIZEA_DURATION: HAIZEA_DURATION_UNLIMITED,
                            HAIZEA_PREEMPTIBLE: HAIZEA_PREEMPTIBLE_NO}
        else:
            haizea_param = self.get_vector_value(attrs[HAIZEA_PARAM])
        start = haizea_param[HAIZEA_START]
        if start == HAIZEA_START_NOW:
            return self.create_immediate_lease(req, attrs, haizea_param)
        elif start  == HAIZEA_START_BESTEFFORT:
            return self.create_besteffort_lease(req, attrs, haizea_param)
        else:
            return self.create_ar_lease(req, attrs, haizea_param)
    
    def get_vector_value(self, value):
        return dict([n.split("=") for n in value.split(",")])
    
    def get_common_attrs(self, req, attrs, haizea_param):
        disk = self.get_vector_value(attrs[ONE_DISK])
        tSubmit = UNIX2DateTime(req["stime"])
        vmimage = disk[ONE_DISK_SOURCE]
        vmimagesize = 0
        numnodes = 1
        resreq = ResourceTuple.create_empty()
        resreq.set_by_type(constants.RES_CPU, float(attrs[ONE_CPU]))
        resreq.set_by_type(constants.RES_MEM, int(attrs[ONE_MEMORY]))

        duration = haizea_param[HAIZEA_DURATION]
        if duration == HAIZEA_DURATION_UNLIMITED:
            # This is an interim solution (make it run for a century).
            # TODO: Integrate concept of unlimited duration in the lease datastruct
            duration = DateTimeDelta(36500)
        else:
            duration = ISO.ParseTimeDelta(duration)
            
        preemptible = haizea_param[HAIZEA_PREEMPTIBLE]
        preemptible = (preemptible == HAIZEA_PREEMPTIBLE_YES)

        return tSubmit, vmimage, vmimagesize, numnodes, resreq, duration, preemptible
    
    def create_besteffort_lease(self, req, attrs, haizea_param):
        tSubmit, vmimage, vmimagesize, numnodes, resreq, duration, preemptible = self.get_common_attrs(req, attrs, haizea_param)
 
        leasereq = BestEffortLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        leasereq.state = constants.LEASE_STATE_PENDING
        # Enactment info should be changed to the "array id" when groups
        # are implemented in OpenNebula
        leasereq.enactment_info = int(req["oid"])
        # Only one node for now
        leasereq.vnode_enactment_info = {}
        leasereq.vnode_enactment_info[1] = int(req["oid"])
        return leasereq
    
    def create_ar_lease(self, req, attrs, haizea_param):
        tSubmit, vmimage, vmimagesize, numnodes, resreq, duration, preemptible = self.get_common_attrs(req, attrs, haizea_param)

        start = haizea_param[HAIZEA_START]
        if start[0] == "+":
            # Relative time
            # For testing, should be:
            # tStart = tSubmit + ISO.ParseTime(tStart[1:])
            start = roundDateTime(self.rm.clock.get_time() + ISO.ParseTime(start[1:]))
        else:
            start = ISO.ParseDateTime(start)
        leasereq = ARLease(tSubmit, start, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        leasereq.state = constants.LEASE_STATE_PENDING
        # Enactment info should be changed to the "array id" when groups
        # are implemented in OpenNebula
        leasereq.enactmentInfo = int(req["oid"])
        # Only one node for now
        leasereq.vnode_enactment_info = {}
        leasereq.vnode_enactment_info[1] = int(req["oid"])
        return leasereq

    def create_immediate_lease(self, req, attrs, haizea_param):
        tSubmit, vmimage, vmimagesize, numnodes, resreq, duration, preemptible = self.get_common_attrs(req, attrs, haizea_param)
 
        leasereq = ImmediateLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        leasereq.state = constants.LEASE_STATE_PENDING
        # Enactment info should be changed to the "array id" when groups
        # are implemented in OpenNebula
        leasereq.enactment_info = int(req["oid"])
        # Only one node for now
        leasereq.vnode_enactment_info = {}
        leasereq.vnode_enactment_info[1] = int(req["oid"])
        return leasereq
        