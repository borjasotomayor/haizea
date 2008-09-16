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
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease, ImmediateLease, ResourceTuple
from haizea.resourcemanager.frontends import RequestFrontend
from haizea.common.utils import round_datetime, get_config, get_clock
from mx.DateTime import DateTimeDelta, TimeDelta, ISO
import logging

class RPCFrontend(RequestFrontend):
    HAIZEA_START_NOW = "now"
    HAIZEA_START_BESTEFFORT = "best_effort"
    HAIZEA_DURATION_UNLIMITED = "unlimited"

    def __init__(self, rm):
        self.rm = rm
        self.logger = logging.getLogger("RPCREQ")
        self.accumulated = []
        config = get_config()
        self.rm.rpc_server.register_rpc(self.create_lease)

    def get_accumulated_requests(self):
        acc = self.accumulated
        self.accumulated = []
        return acc
    
    def exists_more_requests(self): 
        return True
            
    def create_lease(self, start, duration, preemptible, numnodes, cpu, mem, vmimage, vmimagesize):
        tSubmit = round_datetime(get_clock().get_time())
        resreq = ResourceTuple.create_empty()
        resreq.set_by_type(constants.RES_CPU, float(cpu))
        resreq.set_by_type(constants.RES_MEM, int(mem))        
        if duration == RPCFrontend.HAIZEA_DURATION_UNLIMITED:
            # This is an interim solution (make it run for a century).
            # TODO: Integrate concept of unlimited duration in the lease datastruct
            duration = DateTimeDelta(36500)
        else:
            duration = ISO.ParseTimeDelta(duration)

        if start == RPCFrontend.HAIZEA_START_NOW:
            leasereq = ImmediateLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        elif start  == RPCFrontend.HAIZEA_START_BESTEFFORT:
            leasereq = BestEffortLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        else:
            if start[0] == "+":
                # Relative time
                start = round_datetime(tSubmit + ISO.ParseTime(start[1:]))
            else:
                start = ISO.ParseDateTime(start)
            leasereq = ARLease(tSubmit, start, duration, vmimage, vmimagesize, numnodes, resreq, preemptible)
        
        self.accumulated.append(leasereq)
        
        return leasereq.id


        
        
    
    
            
