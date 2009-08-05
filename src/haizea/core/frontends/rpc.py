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
from haizea.core.scheduler.slottable import ResourceTuple
from haizea.core.leases import Lease
from haizea.core.frontends import RequestFrontend
from haizea.common.utils import round_datetime, get_config, get_clock, get_lease_id
from mx.DateTime import DateTimeDelta, TimeDelta, ISO
import logging

class RPCFrontend(RequestFrontend):
    def __init__(self, manager):
        self.manager = manager
        self.logger = logging.getLogger("RPCREQ")
        self.accumulated = []
        config = get_config()
        self.manager.rpc_server.register_rpc(self.create_lease)

    def get_accumulated_requests(self):
        acc = self.accumulated
        self.accumulated = []
        return acc
    
    def exists_more_requests(self): 
        return True

    def create_lease(self, lease_xml_str):     
        lease = Lease.from_xml_string(lease_xml_str)
        lease.id = get_lease_id()
        self.accumulated.append(lease)        
        return lease.id
        
        
    
    
            
