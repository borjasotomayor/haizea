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

from haizea.resourcemanager.deployment.base import DeploymentBase
import haizea.common.constants as constants

class UnmanagedDeployment(DeploymentBase):
    def __init__(self, scheduler):
        DeploymentBase.__init__(self, scheduler)
    
    def schedule(self, lease, vmrr, nexttime):
        lease.state = constants.LEASE_STATE_DEPLOYED
            
    def find_earliest_starting_times(self, lease_req, nexttime):
        nodIDs = [n.nod_id for n in self.resourcepool.getNodes()]
        earliest = dict([(node, [nexttime, constants.REQTRANSFER_NO, None]) for node in nodIDs])
        return earliest
            
    def cancel_deployment(self, lease):
        pass