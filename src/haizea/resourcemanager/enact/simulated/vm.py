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

from haizea.resourcemanager.enact.base import VMEnactmentBase
import haizea.common.constants as constants
import logging

class VMEnactment(VMEnactmentBase):
    def __init__(self, resourcepool):
        VMEnactmentBase.__init__(self, resourcepool)
        self.logger = logging.getLogger("ENACT.SIMUL.VM")
        
    def start(self, action):
        for vnode in action.vnodes:
            # Unpack action
            pnode = action.vnodes[vnode].pnode
            image = action.vnodes[vnode].diskimage
            cpu = action.vnodes[vnode].resources.get_by_type(constants.RES_CPU)
            memory = action.vnodes[vnode].resources.get_by_type(constants.RES_MEM)
            self.logger.debug("Received request to start VM for L%iV%i on host %i, image=%s, cpu=%i, mem=%i"
                         % (action.lease_haizea_id, vnode, pnode, image, cpu, memory))
    
    def stop(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to stop VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def suspend(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to suspend VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def resume(self, action):
        for vnode in action.vnodes:
            self.logger.debug("Received request to resume VM for L%iV%i"
                         % (action.lease_haizea_id, vnode))

    def verifySuspend(self, action):
        return 0
    
    def verifyResume(self, action):
        return 0