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

class EnactmentAction(object):
    def __init__(self):
        self.lease_haizea_id = None
        self.lease_enactment_info = None
            
    def from_rr(self, rr):
        self.lease_haizea_id = rr.lease.id
        self.lease_enactment_info = rr.lease.enactment_info
        
class VNode(object):
    def __init__(self, enactment_info):
        self.enactment_info = enactment_info
        self.pnode = None
        self.resources = None
        self.diskimage = None
        
class VMEnactmentAction(EnactmentAction):
    def __init__(self):
        EnactmentAction.__init__(self)
        self.vnodes = {}
    
    def from_rr(self, rr):
        EnactmentAction.from_rr(self, rr)
        self.vnodes = dict([(vnode, VNode(info)) for (vnode, info) in rr.lease.vnode_enactment_info.items()])

class VMEnactmentStartAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentStopAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

    def from_rr(self, rr):
        VMEnactmentAction.from_rr(self, rr)
        self.vnodes = dict([(k, v) for (k,v) in self.vnodes.items() if k in rr.vnodes])

class VMEnactmentResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

    def from_rr(self, rr):
        VMEnactmentAction.from_rr(self, rr)
        self.vnodes = dict([(k, v) for (k,v) in self.vnodes.items() if k in rr.vnodes])

class VMEnactmentConfirmSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

    def from_rr(self, rr):
        VMEnactmentAction.from_rr(self, rr)
        self.vnodes = dict([(k, v) for (k,v) in self.vnodes.items() if k in rr.vnodes])

class VMEnactmentConfirmResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

    def from_rr(self, rr):
        VMEnactmentAction.from_rr(self, rr)
        self.vnodes = dict([(k, v) for (k,v) in self.vnodes.items() if k in rr.vnodes])
