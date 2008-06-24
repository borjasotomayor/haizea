# -------------------------------------------------------------------------- #
# Copyright 2006-2008, Borja Sotomayor                                       #
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

class EnactmentAction(object):
    def __init__(self):
        self.leaseHaizeaID = None
        self.leaseEnactmentInfo = None
            
    def fromRR(self, rr):
        self.leaseHaizeaID = rr.lease.leaseID
        self.leaseEnactmentInfo = rr.lease.enactmentInfo
        
class VNode(object):
    def __init__(self, enactmentInfo):
        self.enactmentInfo = enactmentInfo
        self.pnode = None
        self.res = None
        self.diskimage = None
        
class VMEnactmentAction(EnactmentAction):
    def __init__(self):
        EnactmentAction.__init__(self)
        self.vnodes = {}
    
    def fromRR(self, rr):
        EnactmentAction.fromRR(self, rr)
        self.vnodes = dict([(vnode, VNode(info)) for (vnode,info) in rr.lease.vnodeEnactmentInfo.items()])

class VMEnactmentStartAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentStopAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentConfirmSuspendAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)

class VMEnactmentConfirmResumeAction(VMEnactmentAction):
    def __init__(self):
        VMEnactmentAction.__init__(self)
