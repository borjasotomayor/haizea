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

from haizea.resourcemanager.enact.base import StorageEnactmentBase

class StorageEnactment(StorageEnactmentBase):
    def __init__(self, resourcepool):
        StorageEnactmentBase.__init__(self, resourcepool)
        self.imagepath="/images/playground/borja"
        
    def resolveToFile(self, lease_id, vnode, diskImageID):
        return "%s/%s/%s.img" % (self.imagepath, diskImageID, diskImageID)