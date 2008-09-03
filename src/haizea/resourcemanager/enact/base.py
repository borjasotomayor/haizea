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

from haizea.common.utils import abstract
import haizea.resourcemanager.datastruct as ds

class ResourcePoolInfoBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
        
        resourcetypes = self.get_resource_types() #IGNORE:E1111
        ds.ResourceTuple.set_resource_types(resourcetypes)

        
    def get_nodes(self): 
        """ Returns the nodes in the resource pool. """
        abstract()
        
    def get_fifo_node(self):
        """ Returns the image node for FIFO transfers
        
        Note that this function will disappear as soon
        as we merge the EDF and FIFO image nodes (and
        their respective algorithms)
        """
        abstract()

    def get_edf_node(self):
        """ Returns the image node for EDF transfers
        
        Note that this function will disappear as soon
        as we merge the EDF and FIFO image nodes (and
        their respective algorithms)
        """
        abstract()
        
    def get_resource_types(self):
        abstract()
        
class StorageEnactmentBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
    
class VMEnactmentBase(object):
    def __init__(self, resourcepool):
        self.resourcepool = resourcepool
        
    def start(self, vms): abstract()
    
    def stop(self, vms): abstract()
    
    def suspend(self, vms): abstract()
    
    def resume(self, vms): abstract()