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

"""This module provides pluggable lease admission policies. See the documentation
for haizea.core.schedule.policy.LeaseAdmissionPolicy for more details on
lease admission policies.
"""


from haizea.core.scheduler.policy import LeaseAdmissionPolicy
from haizea.core.leases import Lease

class AcceptAllPolicy(LeaseAdmissionPolicy):
    """A simple admission policy: all lease requests are accepted.
    """
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """  
        LeaseAdmissionPolicy.__init__(self, slottable)
        
    def accept_lease(self, lease):
        """Lease admission function
        
        See class documentation for details on what policy is implemented here.
        Returns True if the lease can be accepted, False if it should be rejected.
        
        Argument
        lease -- Lease request
        """           
        return True  
    
class NoARsPolicy(LeaseAdmissionPolicy):
    """A simple admission policy: all lease requests, except AR requests,
    are accepted.
    """
    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """  
        LeaseAdmissionPolicy.__init__(self, slottable)
        
    def accept_lease(self, lease):
        """Lease admission function
        
        See class documentation for details on what policy is implemented here.
        Returns True if the lease can be accepted, False if it should be rejected.
        
        Argument
        lease -- Lease request
        """        
        return lease.get_type() != Lease.ADVANCE_RESERVATION