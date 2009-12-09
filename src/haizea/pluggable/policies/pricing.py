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

"""This module provides pluggable lease pricing policies. See the documentation
for haizea.core.schedule.policy.PricingPolicy for more details on
lease preemption policies.
"""

from haizea.core.leases import Lease
from haizea.core.scheduler.policy import PricingPolicy
from haizea.common.utils import get_config

class FreePolicy(PricingPolicy):
    """Simple pricing policy: all leases are free (price is zero)
    """
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        PricingPolicy.__init__(self, slottable)
    
    def price_lease(self, lease, preempted_leases):
        """Computes the price of a lease
        
        See class documentation for details on what policy is implemented here.
        See documentation of PricingPolicy.price_lease
        for more details on this function.
        
        Arguments:
        lease -- Lease that is being scheduled.
        preempted_leases -- Leases that would have to be preempted to support this lease.
        """                    
        return 0.0

class FairPricePolicy(PricingPolicy):
    """Base class for policies that rely on the notion of a fair rate for computation
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        PricingPolicy.__init__(self, slottable)
        self.fair_rate = get_config().config.getfloat("pricing", "fair-rate")
    
    def get_fair_price(self, lease):
        fair_price = (lease.duration.requested.seconds / 3600) * lease.numnodes * self.fair_rate
        return fair_price
    
    
class AlwaysFairPricePolicy(FairPricePolicy):
    """Base class for policies that rely on the notion of a fair rate for computation
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        FairPricePolicy.__init__(self, slottable)
    
    def price_lease(self, lease, preempted_leases):
        """Computes the price of a lease
        
        See class documentation for details on what policy is implemented here.
        See documentation of PricingPolicy.price_lease
        for more details on this function.
        
        Arguments:
        lease -- Lease that is being scheduled.
        preempted_leases -- Leases that would have to be preempted to support this lease.
        """
        return self.get_fair_price(lease)