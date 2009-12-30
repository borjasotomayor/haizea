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
from haizea.common.stats import percentile

import random

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
    """...
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
    
class RandomMultipleOfFairPricePolicy(FairPricePolicy):
    """Base class for policies that rely on the notion of a fair rate for computation
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        FairPricePolicy.__init__(self, slottable)
        random.seed(get_config().config.getint("pricing", "seed"))
        self.min_multiplier = get_config().config.getfloat("pricing", "min-multiplier")
        self.max_multiplier = get_config().config.getfloat("pricing", "max-multiplier")
    
    def price_lease(self, lease, preempted_leases):
        """Computes the price of a lease
        
        See class documentation for details on what policy is implemented here.
        See documentation of PricingPolicy.price_lease
        for more details on this function.
        
        Arguments:
        lease -- Lease that is being scheduled.
        preempted_leases -- Leases that would have to be preempted to support this lease.
        """
        mult = random.uniform(self.min_multiplier, self.max_multiplier)
        fair_price = self.get_fair_price(lease)
        return mult * fair_price
    
class MaxMultipleOfFairPricePolicy(FairPricePolicy):
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
        mult = float(lease.extras["simul_pricemarkup"])
        fair_price = self.get_fair_price(lease)
        return mult * fair_price    
    
class UserInfo(object):
    def __init__(self):
        self.min_markup_accept = None
        self.max_markup_accept = None
        self.min_markup_reject = None
        self.max_markup_reject = None
        self.markup_estimate = None
        self.found = False
    
class AdaptiveFairPricePolicy(FairPricePolicy):
    """Base class for policies that rely on the notion of a fair rate for computation
    """    
    def __init__(self, slottable):
        """Constructor
        
        Argument
        slottable -- A fully constructed SlotTable
        """        
        FairPricePolicy.__init__(self, slottable)
        self.multiplier = 1.0
        self.users = {}
    
    def price_lease(self, lease, preempted_leases):
        """Computes the price of a lease
        
        See class documentation for details on what policy is implemented here.
        See documentation of PricingPolicy.price_lease
        for more details on this function.
        
        Arguments:
        lease -- Lease that is being scheduled.
        preempted_leases -- Leases that would have to be preempted to support this lease.
        """
        fair_price = self.get_fair_price(lease)
        return self.multiplier * fair_price
    
    def feedback(self, lease):
        """Called after a lease has been accepted or rejected, to provide
        feeback to the pricing policy.
        
        Arguments:
        lease -- Lease that has been accepted/rejected
        """
        if lease.price == None:
            return
        fair_price = lease.extras["fair_price"]
        price = lease.price
        if price == -1:
            price = lease.extras["rejected_price"]

        lease_multiplier = price / fair_price
        
        if not self.users.has_key(lease.user_id):
            self.users[lease.user_id] = UserInfo()
            
        if lease.get_state() == Lease.STATE_REJECTED_BY_USER:
            if self.users[lease.user_id].min_markup_reject == None:
                self.users[lease.user_id].min_markup_reject = lease_multiplier
                self.users[lease.user_id].max_markup_reject = lease_multiplier
            else:
                self.users[lease.user_id].min_markup_reject = min(lease_multiplier, self.users[lease.user_id].min_markup_reject)
                self.users[lease.user_id].max_markup_reject = max(lease_multiplier, self.users[lease.user_id].max_markup_reject)
        else:
            if self.users[lease.user_id].min_markup_accept == None:
                self.users[lease.user_id].min_markup_accept = lease_multiplier
                self.users[lease.user_id].max_markup_accept = lease_multiplier
            else:
                if self.users[lease.user_id].min_markup_reject != None:
                    self.users[lease.user_id].found = True
                else:
                    self.users[lease.user_id].min_markup_accept = min(lease_multiplier, self.users[lease.user_id].min_markup_accept)
                    self.users[lease.user_id].max_markup_accept = max(lease_multiplier, self.users[lease.user_id].max_markup_accept)
                
        for user in self.users:
            if not self.users[user].found:
                if self.users[user].min_markup_reject == None:
                    # All accepts
                    estimate = self.users[user].max_markup_accept * 1.5
                elif self.users[user].min_markup_accept == None:
                    # All rejects
                    estimate = self.users[user].min_markup_reject * 0.5
                else:
                    estimate = (self.users[user].max_markup_accept + self.users[user].min_markup_reject) / 2
                    
                self.users[user].markup_estimate = estimate
            
        estimates = sorted([u.markup_estimate for u in self.users.values()])
        
        self.multiplier = percentile(estimates, 0.5)