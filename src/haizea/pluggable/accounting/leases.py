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

"""Accounting probes that collect data from leases"""

from haizea.core.accounting import AccountingProbe, AccountingDataCollection
from haizea.core.leases import Lease
from haizea.common.utils import get_policy

from mx.DateTime import TimeDelta, DateTime
          
class LeaseProbe(AccountingProbe):
    """
    Collects information from any lease
    
    """
    COUNTER_REQUESTED="Requested leases"
    COUNTER_COMPLETED="Completed leases"
    COUNTER_REJECTED="Rejected leases"
    COUNTER_REJECTED_BY_USER="Rejected leases (by user)"
    STAT_REQUESTED="Total requested leases"
    STAT_COMPLETED="Total completed leases"
    STAT_REJECTED="Total rejected leases"
    STAT_REJECTED_BY_USER="Total rejected leases (by user)"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(LeaseProbe.COUNTER_REQUESTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(LeaseProbe.COUNTER_COMPLETED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(LeaseProbe.COUNTER_REJECTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(LeaseProbe.COUNTER_REJECTED_BY_USER, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(LeaseProbe.STAT_REQUESTED)
        self.accounting.create_stat(LeaseProbe.STAT_COMPLETED)
        self.accounting.create_stat(LeaseProbe.STAT_REJECTED)
        self.accounting.create_stat(LeaseProbe.STAT_REJECTED_BY_USER)

    def finalize_accounting(self):
        """See AccountingProbe.finalize_accounting"""        
        self._set_stat_from_counter(LeaseProbe.STAT_REQUESTED, LeaseProbe.COUNTER_REQUESTED)
        self._set_stat_from_counter(LeaseProbe.STAT_COMPLETED, LeaseProbe.COUNTER_COMPLETED)
        self._set_stat_from_counter(LeaseProbe.STAT_REJECTED, LeaseProbe.COUNTER_REJECTED)
        self._set_stat_from_counter(LeaseProbe.STAT_REJECTED_BY_USER, LeaseProbe.COUNTER_REJECTED_BY_USER)

    def at_lease_request(self, lease):
        """See AccountingProbe.at_lease_request"""                
        self.accounting.incr_counter(LeaseProbe.COUNTER_REQUESTED, lease.id)

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""
        if lease.get_state() == Lease.STATE_REJECTED:
            self.accounting.incr_counter(LeaseProbe.COUNTER_REJECTED, lease.id)    
        elif lease.get_state() == Lease.STATE_REJECTED_BY_USER:
            self.accounting.incr_counter(LeaseProbe.COUNTER_REJECTED_BY_USER, lease.id)    
        elif lease.get_state() == Lease.STATE_DONE:
            self.accounting.incr_counter(LeaseProbe.COUNTER_COMPLETED, lease.id)    
                
class LeaseExtrasProbe(AccountingProbe):
    """
    Collects information from any lease
    
    """
    
    LEASE_STAT_STATE="State"
    LEASE_STAT_NUMNODES="Number of nodes"
    LEASE_STAT_REQDURATION="Requested duration"
    LEASE_STAT_DURATION="Actual duration"
    LEASE_STAT_START="Start"
    LEASE_STAT_DEADLINE="Deadline"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_STATE)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_NUMNODES)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_REQDURATION)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_DURATION)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_START)
        self.accounting.create_lease_stat(LeaseExtrasProbe.LEASE_STAT_DEADLINE)
        

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""
        self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_STATE, lease.id, Lease.state_str[lease.get_state()])
        self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_NUMNODES, lease.id, lease.numnodes)
        self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_REQDURATION, lease.id, lease.duration.requested.seconds)
        if lease.duration.actual != None:
            self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_DURATION, lease.id, lease.duration.actual.seconds)
        if lease.start.is_requested_exact():
            self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_START, lease.id, (lease.start.requested - lease.submit_time).seconds)
            if lease.deadline != None:
                self.accounting.set_lease_stat(LeaseExtrasProbe.LEASE_STAT_DEADLINE, lease.id, (lease.deadline - lease.start.requested).seconds)
        for name, value in lease.extras.items():
            self.accounting.create_lease_stat(name)
            self.accounting.set_lease_stat(name, lease.id, value)
              
                
                
class ARProbe(AccountingProbe):
    """
    Collects information from Advance Reservation leases
    
    * Counters
    
      - "Accepted AR": Number of accepted AR leases 
      - "Rejected AR": Number of rejected AR leases

    * Per-run data
    
      - "Total accepted AR": Final number of accepted AR leases
      - "Total rejected AR": Final number of rejected AR leases

    """
    COUNTER_ACCEPTED="Accepted AR"
    COUNTER_REJECTED="Rejected AR"
    STAT_ACCEPTED="Total accepted AR"
    STAT_REJECTED="Total rejected AR"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(ARProbe.COUNTER_ACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(ARProbe.COUNTER_REJECTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(ARProbe.STAT_ACCEPTED)
        self.accounting.create_stat(ARProbe.STAT_REJECTED)

    def finalize_accounting(self):
        """See AccountingProbe.finalize_accounting"""        
        self._set_stat_from_counter(ARProbe.STAT_ACCEPTED, ARProbe.COUNTER_ACCEPTED)
        self._set_stat_from_counter(ARProbe.STAT_REJECTED, ARProbe.COUNTER_REJECTED)

    def at_lease_request(self, lease):
        """See AccountingProbe.at_lease_request"""                
        if lease.get_type() == Lease.ADVANCE_RESERVATION:
            if lease.get_state() == Lease.STATE_PENDING:
                self.accounting.incr_counter(ARProbe.COUNTER_ACCEPTED, lease.id)
            elif lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(ARProbe.COUNTER_REJECTED, lease.id)

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""
        if lease.get_type() == Lease.ADVANCE_RESERVATION:
            if lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(ARProbe.COUNTER_REJECTED, lease.id)


class IMProbe(AccountingProbe):
    """
    Collects information from immediate leases
    
    * Counters
    
      - "Accepted Immediate": Number of accepted Immediate leases 
      - "Rejected Immediate": Number of rejected Immediate leases

    * Per-run data
    
      - "Total accepted Immediate": Final number of accepted Immediate leases
      - "Total rejected Immediate": Final number of rejected Immediate leases

    """
    COUNTER_ACCEPTED="Accepted Immediate"
    COUNTER_REJECTED="Rejected Immediate"
    STAT_ACCEPTED="Total accepted Immediate"
    STAT_REJECTED="Total rejected Immediate"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""        
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(IMProbe.COUNTER_ACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(IMProbe.COUNTER_REJECTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(IMProbe.STAT_ACCEPTED)
        self.accounting.create_stat(IMProbe.STAT_REJECTED)

    def finalize_accounting(self):
        """See AccountingProbe.finalize_accounting"""        
        self._set_stat_from_counter(IMProbe.STAT_ACCEPTED, IMProbe.COUNTER_ACCEPTED)
        self._set_stat_from_counter(IMProbe.STAT_REJECTED, IMProbe.COUNTER_REJECTED)

    def at_lease_request(self, lease):
        """See AccountingProbe.at_lease_request"""                        
        if lease.get_type() == Lease.IMMEDIATE:
            if lease.get_state() == Lease.STATE_PENDING:
                self.accounting.incr_counter(IMProbe.COUNTER_ACCEPTED, lease.id)
            elif lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(IMProbe.COUNTER_REJECTED, lease.id)

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""        
        if lease.get_type() == Lease.IMMEDIATE:
            if lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(IMProbe.COUNTER_REJECTED, lease.id)


class BEProbe(AccountingProbe):
    """
    Collects information from best-effort leases
    
    * Counters
    
      - "Best-effort completed": Number of best-effort leases completed
        throughout the run
      - "Queue size": Size of the queue throughout the run

    * Per-lease data
    
      - "Waiting time": Time (in seconds) the lease waited in the queue
        before resources were allocated to it.
      - "Slowdown": Slowdown of the lease (time required to run the lease
        to completion divided by the time it would have required on a
        dedicated system)

    * Per-run data
    
      - "Total best-effort completed": Final number of completed best-effort leases
      - "all-best-effort": The time (in seconds) when the last best-effort
        lease was completed.

    """
        
    COUNTER_BESTEFFORTCOMPLETED="Best-effort completed"
    COUNTER_QUEUESIZE="Queue size"
    LEASE_STAT_WAITINGTIME="Waiting time"
    LEASE_STAT_SLOWDOWN="Slowdown"
    STAT_BESTEFFORTCOMPLETED="Total best-effort completed"
    STAT_ALLBESTEFFORT="all-best-effort"
    
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(BEProbe.COUNTER_BESTEFFORTCOMPLETED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(BEProbe.COUNTER_QUEUESIZE, AccountingDataCollection.AVERAGE_TIMEWEIGHTED)
        self.accounting.create_lease_stat(BEProbe.LEASE_STAT_WAITINGTIME)
        self.accounting.create_lease_stat(BEProbe.LEASE_STAT_SLOWDOWN)
        self.accounting.create_stat(BEProbe.STAT_BESTEFFORTCOMPLETED)
        self.accounting.create_stat(BEProbe.STAT_ALLBESTEFFORT)
    
    def finalize_accounting(self):
        """See AccountingProbe.finalize_accounting"""        
        self._set_stat_from_counter(BEProbe.STAT_BESTEFFORTCOMPLETED, BEProbe.COUNTER_BESTEFFORTCOMPLETED)
        all_best_effort = self.accounting.get_last_counter_time(BEProbe.COUNTER_BESTEFFORTCOMPLETED)
        self.accounting.set_stat(BEProbe.STAT_ALLBESTEFFORT, all_best_effort)
    
    def at_timestep(self, lease_scheduler):
        """See AccountingProbe.at_timestep"""        
        queue_len = lease_scheduler.queue.length()
        self.accounting.append_to_counter(BEProbe.COUNTER_QUEUESIZE, queue_len)

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""                        
        if lease.get_type() == Lease.BEST_EFFORT:
            wait = lease.get_waiting_time().seconds
            self.accounting.set_lease_stat(BEProbe.LEASE_STAT_WAITINGTIME, lease.id, wait)
            self.accounting.set_lease_stat(BEProbe.LEASE_STAT_SLOWDOWN, lease.id, lease.get_slowdown())
            self.accounting.incr_counter(BEProbe.COUNTER_BESTEFFORTCOMPLETED, lease.id)

class PriceProbe(AccountingProbe):
    """
    Collects information from priced leases
    
    """
    COUNTER_REVENUE="Revenue"
    COUNTER_SURCHARGE="Surcharge"
    COUNTER_MISSED_REVENUE_UNDERCHARGE="Missed revenue (undercharging)"
    COUNTER_MISSED_REVENUE_REJECT="Missed revenue (reject)"
    COUNTER_MISSED_REVENUE_REJECT_BY_USER="Missed revenue (reject by user)"
    STAT_REVENUE="Revenue"
    STAT_SURCHARGE="Surcharge"
    STAT_MISSED_REVENUE_UNDERCHARGE="Missed revenue (undercharging)"
    STAT_MISSED_REVENUE_REJECT="Missed revenue (reject)"
    STAT_MISSED_REVENUE_REJECT_BY_USER="Missed revenue (reject by user)"
    STAT_MISSED_REVENUE="Missed revenue (total)"
    LEASE_STAT_PRICE="Price"    
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""        
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(PriceProbe.COUNTER_REVENUE, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(PriceProbe.COUNTER_SURCHARGE, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(PriceProbe.COUNTER_MISSED_REVENUE_UNDERCHARGE, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(PriceProbe.COUNTER_MISSED_REVENUE_REJECT, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(PriceProbe.COUNTER_MISSED_REVENUE_REJECT_BY_USER, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(PriceProbe.STAT_REVENUE)
        self.accounting.create_stat(PriceProbe.STAT_SURCHARGE)
        self.accounting.create_stat(PriceProbe.STAT_MISSED_REVENUE_UNDERCHARGE)
        self.accounting.create_stat(PriceProbe.STAT_MISSED_REVENUE_REJECT)
        self.accounting.create_stat(PriceProbe.STAT_MISSED_REVENUE_REJECT_BY_USER)
        self.accounting.create_stat(PriceProbe.STAT_MISSED_REVENUE)
        self.accounting.create_lease_stat(PriceProbe.LEASE_STAT_PRICE)


    def finalize_accounting(self):
        """See AccountingProbe.finalize_accounting"""        
        self._set_stat_from_counter(PriceProbe.STAT_REVENUE, PriceProbe.COUNTER_REVENUE)
        self._set_stat_from_counter(PriceProbe.STAT_SURCHARGE, PriceProbe.COUNTER_SURCHARGE)
        self._set_stat_from_counter(PriceProbe.STAT_MISSED_REVENUE_UNDERCHARGE, PriceProbe.COUNTER_MISSED_REVENUE_UNDERCHARGE)
        self._set_stat_from_counter(PriceProbe.STAT_MISSED_REVENUE_REJECT, PriceProbe.COUNTER_MISSED_REVENUE_REJECT)
        self._set_stat_from_counter(PriceProbe.STAT_MISSED_REVENUE_REJECT_BY_USER, PriceProbe.COUNTER_MISSED_REVENUE_REJECT_BY_USER)
     
        r1 = self.accounting.get_last_counter_value(PriceProbe.COUNTER_MISSED_REVENUE_UNDERCHARGE)
        r2 = self.accounting.get_last_counter_value(PriceProbe.COUNTER_MISSED_REVENUE_REJECT)
        r3 = self.accounting.get_last_counter_value(PriceProbe.COUNTER_MISSED_REVENUE_REJECT_BY_USER)
        self.accounting.set_stat(PriceProbe.STAT_MISSED_REVENUE, r1+r2+r3)


    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""        
        if lease.get_state() == Lease.STATE_DONE:
            self.accounting.incr_counter(PriceProbe.COUNTER_REVENUE, lease.id, lease.price)
            self.accounting.set_lease_stat(PriceProbe.LEASE_STAT_PRICE, lease.id, lease.price)

        if lease.extras.has_key("simul_userrate"):
            user_rate = float(lease.extras["simul_userrate"])
            user_price = get_policy().pricing.get_base_price(lease, user_rate)
            
            if lease.get_state() == Lease.STATE_DONE and lease.extras.has_key("rate"):
                surcharge = lease.price - get_policy().pricing.get_base_price(lease, lease.extras["rate"])
                self.accounting.incr_counter(PriceProbe.COUNTER_MISSED_REVENUE_UNDERCHARGE, lease.id, user_price - lease.price)
                self.accounting.incr_counter(PriceProbe.COUNTER_SURCHARGE, lease.id, surcharge)
            elif lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(PriceProbe.COUNTER_MISSED_REVENUE_REJECT, lease.id, user_price)
            elif lease.get_state() == Lease.STATE_REJECTED_BY_USER:
                self.accounting.incr_counter(PriceProbe.COUNTER_MISSED_REVENUE_REJECT_BY_USER, lease.id, user_price)
            
                
class DeadlineProbe(AccountingProbe):
    """
    Collects information from deadline leases

    """
    LEASE_STAT_SLOWDOWN="Slowdown"
    
    def __init__(self, accounting):
        """See AccountingProbe.__init__"""
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_lease_stat(DeadlineProbe.LEASE_STAT_SLOWDOWN)

    def at_lease_done(self, lease):
        """See AccountingProbe.at_lease_done"""
        if lease.get_type() == Lease.DEADLINE:
            if lease.get_state() == Lease.STATE_DONE:
                time_on_dedicated = lease.duration.original
                time_on_loaded = lease.end - lease.start.requested
                bound = TimeDelta(seconds=10)
                if time_on_dedicated < bound:
                    time_on_dedicated = bound         
                slowdown = time_on_loaded / time_on_dedicated       
                self.accounting.set_lease_stat(DeadlineProbe.LEASE_STAT_SLOWDOWN, lease.id, slowdown)
