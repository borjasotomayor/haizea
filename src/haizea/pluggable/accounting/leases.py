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

from haizea.core.accounting import AccountingProbe, AccountingDataCollection
from haizea.core.leases import Lease
                
class ARProbe(AccountingProbe):
    
    COUNTER_ACCEPTED="Accepted AR"
    COUNTER_REJECTED="Rejected AR"
    STAT_ACCEPTED="Total accepted AR"
    STAT_REJECTED="Total rejected AR"
    
    def __init__(self, accounting):
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(ARProbe.COUNTER_ACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(ARProbe.COUNTER_REJECTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(ARProbe.STAT_ACCEPTED)
        self.accounting.create_stat(ARProbe.STAT_REJECTED)

    def finalize_accounting(self):
        self._set_stat_from_counter(ARProbe.STAT_ACCEPTED, ARProbe.COUNTER_ACCEPTED)
        self._set_stat_from_counter(ARProbe.STAT_REJECTED, ARProbe.COUNTER_REJECTED)

    def at_lease_request(self, lease):
        if lease.get_type() == Lease.ADVANCE_RESERVATION:
            if lease.get_state() == Lease.STATE_PENDING:
                self.accounting.incr_counter(ARProbe.COUNTER_ACCEPTED, lease.id)
            elif lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(ARProbe.COUNTER_REJECTED, lease.id)

    def at_lease_done(self, lease):
        if lease.get_type() == Lease.ADVANCE_RESERVATION:
            if lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(ARProbe.COUNTER_REJECTED, lease.id)


class IMProbe(AccountingProbe):

    COUNTER_ACCEPTED="Accepted Immediate"
    COUNTER_REJECTED="Rejected Immediate"
    STAT_ACCEPTED="Total accepted Immediate"
    STAT_REJECTED="Total rejected Immediate"
    
    def __init__(self, accounting):
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(IMProbe.COUNTER_ACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(IMProbe.COUNTER_REJECTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_stat(IMProbe.STAT_ACCEPTED)
        self.accounting.create_stat(IMProbe.STAT_REJECTED)

    def finalize_accounting(self):
        self._set_stat_from_counter(IMProbe.STAT_ACCEPTED, IMProbe.COUNTER_ACCEPTED)
        self._set_stat_from_counter(IMProbe.STAT_REJECTED, IMProbe.COUNTER_REJECTED)

    def at_lease_request(self, lease):
        if lease.get_type() == Lease.IMMEDIATE:
            if lease.get_state() == Lease.STATE_PENDING:
                self.accounting.incr_counter(IMProbe.COUNTER_ACCEPTED, lease.id)
            elif lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(IMProbe.COUNTER_REJECTED, lease.id)

    def at_lease_done(self, lease):
        if lease.get_type() == Lease.IMMEDIATE:
            if lease.get_state() == Lease.STATE_REJECTED:
                self.accounting.incr_counter(IMProbe.COUNTER_REJECTED, lease.id)



class BEProbe(AccountingProbe):
    
    COUNTER_BESTEFFORTCOMPLETED="Best-effort completed"
    COUNTER_QUEUESIZE="Queue size"
    LEASE_STAT_WAITINGTIME="Waiting time"
    LEASE_STAT_SLOWDOWN="Slowdown"
    STAT_BESTEFFORTCOMPLETED="Total best-effort completed"
    STAT_ALLBESTEFFORT="all-best-effort"
    
    
    def __init__(self, accounting):
        AccountingProbe.__init__(self, accounting)
        self.accounting.create_counter(BEProbe.COUNTER_BESTEFFORTCOMPLETED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(BEProbe.COUNTER_QUEUESIZE, AccountingDataCollection.AVERAGE_TIMEWEIGHTED)
        self.accounting.create_lease_stat(BEProbe.LEASE_STAT_WAITINGTIME)
        self.accounting.create_lease_stat(BEProbe.LEASE_STAT_SLOWDOWN)
        self.accounting.create_stat(BEProbe.STAT_BESTEFFORTCOMPLETED)
        self.accounting.create_stat(BEProbe.STAT_ALLBESTEFFORT)
    
    def finalize_accounting(self):
        self._set_stat_from_counter(BEProbe.STAT_BESTEFFORTCOMPLETED, BEProbe.COUNTER_BESTEFFORTCOMPLETED)
        all_best_effort = self.accounting.data.counters[BEProbe.COUNTER_BESTEFFORTCOMPLETED][-1][0]
        self.accounting.set_stat(BEProbe.STAT_ALLBESTEFFORT, all_best_effort)
    
    def at_timestep(self, lease_scheduler):
        queue_len = lease_scheduler.queue.length()
        self.accounting.append_to_counter(BEProbe.COUNTER_QUEUESIZE, queue_len)

    def at_lease_done(self, lease):
        if lease.get_type() == Lease.BEST_EFFORT:
            wait = lease.get_waiting_time().seconds
            self.accounting.set_lease_stat(BEProbe.LEASE_STAT_WAITINGTIME, lease.id, wait)
            self.accounting.set_lease_stat(BEProbe.LEASE_STAT_SLOWDOWN, lease.id, lease.get_slowdown())
            self.accounting.incr_counter(BEProbe.COUNTER_BESTEFFORTCOMPLETED, lease.id)
