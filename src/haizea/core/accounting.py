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

import os
import os.path
import haizea.common.constants as constants
from haizea.core.leases import Lease
from haizea.common.utils import pickle, get_config, get_clock
from errno import EEXIST

class AccountingData(object):
    def __init__(self):
        # Counters
        self.counters = {}
        self.counter_avg_type = {}
        
        # Per-lease data
        self.lease_stats = {}

        # Per-run data ("statistics")
        self.stats = {}

        # Leases
        self.leases = {}
        
        # Attributes
        self.attrs = {}
        
        self.starttime = None
        

class AccountingDataCollection(object):
    
    AVERAGE_NONE=0
    AVERAGE_NORMAL=1
    AVERAGE_TIMEWEIGHTED=2
    
    def __init__(self, manager, datafile):
        self.data = AccountingData()
        self.manager = manager
        self.datafile = datafile
        self.probes = []
        
        attrs = get_config().get_attrs()
        for attr in attrs:
            self.data.attrs[attr] = get_config().get_attr(attr)

    def add_probe(self):
        pass

    def create_counter(self, counter_id, avgtype):
        self.data.counters[counter_id] = []
        self.data.counter_avg_type[counter_id] = avgtype

    def incr_counter(self, counter_id, lease_id = None):
        time = get_clock().get_time()
        self.append_stat(counter_id, self.data.counters[counter_id] + 1, lease_id, time)

    def decr_counter(self, counter_id, lease_id = None):
        time = get_clock().get_time()
        self.append_stat(counter_id, self.data.counters[counter_id] - 1, lease_id, time)
        
    def append_stat(self, counter_id, value, lease_id = None, time = None):
        if time == None:
            time = get_clock().get_time()
        if len(self.data.counters[counter_id]) > 0:
            prevtime = self.data.counters[counter_id][-1][0]
        else:
            prevtime = None

        if time == prevtime:
            self.data.counters[counter_id][-1][2] = value
        else:
            self.data.counters[counter_id].append([time, lease_id, value])

        
    def start(self, time):
        self.data.starttime = time
        
        # Start the counters
        for counter_id in self.data.counters:
            self.append_stat(counter_id, 0, time = time)

        
    def stop(self):
        time = get_clock().get_time()

        # Stop the counters
        for counter_id in self.data.counters:
            self.append_stat(counter_id, self.data.counters[counter_id][-1][2], time=time)
        
        # Add the averages
        for counter_id in self.data.counters:
            l = self.normalize_times(self.data.counters[counter_id])
            avgtype = self.data.counter_avg_type[counter_id]
            if avgtype == constants.AVERAGE_NONE:
                self.data.counters[counter_id] = self.add_no_average(l)
            elif avgtype == constants.AVERAGE_NORMAL:
                self.data.counters[counter_id] = self.add_average(l)
            elif avgtype == constants.AVERAGE_TIMEWEIGHTED:
                self.data.counters[counter_id] = self.add_timeweighted_average(l)
            
    def normalize_times(self, data):
        return [((v[0] - self.data.starttime).seconds, v[1], v[2]) for v in data]
        
    def add_no_average(self, data):
        return [(v[0], v[1], v[2], None) for v in data]
    
    def add_timeweighted_average(self, data):
        accum = 0
        prev_time = None
        prev_value = None
        stats = []
        for v in data:
            time = v[0]
            lease_id = v[1]
            value = v[2]
            if prev_time != None:
                timediff = time - prev_time
                weighted_value = prev_value*timediff
                accum += weighted_value
                avg = accum/time
            else:
                avg = value
            stats.append((time, lease_id, value, avg))
            prev_time = time
            prev_value = value
        
        return stats        
    
    def add_average(self, data):
        accum = 0
        count = 0
        stats = []
        for v in data:
            value = v[2]
            accum += value
            count += 1
            avg = accum/count
            stats.append((v[0], v[1], value, avg))
        
        return stats          
    
    def save_to_disk(self):
        try:
            dirname = os.path.dirname(self.datafile)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        except OSError, e:
            if e.errno != EEXIST:
                raise e
    
        # Add lease data
        leases = self.manager.scheduler.completed_leases.entries
        # Remove some data that won't be necessary in the reporting tools
        for l in leases.values():
            l.clear_rrs()
            l.logger = None
            self.data.leases[l.id] = l

        # Save data
        pickle(self.data, self.datafile)

class AccountingProbe(object):
    # Base abstract class for an accounting probe
    def __init__(self, accounting):
        self.accounting = accounting
    
    def create_counters(self):
        pass
    
    def finalize_accounting(self):
        pass
    
    def at_timestep(self, lease_scheduler):
        pass
    
    def at_lease_request(self, lease):
        pass
    
    def at_lease_done(self, lease):
        pass
                
                
class ARProbe(object):
    
    COUNTER_ARACCEPTED="Accepted AR"
    COUNTER_ARREJECTED="Rejected AR"
    
    def __init__(self):
        AccountingProbe.__init__(self, accounting)

    def create_counters(self):
        self.accounting.create_counter(ARProbe.COUNTER_ARACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(ARProbe.COUNTER_ARREJECTED, AccountingDataCollection.AVERAGE_NONE)

    def finalize_accounting(self):
        pass
        # Accepted/rejected

    def at_lease_request(self, lease):
        pass
        # If lease is accepted, ...
        # If lease is rejected, ...

class IMProbe(object):
    
    COUNTER_IMACCEPTED="Accepted Immediate"
    COUNTER_IMREJECTED="Rejected Immediate"
    
    def __init__(self):
        AccountingProbe.__init__(self, accounting)
    
    def create_counters(self):
        self.accounting.create_counter(IMProbe.COUNTER_IMACCEPTED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(IMProbe.COUNTER_IMREJECTED, AccountingDataCollection.AVERAGE_NONE)

    def finalize_accounting(self):
        pass
        # Accepted/rejected

    def at_lease_request(self, lease):
        pass
        # If lease is accepted, ...
        # If lease is rejected, ...

class BEProbe(object):
    
    COUNTER_BESTEFFORTCOMPLETED="Best-effort completed"
    COUNTER_QUEUESIZE="Queue size"    
    
    def __init__(self):
        AccountingProbe.__init__(self, accounting)
    
    def create_counters(self):
        self.accounting.create_counter(BEProbe.COUNTER_BESTEFFORTCOMPLETED, AccountingDataCollection.AVERAGE_NONE)
        self.accounting.create_counter(BEProbe.COUNTER_QUEUESIZE, AccountingDataCollection.AVERAGE_TIMEWEIGHTED)

    def finalize_accounting(self):
        pass
        # Compute all-best-effort-done
    
    def at_timestep(self, lease_scheduler):
        pass
        # Get queue size

    def at_lease_done(self, lease):
        pass
        # Compute waiting time and slowdown

class UtilizationProbe(object):
    
    COUNTER_DISKUSAGE="Disk usage"
    COUNTER_UTILIZATION="Resource utilization"        
    
    def __init__(self):
        AccountingProbe.__init__(self, accounting)
    
    def create_counters(self):
        self.accounting.create_counter(UtilizationProbe.COUNTER_DISKUSAGE, constants.AVERAGE_NONE)
        self.accounting.create_counter(UtilizationProbe.COUNTER_UTILIZATION, constants.AVERAGE_NONE)
    
    def at_timestep(self, lease_scheduler):
        self.accounting.append_stat(constants.COUNTER_UTILIZATION, util)

