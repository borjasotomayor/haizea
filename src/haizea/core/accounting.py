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

"""Classes used to collect data"""

import os
import os.path
from haizea.common.utils import pickle, get_config, get_clock
from errno import EEXIST

class AccountingData(object):
    """A container for all the accounting data. When Haizea saves
    accounting data, it does so by pickling an object of this class.
    """
    
    def __init__(self):
        """Initializes all the counters and data to empty values"""
        # Counters
        self.counters = {}
        self.counter_avg_type = {}
        
        # Per-lease data
        self.lease_stats_names = []
        self.lease_stats = {}

        # Per-run data ("statistics")
        self.stats_names = []
        self.stats = {}

        # Leases
        self.leases = {}
        
        # Attributes
        self.attrs = {}
        
        self.starttime = None
        

class AccountingDataCollection(object):
    """Accounting data collection """
    
    AVERAGE_NONE=0
    AVERAGE_NORMAL=1
    AVERAGE_TIMEWEIGHTED=2
    
    def __init__(self, datafile):
        """Constructor
        
        @param datafile: Path to file where accounting data will be saved
        @type datafile: C{str}
        """
        self.__data = AccountingData()
        self.__datafile = datafile
        self.__probes = []
        
        attrs = get_config().get_attrs()
        for attr in attrs:
            self.__data.attrs[attr] = get_config().get_attr(attr)

    def add_probe(self, probe):
        """Adds a new accounting probe
        
        @param probe: Probe to add
        @type probe: L{AccountingProbe}
        """
        self.__probes.append(probe)

    def create_counter(self, counter_id, avgtype):
        self.__data.counters[counter_id] = []
        self.__data.counter_avg_type[counter_id] = avgtype

    def create_lease_stat(self, stat_id):
        self.__data.lease_stats_names.append(stat_id)

    def create_stat(self, stat_id):
        self.__data.stats_names.append(stat_id)

    def incr_counter(self, counter_id, lease_id = None):
        time = get_clock().get_time()
        self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2] + 1, lease_id, time)

    def decr_counter(self, counter_id, lease_id = None):
        time = get_clock().get_time()
        self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2] - 1, lease_id, time)
        
    def set_lease_stat(self, stat_id, lease_id, value):
        self.__data.lease_stats.setdefault(lease_id, {})[stat_id] = value

    def set_stat(self, stat_id, value):
        self.__data.stats[stat_id] = value
        
    def append_to_counter(self, counter_id, value, lease_id = None, time = None):
        if time == None:
            time = get_clock().get_time()
        if len(self.__data.counters[counter_id]) > 0:
            prevtime = self.__data.counters[counter_id][-1][0]
        else:
            prevtime = None

        if time == prevtime:
            self.__data.counters[counter_id][-1][2] = value
        else:
            self.__data.counters[counter_id].append([time, lease_id, value])

    def get_last_counter_time(self, counter_id):
        return self.__data.counters[counter_id][-1][0]

    def get_last_counter_value(self, counter_id):
        return self.__data.counters[counter_id][-1][2]
        
    def start(self, time):
        self.__data.starttime = time
        
        # Start the counters
        for counter_id in self.__data.counters:
            self.append_to_counter(counter_id, 0, time = time)

        
    def stop(self):
        time = get_clock().get_time()

        # Stop the counters
        for counter_id in self.__data.counters:
            self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2], time=time)
        
        # Add the averages
        for counter_id in self.__data.counters:
            l = self.__normalize_times(self.__data.counters[counter_id])
            avgtype = self.__data.counter_avg_type[counter_id]
            if avgtype == AccountingDataCollection.AVERAGE_NONE:
                self.__data.counters[counter_id] = self.__add_no_average(l)
            elif avgtype == AccountingDataCollection.AVERAGE_NORMAL:
                self.__data.counters[counter_id] = self.__add_average(l)
            elif avgtype == AccountingDataCollection.AVERAGE_TIMEWEIGHTED:
                self.__data.counters[counter_id] = self.__add_timeweighted_average(l)
        
        for probe in self.__probes:
            probe.finalize_accounting()
            
    def save_to_disk(self, leases):
        try:
            dirname = os.path.dirname(self.__datafile)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        except OSError, e:
            if e.errno != EEXIST:
                raise e
    
        # Add lease data
        # Remove some data that won't be necessary in the reporting tools
        for l in leases.values():
            l.clear_rrs()
            l.logger = None
            self.__data.leases[l.id] = l

        # Save data
        pickle(self.__data, self.__datafile)
            
    def at_timestep(self, lease_scheduler):
        for probe in self.__probes:
            probe.at_timestep(lease_scheduler)
    
    def at_lease_request(self, lease):
        for probe in self.__probes:
            probe.at_lease_request(lease)
    
    def at_lease_done(self, lease):
        for probe in self.__probes:
            probe.at_lease_done(lease)
                
    def __normalize_times(self, data):
        return [((v[0] - self.__data.starttime).seconds, v[1], v[2]) for v in data]
        
    def __add_no_average(self, data):
        return [(v[0], v[1], v[2], None) for v in data]
    
    def __add_timeweighted_average(self, data):
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
    
    def __add_average(self, data):
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
    

class AccountingProbe(object):
    # Base abstract class for an accounting probe
    def __init__(self, accounting):
        self.accounting = accounting
        
    def finalize_accounting(self):
        pass
    
    def at_timestep(self, lease_scheduler):
        pass
    
    def at_lease_request(self, lease):
        pass
    
    def at_lease_done(self, lease):
        pass
    
    def _set_stat_from_counter(self, stat_id, counter_id):
        value = self.accounting.get_last_counter_value(counter_id)
        self.accounting.set_stat(stat_id, value)
                           

