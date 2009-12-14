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
from haizea.common.utils import pickle, get_clock
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
    """Accounting data collection
    
    This class provides a framework to collect data while Haizea is running.
    It is designed so that the code that collects the data is placed in
    separate classes (the "probes"; a probe must be a child class of
    AccountingProbe). Probes can collect three types of data:
    
    Accounting probes can collect three types of data:

     - Per-lease data: Data attributable to individual leases or derived 
       from how each lease was scheduled. 
     - Per-run data: Data from a single run of Haizea. 
     - Counters: A counter is a time-ordered list showing how some metric 
       varied throughout a single run of Haizea. 
    
    The the AccountingDataCollection class takes care of calling these 
    probes at three points when Haizea is running:
    (1) at every time step, (2) when a lease is requested, and (3) when a
    lease is done.

    """
    
    AVERAGE_NONE=0
    AVERAGE_NORMAL=1
    AVERAGE_TIMEWEIGHTED=2


    def __init__(self, datafile, attrs):
        """Constructor
        
        @param datafile: Path to file where accounting data will be saved
        @type datafile: C{str}
        @param attrs: Run attributes
        @type attrs: C{dict}
        """
        self.__data = AccountingData()
        self.__datafile = datafile
        self.__probes = []

        self.__data.attrs = attrs


    def add_probe(self, probe):
        """Adds a new accounting probe
        
        @param probe: Probe to add
        @type probe: L{AccountingProbe}
        """
        self.__probes.append(probe)


    def create_counter(self, counter_id, avgtype):
        """Adds a new counter.
        
        Counters can store not just the value of the counter throughout
        time, but also a running average. This is specified with the
        avgtype parameter, which can be equal to:
        
         - AccountingDataCollection.AVERAGE_NONE: Don't compute an average
         - AccountingDataCollection.AVERAGE_NORMAL: For each entry, compute
           the average of all the values including and preceding that entry.
         - AccountingDataCollection.AVERAGE_TIMEWEIGHTED: For each entry,
           compute the average of all the values including and preceding
           that entry, weighing the average according to the time between
           each entry.
        
        @param counter_id: Name of the counter
        @type counter_id: C{str}
        @param avgtype: Type of average to compute
        @type avgtype: C{int}
        """        
        self.__data.counters[counter_id] = []
        self.__data.counter_avg_type[counter_id] = avgtype


    def create_lease_stat(self, stat_id):
        """Adds a new per-lease type of data ("stat").
        
        @param stat_id: Name of the stat
        @type stat_id: C{str}
        """        
        self.__data.lease_stats_names.append(stat_id)


    def create_stat(self, stat_id):
        """Adds a new per-run type of data ("stat").
        
        @param stat_id: Name of the stat
        @type stat_id: C{str}
        """        
        self.__data.stats_names.append(stat_id)


    def incr_counter(self, counter_id, lease_id = None, amount = 1):
        """Increment a counter
        
        @param counter_id: Name of the counter
        @type counter_id: C{str}
        @param lease_id: Optionally, the lease that caused this increment.
        @type lease_id: C{int}
        """        
        time = get_clock().get_time()
        self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2] + amount, lease_id)


    def decr_counter(self, counter_id, lease_id = None, amount = 1):
        """Decrement a counter
        
        @param counter_id: Name of the counter
        @type counter_id: C{str}
        @param lease_id: Optionally, the ID of the lease that caused this increment.
        @type lease_id: C{int}
        """        
        time = get_clock().get_time()
        self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2] - amount, lease_id)


    def append_to_counter(self, counter_id, value, lease_id = None):
        """Append a value to a counter
        
        @param counter_id: Name of the counter
        @type counter_id: C{str}
        @param value: Value to append
        @type value: C{int} or C{float}
        @param lease_id: Optionally, the ID of the lease that caused this increment.
        @type lease_id: C{int}
        """      
        time = get_clock().get_time()
        if len(self.__data.counters[counter_id]) > 0:
            prevtime = self.__data.counters[counter_id][-1][0]
            prevlease = self.__data.counters[counter_id][-1][1]
            prevval = self.__data.counters[counter_id][-1][2]
            if time == prevtime:
                self.__data.counters[counter_id][-1][2] = value
            else:
                if prevlease != lease_id or prevval != value:
                    self.__data.counters[counter_id].append([time, lease_id, value])
        else:
            self.__data.counters[counter_id].append([time, lease_id, value])



    def get_last_counter_time(self, counter_id):
        """Get the time of the last entry in a counter
        
        """        
        return self.__data.counters[counter_id][-1][0]
    

    def get_last_counter_value(self, counter_id):
        """Get the value of the last entry in a counter
        
        """        
        return self.__data.counters[counter_id][-1][2]
            

    def set_lease_stat(self, stat_id, lease_id, value):
        """Set the value of a per-lease datum
        
        @param stat_id: Name of the stat
        @type stat_id: C{str}
        @param lease_id: The ID of the lease the value is associated to
        @type lease_id: C{int}
        @param value: Value of the stat
        @type value: C{int} or C{float}
        """             
        self.__data.lease_stats.setdefault(lease_id, {})[stat_id] = value


    def set_stat(self, stat_id, value):
        """Set the value of a per-run datum
        
        @param stat_id: Name of the stat
        @type stat_id: C{str}
        @param value: Value of the stat
        @type value: C{int} or C{float}
        """  
        self.__data.stats[stat_id] = value
        
            
    def start(self, time):
        """Start collecting data
        
        @param time: Time at which data started being collected
        @type time: L{mx.DateTime}
        """        
        self.__data.starttime = time
        
        # Start the counters
        for counter_id in self.__data.counters:
            self.append_to_counter(counter_id, 0)

        
    def stop(self):
        """Stop collecting data
        
        """               
        time = get_clock().get_time()

        # Stop the counters
        for counter_id in self.__data.counters:
            self.append_to_counter(counter_id, self.__data.counters[counter_id][-1][2])
        
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
        """Save accounting data to disk.
        
        @param leases: List of leases to be saved to disk
        @type leases: List of L{Lease}s
        """           
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
        """Invoke the probes' at_timestep handlers.
        
        @param lease_scheduler: Lease Scheduler
        @type lease_scheduler: L{LeaseScheduler}
        """        
        for probe in self.__probes:
            probe.at_timestep(lease_scheduler)
    
    
    def at_lease_request(self, lease):
        """Invoke the probes' at_lease_request handlers.
        
        @param lease: Requested lease
        @type lease: L{Lease}
        """        
        for probe in self.__probes:
            probe.at_lease_request(lease)

    
    def at_lease_done(self, lease):
        """Invoke the probes' at_lease_done handlers.
        
        @param lease: Lease that was completed
        @type lease: L{Lease}
        """        
        for probe in self.__probes:
            probe.at_lease_done(lease)
                
                
    def __normalize_times(self, counter):
        return [((v[0] - self.__data.starttime).seconds, v[1], v[2]) for v in counter]
        
        
    def __add_no_average(self, counter):
        return [(v[0], v[1], v[2], None) for v in counter]
    
    
    def __add_timeweighted_average(self, counter):
        accum = 0
        prev_time = None
        prev_value = None
        stats = []
        for v in counter:
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
    
    
    def __add_average(self, counter):
        accum = 0
        count = 0
        stats = []
        for v in counter:
            value = v[2]
            accum += value
            count += 1
            avg = accum/count
            stats.append((v[0], v[1], value, avg))
        
        return stats          
    

class AccountingProbe(object):
    """Base class for accounting probes
    
    Accounting probes must extend this class, and can override some of
    the methods to make sure the accounting framework runs the probe
    at certain points (see method documentation for details on when
    to override a method).

    """
    def __init__(self, accounting):
        """Constructor
        
        Child classes must use their constructors to create counters 
        (with AccountingDataCollection.create_counter) and specify 
        per-lease data (with AccountingDataCollection.create_lease_stat)
        and per-run data (with AccountingDataCollection.create_stat).
        """
        self.accounting = accounting
        
    def finalize_accounting(self):
        """Finalize data collection.
        
        Override this method to perform any actions when data collection
        stops. This is usually where per-run data is computed.
        """
        pass
    
    def at_timestep(self, lease_scheduler):
        """Collect data at a timestep.
        
        Override this method to perform any actions every time the
        Haizea scheduler wakes up.

        @param lease_scheduler: Lease Scheduler
        @type lease_scheduler: L{LeaseScheduler}
        """
        pass
    
    def at_lease_request(self, lease):
        """Collect data after a lease request.
        
        Override this method to perform any actions after a lease
        has been requested.
 
        @param lease: Requested lease
        @type lease: L{Lease}
        """
        pass
    
    def at_lease_done(self, lease):
        """Collect data when a lease is done (this includes successful
        completion and rejected/cancelled/failed leases).
        
        @param lease: Lease that was completed
        @type lease: L{Lease}
        """        
        pass
    
    def _set_stat_from_counter(self, stat_id, counter_id):
        """Convenience function that sets the value of a per-run
        stat with the last value of a counter.
        
        @param stat_id: Name of per-run stat
        @type stat_id: C{str}
        @param counter_id: Name of counter
        @type counter_id: C{str}
        """        
        value = self.accounting.get_last_counter_value(counter_id)
        self.accounting.set_stat(stat_id, value)
                           
