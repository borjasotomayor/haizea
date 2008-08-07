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

import os
import os.path
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from haizea.common.utils import pickle
from errno import EEXIST

class StatsData(object):
    def __init__(self):
        # Counters
        self.counters = {}
        self.counter_lists = {}
        self.counter_avg_type = {}
    
        # What are the nodes doing?
        self.nodes = {}      
        
        # Lease data
        self.leases = {}
        
    def get_waiting_times(self):
        waiting_times = {}
        for lease_id in self.leases:
            lease = self.leases[lease_id]
            if isinstance(lease, ds.BestEffortLease):
                waiting_times[lease_id] = lease.get_waiting_time()
        return waiting_times

    def get_slowdowns(self):
        slowdowns = {}
        for lease_id in self.leases:
            lease = self.leases[lease_id]
            if isinstance(lease, ds.BestEffortLease):
                slowdowns[lease_id] = lease.get_slowdown()
        return slowdowns

class StatsCollection(object):
    def __init__(self, rm, datafile):
        self.data = StatsData()
        self.rm = rm
        self.datafile = datafile   
        self.starttime = None

    def create_counter(self, counter_id, avgtype, initial=0):
        self.data.counters[counter_id] = initial
        self.data.counter_lists[counter_id] = []
        self.data.counter_avg_type[counter_id] = avgtype

    def incr_counter(self, counter_id, lease_id = None):
        time = self.rm.clock.get_time()
        self.append_stat(counter_id, self.data.counters[counter_id] + 1, lease_id, time)

    def decr_counter(self, counter_id, lease_id = None):
        time = self.rm.clock.get_time()
        self.append_stat(counter_id, self.data.counters[counter_id] - 1, lease_id, time)
        
    def append_stat(self, counter_id, value, lease_id = None, time = None):
        if time == None:
            time = self.rm.clock.get_time()
        if len(self.data.counter_lists[counter_id]) > 0:
            prevtime = self.data.counter_lists[counter_id][-1][0]
        else:
            prevtime = None
        self.data.counters[counter_id] = value
        if time == prevtime:
            self.data.counter_lists[counter_id][-1][2] = value
        else:
            self.data.counter_lists[counter_id].append([time, lease_id, value])

        
    def start(self, time):
        self.starttime = time
        
        # Start the counters
        for counter_id in self.data.counters:
            initial = self.data.counters[counter_id]
            self.append_stat(counter_id, initial, time = time)
        
        # Start the doing
        numnodes = self.rm.resourcepool.getNumNodes()
        for n in range(numnodes):
            self.data.nodes[n+1] = [(time, constants.DOING_IDLE)]

    def tick(self):
        time = self.rm.clock.get_time()
        # Update the doing
        for node in self.rm.resourcepool.nodes:
            nodenum = node.nod_id
            doing = node.getState()
            (lasttime, lastdoing) = self.data.nodes[nodenum][-1]
            if doing == lastdoing:
                # No need to update
                pass
            else:
                if lasttime == time:
                    self.data.nodes[nodenum][-1] = (time, doing)
                else:
                    self.data.nodes[nodenum].append((time, doing))
        
    def stop(self):
        time = self.rm.clock.get_time()

        # Stop the counters
        for counter_id in self.data.counters:
            self.append_stat(counter_id, self.data.counters[counter_id], time=time)
        
        # Add the averages
        for counter_id in self.data.counters:
            l = self.normalize_times(self.data.counter_lists[counter_id])
            avgtype = self.data.counter_avg_type[counter_id]
            if avgtype == constants.AVERAGE_NONE:
                self.data.counter_lists[counter_id] = self.add_no_average(l)
            elif avgtype == constants.AVERAGE_NORMAL:
                self.data.counter_lists[counter_id] = self.add_average(l)
            elif avgtype == constants.AVERAGE_TIMEWEIGHTED:
                self.data.counter_lists[counter_id] = self.add_timeweighted_average(l)
        
        # Stop the doing
        for node in self.rm.resourcepool.nodes:
            nodenum = node.nod_id
            doing = node.vm_doing
            (lasttime, lastdoing) = self.data.nodes[nodenum][-1]
            if time != lasttime:
                self.data.nodes[nodenum].append((time, doing))
                
        self.normalize_doing()
            
    def normalize_times(self, data):
        return [((v[0] - self.starttime).seconds, v[1], v[2]) for v in data]
        
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
    
    def normalize_doing(self):
        nodes = dict([(i+1, []) for i in range(self.rm.resourcepool.getNumNodes())])
        for n in self.data.nodes:
            nodes[n] = []
            prevtime = None
            prevdoing = None
            for (time, doing) in self.data.nodes[n]:
                if prevtime != None:
                    difftime = (time-prevtime).seconds
                    nodes[n].append((difftime, prevdoing))
                prevtime = time
                prevdoing = doing
        self.data.nodes = nodes
    
    def save_to_disk(self):
        try:
            dirname = os.path.dirname(self.datafile)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        except OSError, e:
            if e.errno != EEXIST:
                raise e
    
        # Add lease data
        leases = self.rm.scheduler.completedleases.entries
        # Remove some data that won't be necessary in the reporting tools
        for l in leases.values():
            l.clear_rrs()
            l.scheduler = None
            l.logger = None
            self.data.leases[l.id] = l

        # Save data
        pickle(self.data, self.datafile)

                
            
