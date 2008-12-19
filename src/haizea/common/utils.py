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

from mx import DateTime
from math import ceil, floor
from cPickle import dump, load, HIGHEST_PROTOCOL
from datetime import datetime

def generate_config_name(profile, tracefile, injectedfile):
    tracename=tracefile.split("/")[-1].split(".")[0]
    
    if injectedfile != None and injectedfile != "None":
        injectname=injectedfile.split("/")[-1].split(".")[0]
        name = tracename + "+" + injectname
    else:
        name = tracename
            
    name = profile + "_" + name
    return name
    
def round_datetime_delta(d):
    return DateTime.DateTimeDelta(d.day, d.hour, d.minute, int(ceil(d.second)))

def round_datetime(d):
    d += DateTime.TimeDelta(seconds=0.5)
    return DateTime.DateTime(d.year, d.month, d.day, d.hour, d.minute, int(floor(d.second)))

def UNIX2DateTime(t):
    return DateTime.TimestampFromTicks(t)

    
def vnodemapstr(vnodes):
    if len(vnodes) == 0:
        return "UNUSED"
    else:
        return ",".join(["L"+`l`+"V"+`v` for (l, v) in vnodes])
    
# Based on http://norvig.com/python-iaq.html
def abstract():
    import inspect
    caller = inspect.stack()[1][3]
    raise NotImplementedError(caller + ' must be implemented in subclass')

def pickle(data, file):
    f = open (file, "w")
    dump(data, f, protocol = HIGHEST_PROTOCOL)
    f.close()

def unpickle(file):
    f = open (file, "r")
    data = load(f)
    f.close()
    return data


LEASE_ID = 1

def get_lease_id():
    global LEASE_ID
    l = LEASE_ID
    LEASE_ID += 1
    return l

def pretty_nodemap(nodes):
    pnodes = list(set(nodes.values()))
    normmap = [([y[0] for y in nodes.items() if y[1]==x], x) for x in pnodes]
    for m in normmap: m[0].sort()
    s = "   ".join([", ".join(["V"+`y` for y in x[0]])+" -> P" + `x[1]` for x in normmap])
    return s  

def estimate_transfer_time(size, bandwidth):
    bandwidthMBs = float(bandwidth) / 8
    seconds = size / bandwidthMBs
    return round_datetime_delta(DateTime.TimeDelta(seconds = seconds)) 
 
def xmlrpc_marshall_singlevalue(value):
    if isinstance(value, DateTime.DateTimeType):
        return datetime.fromtimestamp(value)
    elif isinstance(value, DateTime.DateTimeDeltaType):
        return value.seconds
    else:
        return value
    
class Singleton(object):
     """ 
     A singleton base class. 
     Based on: http://code.activestate.com/recipes/52558/
     """
     _singleton = None
     def __new__(cls, *args, **kwargs):
         if cls._singleton == None:
             cls._singleton = object.__new__(cls, *args, **kwargs)
         return cls._singleton
     
     @classmethod
     def get_singleton(cls):
         if '_singleton' not in vars(cls):
             return None
         else:
             return cls._singleton

 
def get_config():
    from haizea.resourcemanager.rm import ResourceManager
    return ResourceManager.get_singleton().config

def get_accounting():
    from haizea.resourcemanager.rm import ResourceManager
    return ResourceManager.get_singleton().accounting

def get_clock():
    from haizea.resourcemanager.rm import ResourceManager
    return ResourceManager.get_singleton().clock

class StateMachine(object):
    pass