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

from mx import DateTime
from math import ceil, floor
from cPickle import dump, load, HIGHEST_PROTOCOL
from datetime import datetime
from docutils.core import publish_string
import re
import textwrap

def generate_config_name(profile, tracefile, annotationfile, injectedfile):
    tracename=tracefile.split("/")[-1].split(".")[0]
    
    if injectedfile != None and injectedfile != "None":
        injectname=injectedfile.split("/")[-1].split(".")[0]
        name = tracename + "+" + injectname
    else:
        name = tracename
        
    if annotationfile != None and annotationfile != "None":
        annotname=annotationfile.split("/")[-1].split(".")[0]
        name += "+" + annotname        
            
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

def pickle(data, filename):
    f = open (filename, "w")
    dump(data, f, protocol = HIGHEST_PROTOCOL)
    f.close()

def unpickle(filename):
    f = open (filename, "r")
    data = load(f)
    f.close()
    return data


LEASE_ID = 1

def get_lease_id():
    global LEASE_ID
    l = LEASE_ID
    LEASE_ID += 1
    return l

def reset_lease_id_counter():
    global LEASE_ID
    LEASE_ID = 1

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
    
def import_class(fq_name):
    fq_name = fq_name.split(".")
    package_name = ".".join(fq_name[:-1])
    class_name = fq_name[-1]
    module = __import__(package_name, globals(), locals(), [class_name])
    exec("cls = module.%s" % class_name)
    return cls
    
def rst2latex(text):
    latex = textwrap.dedent(text).strip()
    latex = publish_string(latex,  writer_name="latex")
    latex = re.compile("\\\\begin{document}\n\n\\\\setlength{\\\\locallinewidth}{\\\\linewidth}\n\n(.*)\\\\end{document}", flags=re.DOTALL).search(latex)
    latex = latex.group(1)
    return latex
    
class Singleton(type):
    """ 
    A singleton metaclass. 
    From: http://en.wikipedia.org/wiki/Singleton_pattern#Python
    """
    def __init__(self, name, bases, dict):
        super(Singleton, self).__init__(name, bases, dict)
        self.instance = None
 
    def __call__(self, *args, **kw):
        if self.instance is None:
            self.instance = super(Singleton, self).__call__(*args, **kw)
 
        return self.instance
    
    def reset_singleton(self):
        self.instance = None
 
def get_config():
    from haizea.core.manager import Manager
    return Manager().config

def get_clock():
    from haizea.core.manager import Manager
    return Manager().clock

def get_policy():
    from haizea.core.manager import Manager
    return Manager().policy

def get_persistence():
    from haizea.core.manager import Manager
    return Manager().persistence

class InvalidStateMachineTransition(Exception):
    pass

class StateMachine(object):
    def __init__(self, initial_state, transitions, state_str = None):
        self.state = initial_state
        self.transitions = transitions
        self.state_str = state_str

    def change_state(self, new_state):
        valid_next_states = [x[0] for x in self.transitions[self.state]]
        if new_state in valid_next_states:
            self.state = new_state
        else:
            raise InvalidStateMachineTransition, "Invalid transition. State is %s, wanted to change to %s, can only change to %s" % (self.get_state_str(self.state), self.get_state_str(new_state), [self.get_state_str(x) for x in valid_next_states])
        
    def get_state(self):
        return self.state
    
    def get_state_str(self, state):
        if self.state_str == None:
            return "%s" % state
        else:
            return self.state_str[state]
        
class OpenNebulaXMLRPCClientSingleton(object):
    
    __metaclass__ = Singleton
    
    def __init__(self, *args, **kwargs):
        from haizea.common.opennebula_xmlrpc import OpenNebulaXMLRPCClient
        self.client = OpenNebulaXMLRPCClient(*args, **kwargs)
        
                                             