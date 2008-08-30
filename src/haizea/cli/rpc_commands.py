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
from haizea.cli.optionparser import OptionParser, Option
from haizea.common.constants import state_str
from mx.DateTime import TimeDelta
import haizea.common.defaults as defaults
from haizea.cli import Command
import xmlrpclib
import sys
from mx.DateTime import ISO

class RPCCommand(Command):
    def __init__(self, argv):
        Command.__init__(self, argv)
        self.optparser.add_option(Option("-s", "--server", action="store", type="string", dest="server", default=defaults.RPC_URI))

    def create_rpc_proxy(self, server):
        return xmlrpclib.ServerProxy(server, allow_none=True)

class haizea_request_lease(RPCCommand):
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
        
        self.optparser.add_option(Option("-t", "--start", action="store", type="string", dest="start"))
        self.optparser.add_option(Option("-d", "--duration", action="store", type="string", dest="duration"))
        self.optparser.add_option(Option("-n", "--numnodes", action="store", type="int", dest="numnodes"))
        self.optparser.add_option(Option("--preemptible", action="store_true", dest="preemptible"))
        self.optparser.add_option(Option("--non-preemptible", action="store_false", dest="preemptible"))
        self.optparser.add_option(Option("-c", "--cpu", action="store", type="float", dest="cpu"))
        self.optparser.add_option(Option("-m", "--mem", action="store", type="int", dest="mem"))
        self.optparser.add_option(Option("-i", "--vmimage", action="store", type="string", dest="vmimage"))
        self.optparser.add_option(Option("-z", "--vmimagesize", action="store", type="int", dest="vmimagesize"))
        
        self.parse_options()
        
    def run(self):
        if self.opt.preemptible == None:
            preemptible = False
        else:
            preemptible = self.opt.preemptible
            
        server = self.create_rpc_proxy(self.opt.server)
        
        try:
            lease_id = server.create_lease(self.opt.start, self.opt.duration, preemptible, self.opt.numnodes, 
                                self.opt.cpu, self.opt.mem, self.opt.vmimage, self.opt.vmimagesize)
            print "Lease submitted correctly."
            print "Lease ID: %i" % lease_id
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
        
class haizea_cancel_lease(RPCCommand):
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
        
        self.optparser.add_option(Option("-l", "--lease", action="store", type="int", dest="lease"))
        
        self.parse_options()
        
    def run(self):
        server = self.create_rpc_proxy(self.opt.server)
        
        try:
            code = server.cancel_lease(self.opt.lease)
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
        
class haizea_list_leases(RPCCommand):
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
        self.parse_options()
        
    def run(self):
        server = self.create_rpc_proxy(self.opt.server)
        
        fields = [("id","ID", 3),
                  ("type","Type", 4),
                  ("state","State", 9),
                  ("start_req", "Starting time", 22),
                  ("duration_req", "Duration", 12),
                  ("numnodes", "Nodes", 3)]
        
        try:
            leases = server.get_leases()
            console_table_printer(fields, leases)
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg

class haizea_list_hosts(RPCCommand):
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
        self.parse_options()
        
    def run(self):
        server = self.create_rpc_proxy(self.opt.server)
        
        fields = [("id","ID", 3),
                  ("hostname","Hostname", 10),
                  ("cpu","CPUs", 6),
                  ("mem", "Mem", 6)]
        
        try:
            hosts = server.get_hosts()
            console_table_printer(fields, hosts)
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg

class haizea_show_queue(RPCCommand):
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
        self.parse_options()
        
    def run(self):
        server = self.create_rpc_proxy(self.opt.server)
        
        fields = [("id","ID", 3),
                  ("type","Type", 4),
                  ("state","State", 9),
                  ("start_sched", "Sched. Start time", 22),
                  ("duration_req", "Duration", 12),
                  ("numnodes", "Nodes", 3)]
        
        try:
            leases = server.get_queue()
            console_table_printer(fields, leases)
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg

def console_table_printer(fields, values):
    print "\33[1m\33[4m",
    for (name,pname,width) in fields:
        width = max(len(pname),width) + 1
        centered = pname.ljust(width)
        print centered,
    print "\33[0m"
    for v in values:
        for (name,pname,width) in fields:
            value = pretty_print_rpcvalue(name, v[name])
            width = max(len(name),width)
            print " %s" % str(value).ljust(width),
        print
    
def pretty_print_rpcvalue(name, value):
    if name == "state":
        value = state_str(value)
    elif name == "duration_req":
        value = TimeDelta(seconds=value)
    elif name == "start_req":
        if value != None:
            value = ISO.ParseDateTime(value.value)

    return value
