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
import haizea.common.defaults as defaults
from haizea.resourcemanager.datastruct import Lease
from haizea.cli.optionparser import OptionParser, Option
from haizea.cli import Command
import xmlrpclib
import sys
from mx.DateTime import TimeDelta
from mx.DateTime import ISO

class RPCCommand(Command):
    def __init__(self, argv):
        Command.__init__(self, argv)
        self.optparser.add_option(Option("-s", "--server", action="store", type="string", dest="server", default=defaults.RPC_URI,
                                         help = """
                                         Haizea RPC server URI. If not specified, the default %s is used
                                         """ % defaults.RPC_URI))

    def create_rpc_proxy(self, server):
        return xmlrpclib.ServerProxy(server, allow_none=True)

class haizea_request_lease(RPCCommand):
    """
    Requests a new lease.
    """
    
    name = "haizea-request-lease"
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
        
        self.optparser.add_option(Option("-t", "--start", action="store", type="string", dest="start",
                                         help = """
                                         Starting time. Can be an ISO timestamp, "best_effort", or "now"
                                         """))
        self.optparser.add_option(Option("-d", "--duration", action="store", type="string", dest="duration",
                                         help = """
                                         Duration. Can be an ISO timestamp or "unlimited"
                                         """))
        self.optparser.add_option(Option("-n", "--numnodes", action="store", type="int", dest="numnodes",
                                         help = """
                                         Number of nodes.
                                         """))
        self.optparser.add_option(Option("--preemptible", action="store_true", dest="preemptible",
                                         help = """
                                         Specifies a preemptible lease.
                                         """))
        self.optparser.add_option(Option("--non-preemptible", action="store_false", dest="preemptible",
                                         help = """
                                         Specifies a non-preemptible lease.
                                         """))
        self.optparser.add_option(Option("-c", "--cpu", action="store", type="float", dest="cpu",
                                         help = """
                                         Percentage of CPU (must be 0 < c <= 1.0)
                                         """))
        self.optparser.add_option(Option("-m", "--mem", action="store", type="int", dest="mem",
                                         help = """
                                         Memory per node (in MB)
                                         """))
        self.optparser.add_option(Option("-i", "--vmimage", action="store", type="string", dest="vmimage",
                                         help = """
                                         Disk image identifier.
                                         """))
        self.optparser.add_option(Option("-z", "--vmimagesize", action="store", type="int", dest="vmimagesize",
                                         help = """
                                         Disk image size.
                                         """))
        
    def run(self):
        self.parse_options()
        
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
        except xmlrpclib.Fault, err:
            print >> sys.stderr, "XMLRPC fault: %s" % err.faultString
            if self.opt.debug:
                raise
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
            if self.opt.debug:
                raise

        
class haizea_cancel_lease(RPCCommand):
    """
    Cancel a lease.
    """
    
    name = "haizea-cancel-lease"
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
        
        self.optparser.add_option(Option("-l", "--lease", action="store", type="int", dest="lease",
                                         help = """
                                         ID of lease to cancel.
                                         """))
        
    def run(self):
        self.parse_options()
        
        server = self.create_rpc_proxy(self.opt.server)
        
        try:
            code = server.cancel_lease(self.opt.lease)
        except xmlrpclib.Fault, err:
            print >> sys.stderr, "XMLRPC fault: %s" % err.faultString
            if self.opt.debug:
                raise
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
            if self.opt.debug:
                raise

        
class haizea_list_leases(RPCCommand):
    """
    List all active leases in the system (i.e., not including completed or cancelled leases)
    """
    
    name = "haizea-list-leases"
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
    def run(self):
        self.parse_options()
        
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
        except xmlrpclib.Fault, err:
            print >> sys.stderr, "XMLRPC fault: %s" % err.faultString
            if self.opt.debug:
                raise
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
            if self.opt.debug:
                raise


class haizea_list_hosts(RPCCommand):
    """
    List hosts managed by Haizea
    """
    
    name = "haizea-list-hosts"
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
    def run(self):
        self.parse_options()
        
        server = self.create_rpc_proxy(self.opt.server)
        
        fields = [("id","ID", 3),
                  ("hostname","Hostname", 10),
                  ("cpu","CPUs", 6),
                  ("mem", "Mem", 6)]
        
        try:
            hosts = server.get_hosts()
            console_table_printer(fields, hosts)
        except xmlrpclib.Fault, err:
            print >> sys.stderr, "XMLRPC fault: %s" % err.faultString
            if self.opt.debug:
                raise
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
            if self.opt.debug:
                raise

class haizea_show_queue(RPCCommand):
    """
    Show the contents of the Haizea queue
    """
    
    name = "haizea-show-queue"
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
                
    def run(self):
        self.parse_options()
        
        server = self.create_rpc_proxy(self.opt.server)
        
        fields = [("id","ID", 3),
                  ("type","Type", 4),
                  ("state","State", 9),
                  ("start_sched", "Sched. Start time", 22),
                  ("duration_req", "Duration", 12),
                  ("numnodes", "Nodes", 3)]
        
        try:
            leases = server.get_queue()
            if len(leases) == 0:
                print "Queue is empty."
            else:
                console_table_printer(fields, leases)
        except xmlrpclib.Fault, err:
            print >> sys.stderr, "XMLRPC fault: %s" % err.faultString
            if self.opt.debug:
                raise
        except Exception, msg:
            print >> sys.stderr, "Error: %s" % msg
            if self.opt.debug:
                raise


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
        value = Lease.state_str[value]
    elif name == "duration_req":
        value = TimeDelta(seconds=value)
    elif name == "start_req":
        if value != None:
            value = ISO.ParseDateTime(value.value)

    return value
