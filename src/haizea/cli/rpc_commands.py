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
import haizea.common.defaults as defaults
import haizea.common.constants as constants
from haizea.core.leases import Lease, Capacity, Duration, Timestamp, DiskImageSoftwareEnvironment
from haizea.common.utils import round_datetime
from haizea.cli.optionparser import OptionParser, Option
from haizea.cli import Command
import xmlrpclib
import sys
from mx.DateTime import TimeDelta, ISO, now
import xml.etree.ElementTree as ET

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

    START_NOW = "now"
    START_BESTEFFORT = "best_effort"
    DURATION_UNLIMITED = "unlimited"    
    
    def __init__(self, argv):
        RPCCommand.__init__(self, argv)
        
        self.optparser.add_option(Option("-f", "--file", action="store", type="string", dest="file",
                                         help = """
                                         File containing a lease description in XML.
                                         """))
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
        self.optparser.add_option(Option("-c", "--cpu", action="store", type="int", dest="cpu",
                                         help = """
                                         Percentage of CPU (must be 0 < c <= 100)
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
        
        if self.opt.file != None:
            lease_elem = ET.parse(self.opt.file).getroot()
            # If a relative starting time is used, replace for an
            # absolute starting time.
            exact = lease.find("start/exact")
            if exact != None:
                exact_time = exact.get("time")
                exact.set("time", str(self.__absolute_time(exact_time)))            
            lease_xml_str = ET.tostring(lease_elem)
        else:
            if self.opt.preemptible == None:
                preemptible = False
            else:
                preemptible = self.opt.preemptible
            
            capacity = Capacity([constants.RES_CPU, constants.RES_MEM])
            capacity.set_quantity(constants.RES_CPU, int(self.opt.cpu) * 100)
            capacity.set_quantity(constants.RES_MEM, int(self.opt.mem))    
            requested_resources = dict([(i+1, capacity) for i in range(self.opt.numnodes)])    
            if self.opt.duration == haizea_request_lease.DURATION_UNLIMITED:
                # This is an interim solution (make it run for a century).
                # TODO: Integrate concept of unlimited duration in the lease datastruct
                duration = DateTimeDelta(36500)
            else:
                duration = ISO.ParseTimeDelta(self.opt.duration)
    
            if self.opt.start == haizea_request_lease.START_NOW:
                lease = Lease(id = None,
                              submit_time = None,
                              requested_resources = requested_resources, 
                              start = Timestamp(Timestamp.NOW),
                              duration = Duration(duration),
                              deadline = None, 
                              preemptible=preemptible,
                              software = DiskImageSoftwareEnvironment(self.opt.vmimage, self.opt.vmimagesize),
                              state = None
                              )
            elif self.opt.start == haizea_request_lease.START_BESTEFFORT:
                lease = Lease(id = None,
                              submit_time = None,
                              requested_resources = requested_resources, 
                              start = Timestamp(Timestamp.UNSPECIFIED),
                              duration = Duration(duration),
                              deadline = None, 
                              preemptible=preemptible,
                              software = DiskImageSoftwareEnvironment(self.opt.vmimage, self.opt.vmimagesize),
                              state = None
                              )
            else:
                start = self.__absolute_time(self.opt.start)
                lease = Lease(id = None,
                              submit_time = None,
                              requested_resources = requested_resources, 
                              start = Timestamp(start),
                              duration = Duration(duration),
                              deadline = None, 
                              preemptible=preemptible,
                              software = DiskImageSoftwareEnvironment(self.opt.vmimage, self.opt.vmimagesize),
                              state = None
                              )

            lease_xml_str = ET.tostring(lease.to_xml())

        server = self.create_rpc_proxy(self.opt.server)
        
        try:
            lease_id = server.create_lease(lease_xml_str)
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

    def __absolute_time(self, time_str):
        if time_str[0] == "+":
            # Relative time
            time = round_datetime(now() + ISO.ParseTime(time_str[1:]))
        else:
            time = Parser.ParseDateTime(time_str)
            
        return time
        
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
                  ("type","Type", 12),
                  ("state","State", 9),
                  ("start_req", "Starting time", 22),
                  ("duration_req", "Duration", 12),
                  ("numnodes", "Nodes", 3)]
        
        try:
            leases = server.get_leases()
            leases_fields = []
            for lease_xml in leases:
                lease = Lease.from_xml_string(lease_xml)
                lease_fields = {}
                lease_fields["id"] = lease.id
                lease_fields["type"] = Lease.type_str[lease.get_type()]
                lease_fields["state"] = Lease.state_str[lease.get_state()]
                lease_fields["start_req"] = lease.start.requested
                lease_fields["duration_req"] = lease.duration.requested
                lease_fields["numnodes"] = len(lease.requested_resources)
                leases_fields.append(lease_fields)
            console_table_printer(fields, leases_fields)
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
                  ("type","Type", 12),
                  ("state","State", 9),
                  ("start_req", "Starting time", 22),
                  ("duration_req", "Duration", 12),
                  ("numnodes", "Nodes", 3)]
        
        try:
            leases = server.get_queue()
            if len(leases) == 0:
                print "Queue is empty."
            else:
                leases_fields = []
                for lease_xml in leases:
                    lease = Lease.from_xml_string(lease_xml)
                    lease_fields = {}
                    lease_fields["id"] = lease.id
                    lease_fields["type"] = Lease.type_str[lease.get_type()]
                    lease_fields["state"] = Lease.state_str[lease.get_state()]
                    lease_fields["start_req"] = lease.start.requested
                    lease_fields["duration_req"] = lease.duration.requested
                    lease_fields["numnodes"] = len(lease.requested_resources)
                    leases_fields.append(lease_fields)
                console_table_printer(fields, leases_fields)                
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
            width = max(len(name),width)
            print " %s" % str(v[name]).ljust(width),
        print
