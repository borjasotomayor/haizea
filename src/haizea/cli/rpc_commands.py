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
import xmlrpclib
import sys

def add_rpc_options(op):
    op.add_option(Option("-s", "--server", action="store", type="string", dest="server", default="http://localhost:42493"))

def create_rpc_proxy(server):
    return xmlrpclib.ServerProxy(server, allow_none=True)

def haizea_request_lease(argv):
    p = OptionParser()
    add_rpc_options(p)
    
    p.add_option(Option("-t", "--start", action="store", type="string", dest="start"))
    p.add_option(Option("-d", "--duration", action="store", type="string", dest="duration"))
    p.add_option(Option("-n", "--numnodes", action="store", type="int", dest="numnodes"))
    p.add_option(Option("--preemptible", action="store_true", dest="preemptible"))
    p.add_option(Option("--non-preemptible", action="store_false", dest="preemptible"))
    p.add_option(Option("-c", "--cpu", action="store", type="float", dest="cpu"))
    p.add_option(Option("-m", "--mem", action="store", type="int", dest="mem"))
    p.add_option(Option("-i", "--vmimage", action="store", type="string", dest="vmimage"))
    p.add_option(Option("-z", "--vmimagesize", action="store", type="int", dest="vmimagesize"))

    opt, args = p.parse_args(argv)

    if opt.preemptible == None:
        preemptible = False
    else:
        preemptible = opt.preemptible
        
    server = create_rpc_proxy(opt.server)
    
    try:
        lease_id = server.create_lease(opt.start, opt.duration, preemptible, opt.numnodes, 
                            opt.cpu, opt.mem, opt.vmimage, opt.vmimagesize)
        print "Lease submitted correctly."
        print "Lease ID: %i" % lease_id
    except Exception, msg:
        print >> sys.stderr, "Error: %s" % msg
        

def haizea_cancel_lease(argv):
    p = OptionParser()
    add_rpc_options(p)
    
    p.add_option(Option("-l", "--lease", action="store", type="int", dest="lease"))

    opt, args = p.parse_args(argv)

    server = create_rpc_proxy(opt.server)
    
    try:
        code = server.cancel_lease(opt.lease)
    except Exception, msg:
        print >> sys.stderr, "Error: %s" % msg
        

def haizea_list_leases(argv):
    p = OptionParser()
    add_rpc_options(p)
    
    opt, args = p.parse_args(argv)

    server = create_rpc_proxy(opt.server)
    
    # TODO: Make this configurable. Ideally, use some sort of
    # "console table printer"
    fields = [("id",3),
              ("type",4),
              ("state",7),
              ("start_req",19),
              ("duration_req",9),
              ("numnodes",3)]
    
    try:
        leases = server.get_leases()
        print "\33[1m\33[4m",
        for (name,width) in fields:
            width = max(len(name),width) + 1
            centered = name.ljust(width)
            print centered,
        print "\33[0m"
        for l in leases:
            for (name,width) in fields:
                value = l[name]
                width = max(len(name),width)
                print " %s" % str(value).ljust(width),
            print
    except Exception, msg:
        print >> sys.stderr, "Error: %s" % msg