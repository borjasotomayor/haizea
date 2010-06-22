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

import haizea.common.constants as constants
from haizea.common.utils import get_clock
from haizea.core.frontends import RequestFrontend
from haizea.core.leases import LeaseWorkload, Lease, DiskImageSoftwareEnvironment, LeaseAnnotations
import operator
import logging

class TracefileFrontend(RequestFrontend):
    def __init__(self, starttime):
        RequestFrontend.__init__(self)
        self.logger = logging.getLogger("TFILE")
        self.starttime = starttime

    def load(self, manager):
        config = manager.config
        
        tracefile = config.get("tracefile")
        injectfile = config.get("injectionfile")
        annotationfile = config.get("annotationfile")
        
        # Read trace file
        # Requests is a list of lease requests
        self.logger.info("Loading tracefile %s" % tracefile)
        self.requests = None
        lease_workload = LeaseWorkload.from_xml_file(tracefile, self.starttime)
        self.requests = lease_workload.get_leases()
    
        if injectfile != None:
            self.logger.info("Loading injection file %s" % injectfile)
            inj_lease_workload = LeaseWorkload.from_xml_file(injectfile, self.starttime)
            inj_leases = inj_lease_workload.get_leases()
            self.requests += inj_leases
            self.requests.sort(key=operator.attrgetter("submit_time"))

        if annotationfile != None:
            self.logger.info("Loading annotation file %s" % annotationfile)
            annotations = LeaseAnnotations.from_xml_file(annotationfile)
            annotations.apply_to_leases(self.requests)
            
        # Add runtime overhead, if necessary
        add_overhead = config.get("add-overhead")
        
        if add_overhead != constants.RUNTIMEOVERHEAD_NONE:
            slowdown_overhead = config.get("runtime-slowdown-overhead")
            boot_overhead = config.get("bootshutdown-overhead")
            for r in self.requests:
                if add_overhead == constants.RUNTIMEOVERHEAD_ALL or (add_overhead == constants.RUNTIMEOVERHEAD_BE and r.get_type() == Lease.BEST_EFFORT):
                    if slowdown_overhead != 0:
                        r.add_runtime_overhead(slowdown_overhead)
                    r.add_boot_overhead(boot_overhead)

        # Override requested memory, if necessary
        memory = config.get("override-memory")
        if memory != constants.NO_MEMORY_OVERRIDE:
            for r in self.requests:
                for n in r.requested_resources:
                    r.requested_resources[n].set_quantity(constants.RES_MEM, memory)            
            
        types = {}
        for r in self.requests:
            types[r.get_type()] = types.setdefault(r.get_type(), 0) + 1
        types_str = " + ".join(["%i %s" % (types[t],Lease.type_str[t]) for t in types])

        self.logger.info("Loaded workload with %i requests (%s)" % (len(self.requests), types_str))
        
        
    def get_accumulated_requests(self):
        # When reading from a trace file, there are no
        # "accumulated requests". Rather, we just take whatever
        # requests are in the trace up to the current time
        # reported by the resource manager
        time = get_clock().get_time()
        nowreq = [r for r in self.requests if r.submit_time <= time]
        self.requests = [r for r in self.requests if r.submit_time > time]   
        return nowreq              

    def exists_more_requests(self):
        return len(self.requests) != 0

    def get_next_request_time(self):
        if self.exists_more_requests():
            return self.requests[0].submit_time
        else:
            return None