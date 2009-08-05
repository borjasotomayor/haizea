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

import haizea.common.constants as constants
from haizea.common.utils import get_clock
from haizea.core.frontends import RequestFrontend
from haizea.core.leases import LeaseWorkload, Lease
import operator
import logging

class TracefileFrontend(RequestFrontend):
    def __init__(self, manager, starttime):
        RequestFrontend.__init__(self, manager)
        self.logger = logging.getLogger("TFILE")
        config = manager.config

        tracefile = config.get("tracefile")
        injectfile = config.get("injectionfile")
        imagefile = config.get("imagefile")
        
        # Read trace file
        # Requests is a list of lease requests
        self.logger.info("Loading tracefile %s" % tracefile)
        self.requests = None
        if tracefile.endswith(".swf"):
            self.requests = LeaseWorkload.from_swf_file(tracefile, starttime)
        elif tracefile.endswith(".lwf") or tracefile.endswith(".xml"):
            lease_workload = LeaseWorkload.from_xml_file(tracefile, starttime)
            self.requests = lease_workload.get_leases()
    
        if injectfile != None:
            self.logger.info("Loading injection file %s" % injectfile)
            inj_lease_workload = LeaseWorkload.from_xml_file(injectfile, starttime)
            inj_leases = inj_lease_workload.get_leases()
            self.requests += inj_leases
            self.requests.sort(key=operator.attrgetter("submit_time"))

        if imagefile != None:
            self.logger.info("Loading image file %s" % imagefile)
            file = open (imgfile, "r")
            imagesizes = {}
            images = []
            state = 0  # 0 -> Reading image sizes  1 -> Reading image sequence
            for line in file:
                if line[0]=='#':
                    state = 1
                elif state == 0:
                    image, size = line.split()
                    imagesizes[image] = int(size)
                elif state == 1:
                    images.append(line.strip())            
            for lease, image_id in zip(self.requests, images):
                lease.software = DiskImageSoftwareEnvironment(image_id, imagesizes[image_id])
        
        # Add runtime overhead, if necessary
        add_overhead = config.get("add-overhead")
        
        if add_overhead != constants.RUNTIMEOVERHEAD_NONE:
            slowdown_overhead = config.get("runtime-slowdown-overhead")
            boot_overhead = config.get("bootshutdown-overhead")
            for r in self.requests:
                if add_overhead == constants.RUNTIMEOVERHEAD_ALL or (add_overhead == constants.RUNTIMEOVERHEAD_BE and isinstance(r,BestEffortLease)):
                   if slowdown_overhead != 0:
                       r.add_runtime_overhead(slowdown_overhead)
                   r.add_boot_overhead(boot_overhead)

        # Override requested memory, if necessary
        memory = config.get("override-memory")
        if memory != constants.NO_MEMORY_OVERRIDE:
            for r in self.requests:
                r.requested_resources.set_by_type(constants.RES_MEM, memory)            
            
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