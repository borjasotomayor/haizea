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
from haizea.resourcemanager.frontends import RequestFrontend
import haizea.traces.readers as tracereaders
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease 
import operator
import logging

class TracefileFrontend(RequestFrontend):
    def __init__(self, rm, starttime):
        RequestFrontend.__init__(self, rm)
        self.logger = logging.getLogger("TFILE")
        config = rm.config

        tracefile = config.get("tracefile")
        injectfile = config.get("injectionfile")
        imagefile = config.get("imagefile")
        
        # Read trace file
        # Requests is a list of lease requests
        self.logger.info("Loading tracefile %s" % tracefile)
        self.requests = None
        if tracefile.endswith(".swf"):
            self.requests = tracereaders.SWF(tracefile, config)
        elif tracefile.endswith(".lwf"):
            self.requests = tracereaders.LWF(tracefile, starttime)
    
        if injectfile != None:
            self.logger.info("Loading injection file %s" % injectfile)
            injectedleases = tracereaders.LWF(injectfile, starttime)
            self.requests += injectedleases
            self.requests.sort(key=operator.attrgetter("submit_time"))

        if imagefile != None:
            self.logger.info("Loading image file %s" % imagefile)
            imagesizes, images = tracereaders.IMG(imagefile)
            for r, i in zip(self.requests, images):
                r.vmimage = i
                r.vmimagesize = imagesizes[i]
                r.requested_resources.set_by_type(constants.RES_DISK, imagesizes[i] + r.resreq.getByType(constants.RES_MEM))
        
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
            
        num_besteffort = len([x for x in self.requests if isinstance(x,BestEffortLease)])
        num_ar = len([x for x in self.requests if isinstance(x,ARLease)])
        self.logger.info("Loaded workload with %i requests (%i best-effort + %i AR)" % (num_besteffort+num_ar, num_besteffort, num_ar))
        
        
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