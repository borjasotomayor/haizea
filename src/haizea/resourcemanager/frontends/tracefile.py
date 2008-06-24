# -------------------------------------------------------------------------- #
# Copyright 2006-2008, Borja Sotomayor                                       #
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
from haizea.resourcemanager.frontends.base import RequestFrontend
import haizea.traces.readers as tracereaders
from haizea.resourcemanager.datastruct import ARLease, BestEffortLease 
import operator


class TracefileFrontend(RequestFrontend):
    def __init__(self, rm, starttime):
        RequestFrontend.__init__(self, rm)
        
        config = rm.config

        tracefile = config.getTracefile()
        injectfile = config.getInjectfile()
        imagefile = config.getImagefile()
        
        # Read trace file
        # Requests is a list of lease requests
        self.requests = None
        if tracefile.endswith(".swf"):
            self.requests = tracereaders.SWF(tracefile, config)
        elif tracefile.endswith(".lwf"):
            self.requests = tracereaders.LWF(tracefile, starttime)
    
        if injectfile != None:
            injectedleases = tracereaders.LWF(injectfile, starttime)
            self.requests += injectedleases
            self.requests.sort(key=operator.attrgetter("tSubmit"))

        if imagefile != None:
            imagesizes, images = tracereaders.IMG(imagefile)
            for r,i in zip(self.requests,images):
                r.vmimage = i
                r.vmimagesize = imagesizes[i]
                r.resreq.setByType(constants.RES_DISK, imagesizes[i] + r.resreq.getByType(constants.RES_MEM))
        
        # Add runtime overhead, if necessary
        overhead = config.getRuntimeOverhead()
        if overhead != None:
            for r in self.requests:
                if isinstance(r,BestEffortLease):
                    r.addRuntimeOverhead(overhead)
                elif isinstance(r,ARLease):
                    if not config.overheadOnlyBestEffort():
                        r.addRuntimeOverhead(overhead)

        # Add boot + shutdown overhead
        overhead = config.getBootOverhead()
        for r in self.requests:
            r.addBootOverhead(overhead)

        # Make the scheduler reachable from the lease request
        for r in self.requests:
            r.setScheduler(rm.scheduler)
        
        
    def getAccumulatedRequests(self):
        # When reading from a trace file, there are no
        # "accumulated requests". Rather, we just take whatever
        # requests are in the trace up to the current time
        # reported by the resource manager
        time = self.rm.clock.getTime()
        nowreq = [r for r in self.requests if r.tSubmit <= time]
        self.requests = [r for r in self.requests if r.tSubmit > time]   
        return nowreq              

    def existsPendingReq(self):
        return len(self.requests) != 0

    def getNextReqTime(self):
        if self.existsPendingReq():
            return self.requests[0].tSubmit
        else:
            return None