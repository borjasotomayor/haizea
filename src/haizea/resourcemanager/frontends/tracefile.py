import haizea.common.constants as constants
from haizea.resourcemanager.frontends.base import RequestFrontend
import haizea.traces.readers as tracereaders
from haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
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
        if tracefile.endswith(".csv"):
            self.requests = tracereaders.CSV(tracefile, config)
        elif tracefile.endswith(".swf"):
            self.requests = tracereaders.SWF(tracefile, config)
        elif tracefile.endswith(".gwf"):
            self.requests = tracereaders.GWF(tracefile, config)
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
                elif isinstance(r,ExactLease):
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