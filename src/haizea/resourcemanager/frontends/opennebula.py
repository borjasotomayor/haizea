import haizea.common.constants as constants
from haizea.resourcemanager.frontends.base import RequestFrontend
import haizea.traces.readers as tracereaders
from haizea.resourcemanager.datastruct import ExactLease, BestEffortLease, ResourceTuple
import operator
from haizea.common.utils import UNIX2DateTime
from pysqlite2 import dbapi2 as sqlite
from mx.DateTime import TimeDelta, ISO
from haizea.common.utils import roundDateTime

RES_CPU="CPU"
RES_MEM="MEMORY"
RES_DISK="DISK"
DISK_IMAGE="IMAGE"

class OpenNebulaFrontend(RequestFrontend):
    def __init__(self, rm):
        self.rm = rm
        self.processed = []
        self.logger = self.rm.logger
        config = self.rm.config

        self.conn = sqlite.connect(config.getONEDB())
        self.conn.row_factory = sqlite.Row
        
    def getAccumulatedRequests(self):
        cur = self.conn.cursor()
        processed = ",".join([`p` for p in self.processed])
        cur.execute("select * from vmpool where state=1 and oid not in (%s)" % processed)
        openNebulaReqs = cur.fetchall()
        requests = []
        for req in openNebulaReqs:
            cur.execute("select * from vm_template where id=%i" % req["oid"])
            template = cur.fetchall()
            attrs = dict([(r["name"],r["value"]) for r in template])
            self.processed.append(req["oid"])
            requests.append(self.ONEreq2lease(req, attrs))
        return requests

    def existsMoreRequests(self):
        return True
    
    def ONEreq2lease(self, req, attrs):
        if attrs.has_key("HAIZEA_START"):
            return self.createExactLease(req, attrs)
        else:
            return self.createBestEffortLease(req, attrs)
    
    def getCommonAttrs(self,req, attrs):
        disk = attrs[RES_DISK]
        diskattrs = dict([n.split("=") for n in disk.split(",")])
        tSubmit = UNIX2DateTime(req["stime"])
        vmimage = diskattrs[DISK_IMAGE]
        vmimagesize = 0
        numnodes = 1
        resreq = ResourceTuple.createEmpty()
        resreq.setByType(constants.RES_CPU, float(attrs[RES_CPU]))
        resreq.setByType(constants.RES_MEM, int(attrs[RES_MEM]))
        return tSubmit, vmimage, vmimagesize, numnodes, resreq
    
    def createBestEffortLease(self, req, attrs):
        tSubmit, vmimage, vmimagesize, numnodes, resreq = self.getCommonAttrs(req, attrs)
        if attrs.has_key("HAIZEA_DURATION"):
            duration = ISO.ParseTime(attrs["HAIZEA_DURATION"])
        else:
            # TODO: Just for testing. Should be unlimited duration.
            duration = TimeDelta(seconds=60) 
        leasereq = BestEffortLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq)
        leasereq.state = constants.LEASE_STATE_PENDING
        # Enactment info should be changed to the "array id" when groups
        # are implemented in OpenNebula
        leasereq.enactmentInfo = int(req["oid"])
        # Only one node for now
        leasereq.vnodeEnactmentInfo = {}
        leasereq.vnodeEnactmentInfo[1] = int(req["oid"])
        leasereq.setScheduler(self.rm.scheduler)
        return leasereq
    
    def createExactLease(self, req, attrs):
        tSubmit, vmimage, vmimagesize, numnodes, resreq = self.getCommonAttrs(req, attrs)
        duration = ISO.ParseTime(attrs["HAIZEA_DURATION"])
        tStart = attrs["HAIZEA_START"]
        if tStart[0] == "+":
            # Relative time
            # For testing, should be:
            # tStart = tSubmit + ISO.ParseTime(tStart[1:])
            tStart = roundDateTime(self.rm.clock.getTime() + ISO.ParseTime(tStart[1:]))
        else:
            tStart = ISO.ParseDateTime(tStart)
        leasereq = ExactLease(tSubmit, tStart, duration, vmimage, vmimagesize, numnodes, resreq)
        leasereq.state = constants.LEASE_STATE_PENDING
        # Enactment info should be changed to the "array id" when groups
        # are implemented in OpenNebula
        leasereq.enactmentInfo = int(req["oid"])
        # Only one node for now
        leasereq.vnodeEnactmentInfo = {}
        leasereq.vnodeEnactmentInfo[1] = int(req["oid"])
        leasereq.setScheduler(self.rm.scheduler)
        return leasereq
        