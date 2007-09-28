from workspace.haizea.constants import res_str, state_str, rstate_str, DS
from workspace.haizea.log import edebug
leaseID = 1

def getLeaseID():
    global leaseID
    l = leaseID
    leaseID += 1
    return l

def prettyNodemap(nodes):
    pnodes = list(set(nodes.values()))
    normmap = [([y[0] for y in nodes.items() if y[1]==x], x) for x in pnodes]
    for m in normmap: m[0].sort()
    s = "   ".join([", ".join(["V"+`y` for y in x[0]])+" -> P" + `x[1]` for x in normmap])
    return s

class LeaseBase(object):
    def __init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq):
        self.leaseID = getLeaseID()
        self.tSubmit = tSubmit
        self.state = None
        self.vmimage = vmimage
        self.vmimagesize = vmimagesize
        self.numnodes = numnodes
        self.resreq = resreq
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
    def printContents(self):
        edebug("Lease ID       : %i" % self.leaseID, DS, None)
        edebug("Submission time: %s" % self.tSubmit, DS, None)
        edebug("State          : %s" % state_str(self.state), DS, None)
        edebug("VM image       : %s" % self.vmimage, DS, None)
        edebug("VM image size  : %s" % self.vmimagesize, DS, None)
        edebug("Num nodes      : %s" % self.numnodes, DS, None)
        edebug("Resource req   : %s" % "  ".join([res_str(r[0]) + ": " + `r[1]` for r in self.resreq.items()]), DS, None)
        edebug("VM image map   : %s" % self.vmimagemap, DS, None)
        edebug("Mem image map  : %s" % self.memimagemap, DS, None)

    def printRR(self):
        edebug("RESOURCE RESERVATIONS", DS, None)
        edebug("~~~~~~~~~~~~~~~~~~~~~", DS, None)
        for r in self.rr:
            r.printContents()
            edebug("##", DS, None)
            
    def appendRR(self, rr):
        self.rr.append(rr)
        
    def hasStartingReservations(self, time):
        return len(self.getStartingReservations(time)) > 0

    def hasEndingReservations(self, time):
        return len(self.getEndingReservations(time)) > 0

    def getStartingReservations(self, time):
        return [r for r in self.rr if r.start == time]

    def getEndingReservations(self, time):
        return [r for r in self.rr if r.end == time]
        
class ExactLease(LeaseBase):
    def __init__(self, tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, prematureend = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.start = start
        self.end = end
        self.prematureend = prematureend
        
    def printContents(self):
        edebug("__________________________________________________", DS, None)
        LeaseBase.printContents(self)
        edebug("Type           : EXACT", DS, None)
        edebug("Start time     : %s" % self.start, DS, None)
        edebug("End time       : %s" % self.end, DS, None)
        edebug("Early end time : %s" % self.prematureend, DS, None)
        self.printRR()
        edebug("--------------------------------------------------", DS, None)
        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, numnodes, resreq)
        self.maxdur = maxdur
        self.remdur = maxdur
        self.realdur = realdur

    def printContents(self):
        edebug("__________________________________________________", DS, None)
        LeaseBase.printContents(self)
        edebug("Type           : BEST-EFFORT", DS, None)
        edebug("Max duration   : %s" % self.maxdur, DS, None)
        edebug("Rem duration   : %s" % self.remdur, DS, None)
        edebug("Real duration  : %s" % self.realdur, DS, None)
        self.printRR()
        edebug("--------------------------------------------------", DS, None)
        
class ResourceReservationBase(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.state = None
        
    def printContents(self):
        edebug("Start          : %s" % self.start, DS, None)
        edebug("End            : %s" % self.end, DS, None)
        edebug("State          : %s" % rstate_str(self.state), DS, None)
        
class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, transfers):
        ResourceReservationBase.__init__(self, start, end)
        self.transfers = transfers

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : FILE TRANSFER", DS, None)
        edebug("Transfers      : %s" % self.transfers, DS, None)

                
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes
        self.oncomplete = oncomplete

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : VM", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        edebug("On Complete    : %s" % self.oncomplete, DS, None)
        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : SUSPEND", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes

    def printContents(self):
        ResourceReservationBase.printContents(self)
        edebug("Type           : RESUME", DS, None)
        edebug("Nodes          : %s" % prettyNodemap(self.nodes), DS, None)
        
class Queue(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.q = []
        
    def isEmpty(self):
        return len(self.q)==0
    
        
class LeaseTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.entries = {}
        
    def getLease(self, leaseID):
        return entries[leaseID]
    
    def isEmpty(self):
        return len(self.entries)==0
    
    def remove(self, lease):
        del self.entries[lease.leaseID]
        
    def add(self, lease):
        self.entries[lease.leaseID] = lease
    
    

