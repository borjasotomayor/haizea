leaseID = 1

def getLeaseID():
    global leaseID
    l = leaseID
    leaseID += 1
    return l

class LeaseBase(object):
    def __init__(self, tSubmit, vmimage, vmimagesize, resreq):
        self.leaseID = getLeaseID()
        self.tSubmit = tSubmit
        self.state = None
        self.vmimage = vmimage
        self.vmimagesize = vmimagesize
        self.resreq = resreq
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
    def printContents(self):
        print "Lease ID       : %i" % self.leaseID
        print "Submission time: %s" % self.tSubmit
        print "State          : %s" % self.state
        print "VM image       : %s" % self.vmimage
        print "VM image size  : %s" % self.vmimagesize
        print "Resource req   : %s" % self.resreq
        print "VM image map   : %s" % self.vmimagemap
        print "Mem image map  : %s" % self.memimagemap

    def printRR(self):
        print "RESOURCE RESERVATIONS"
        print "~~~~~~~~~~~~~~~~~~~~~"
        for r in rr:
            print r
            
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
    def __init__(self, tSubmit, start, end, vmimage, vmimagesize, resreq, prematureend = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, resreq)
        self.start = start
        self.end = end
        self.prematureend = prematureend
        
    def printContents(self):
        print "__________________________________________________"
        LeaseBase.printContents(self)
        print "Type           : EXACT"
        print "Start time     : %s" % self.start
        print "End time       : %s" % self.end
        print "Early end time : %s" % self.prematureend
        print "--------------------------------------------------"
        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, maxdur, vmimage, vmimagesize, resreq, realdur = None):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, resreq)
        self.maxdur = maxdur
        self.remdur = maxdur
        self.realdur = realdur

    def printContents(self):
        print "__________________________________________________"
        LeaseBase.printContents(self)
        print "Type           : BEST-EFFORT"
        print "Max duration   : %s" % self.maxdur
        print "Rem duration   : %s" % self.remdur
        print "Real duration  : %s" % self.realdur
        print "--------------------------------------------------"
        
class ResourceReservationBase(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
        
class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, type, transfers):
        ResourceReservationBase.__init__(self, start, end)
        self.type = type
        self.transfers = transfers
        
class VMResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes
        self.oncomplete = oncomplete
        
class SuspensionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes
        
class ResumptionResourceReservation(ResourceReservationBase):
    def __init__(self, start, end, nodes, oncomplete):
        ResourceReservationBase.__init__(self, start, end)
        self.nodes = nodes
        
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
    
    

