
class LeaseBase(object):
    def __init__(self, tSubmit, vmimage, vmimagesize, resreq):
        self.leaseID = None
        self.tSubmit = tSubmit
        self.state = None
        self.vmimage = vmimage
        self.vmimagesize = vmimagesize
        self.resreq = resreq
        self.vmimagemap = {}
        self.memimagemap = {}
        self.rr = []
        
class ExactLease(LeaseBase):
    def __init__(self, tSubmit, start, end, vmimage, vmimagesize, resreq):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, resreq)
        self.start = start
        self.end = end
        
class BestEffortLease(LeaseBase):
    def __init__(self, tSubmit, maxdur, vmimage, vmimagesize, resreq):
        LeaseBase.__init__(self, tSubmit, vmimage, vmimagesize, resreq)
        self.maxdur = maxdur
        self.remdur = maxdur
        
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
        
class LeaseTable(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.entries = {}
        
    def getLease(self, leaseID):
        return entries[leaseID]
    
class SlotTable(object):
    def __init__(self, scheduler):
        # self.db = ...
        self.scheduler = scheduler
        
    def getNextChangePoint(self, time):
        pass
