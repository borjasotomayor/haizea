import workspace.haizea.datastruct as ds
from workspace.haizea.slottable import SlotTable, SlotFittingException
from workspace.haizea.log import info, debug, warning
import workspace.haizea.constants as constants

class SchedException(Exception):
    pass

class Scheduler(object):
    def __init__(self, rm):
        self.rm = rm
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        self.rejectedleases = ds.LeaseTable(self)
        
    def schedule(self, requests):
        self.processReservations()
        
        # Process exact requests
        for r in requests:
            info("LEASE-%i Processing request" % r.leaseID, constants.SCHED, self.rm.time)
            info("LEASE-%i Start   %s" % (r.leaseID, r.start), constants.SCHED, self.rm.time)
            info("LEASE-%i End     %s" % (r.leaseID, r.end), constants.SCHED, self.rm.time)
            info("LEASE-%i RealEnd %s" % (r.leaseID, r.prematureend), constants.SCHED, self.rm.time)
            info("LEASE-%i ResReq  %s" % (r.leaseID, r.resreq), constants.SCHED, self.rm.time)
            try:
                self.scheduleExactLease(r)
                self.scheduledleases.add(r)
                self.rm.stats.incrAccepted()
            except SchedException, msg:
                self.rm.stats.incrRejected()
                warning("LEASE-%i Scheduling exception: %s" % (r.leaseID, msg), constants.SCHED, self.rm.time)
               
                
        # TODO Process queue
        
    
    def processReservations(self):
        starting = [l for l in self.scheduledleases.entries.values() if l.hasStartingReservations(self.rm.time)]
        ending = [l for l in self.scheduledleases.entries.values() if l.hasEndingReservations(self.rm.time)]
        
        for l in ending:
            rrs = l.getEndingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    pass
                elif isinstance(rr,ds.VMResourceReservation):
                    self.handleEndVM(l, rr)
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    pass
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    pass
        
        for l in starting:
            rrs = l.getStartingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    pass
                elif isinstance(rr,ds.VMResourceReservation):
                    self.handleStartVM(l, rr)                    
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    pass
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    pass

    def handleStartVM(self, l, rr):
        info("LEASE-%i Start of handleStartVM" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        if l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            # TODO: Enactment
        elif l.state == constants.LEASE_STATE_SUSPENDED:
            pass
        l.printContents()
        debug("LEASE-%i End of handleStartVM" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndVM(self, l, rr):
        info("LEASE-%i Start of handleEndVM" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        if rr.oncomplete == constants.ONCOMPLETE_ENDLEASE:
            l.state = constants.LEASE_STATE_DONE
            rr.state = constants.RES_STATE_DONE
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            
        elif rr.oncomplete == constants.ONCOMPLETE_SUSPEND:
            pass
        l.printContents()
        debug("LEASE-%i End of handleEndVM" % l.leaseID, constants.SCHED, self.rm.time)
        
    
    def scheduleExactLease(self, req):
        try:
            (mappings, preemptions, transfers) = self.slottable.fitExact(req.leaseID, req.start, req.end, req.vmimage, req.numnodes, req.resreq, prematureend=req.prematureend, preemptible=False, canpreempt=True)
            if len(preemptions) > 0:
                info("Must preempt the following: %s" % preemptions, constants.SCHED, self.rm.time)
                self.preemptResources(mustpreempt, startTime, endTime)
            
            # Schedule image transfers
            dotransfer = False
            
            if dotransfer:
                req.state = constants.LEASE_STATE_SCHEDULED
            else:
                req.state = constants.LEASE_STATE_DEPLOYED            
            
            
            # Add resource reservations
            vmrr = ds.VMResourceReservation(req.start, req.end, mappings, constants.ONCOMPLETE_ENDLEASE)
            vmrr.state = constants.RES_STATE_SCHEDULED
            req.appendRR(vmrr)
            
            self.slottable.commit()
        except SlotFittingException, msg:
            self.slottable.rollback()
            raise SchedException, "The requested exact lease is infeasible"

    
    def existsScheduledLeases(self):
        return not self.scheduledleases.isEmpty()
    
    def isQueueEmpty(self):
        return self.queue.isEmpty()