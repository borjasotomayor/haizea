import workspace.haizea.datastruct as ds
from workspace.haizea.slottable import SlotTable
from workspace.haizea.log import info, debug, warning
import workspace.haizea.constants as constants

class SchedException(Exception):
    pass

class SlotFittingException(Exception):
    pass

class Scheduler(object):
    def __init__(self, rm):
        self.rm = rm
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        
    def schedule(self, requests):
        starting = [l for l in self.scheduledleases.entries.values() if l.hasStartingReservations(self.rm.time)]
        ending = [l for l in self.scheduledleases.entries.values() if l.hasEndingReservations(self.rm.time)]
        
        for l in ending:
            rrs = l.getEndingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    pass
                elif isinstance(rr,ds.VMResourceReservation):
                    info("LEASE-%i Something ended" % l.leaseID, constants.SCHED, self.rm.time)
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    pass
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    pass
            # Temporary:
            self.scheduledleases.remove(l)
        
        for l in starting:
            rrs = l.getStartingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    pass
                elif isinstance(rr,ds.VMResourceReservation):
                    info("LEASE-%i Something started" % l.leaseID, constants.SCHED, self.rm.time)
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    pass
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    pass
        
        for r in requests:
            info("LEASE-%i Processing request" % r.leaseID, constants.SCHED, self.rm.time)
            info("LEASE-%i Start   %s" % (r.leaseID, r.start), constants.SCHED, self.rm.time)
            info("LEASE-%i End     %s" % (r.leaseID, r.end), constants.SCHED, self.rm.time)
            info("LEASE-%i RealEnd %s" % (r.leaseID, r.prematureend), constants.SCHED, self.rm.time)
            info("LEASE-%i ResReq  %s" % (r.leaseID, r.resreq), constants.SCHED, self.rm.time)
            try:
                self.scheduleExactLease(r)
                self.scheduledleases.add(r)
            except SchedException, msg:
                warning("Scheduling exception: %s" % msg, constants.SCHED, self.rm.time)
               
                
        # TODO Process queue
        
        # TODO return (accepted, rejected) leases
        pass
    
    
    def scheduleExactLease(self, req):
        try:
            pass
            (mappings, preemptions, transfers) = self.slottable.fitExact(req.leaseID, req.start, req.end, req.vmimage, req.resreq, prematureend=req.prematureend, preemptible=False, canpreempt=True)
            if len(preemptions) > 0:
                info("Must preempt the following: %s" % preemptions, constants.SCHED, self.rm.time)
                self.preemptResources(mustpreempt, startTime, endTime)
            
            # Schedule image transfers
            
            # Add resource reservations
            vmrr = ds.VMResourceReservation(req.start, req.end, mappings, constants.ONCOMPLETE_ENDLEASE)
            req.appendRR(vmrr)
            
            self.slottable.commit()
        except SlotFittingException, msg:
            pass
             # TODO Rollback

    
    def existsScheduledLeases(self):
        return not self.scheduledleases.isEmpty()
    
    def isQueueEmpty(self):
        return self.queue.isEmpty()