import workspace.haizea.resourcemanager.datastruct as ds
from workspace.haizea.resourcemanager.slottable import SlotTable, SlotFittingException
from workspace.haizea.common.log import info, debug, warning, edebug
import workspace.haizea.common.constants as constants


class SchedException(Exception):
    pass

class CancelException(Exception):
    pass

class Scheduler(object):
    def __init__(self, rm):
        self.rm = rm
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        self.rejectedleases = ds.LeaseTable(self)
        self.maxres = self.rm.config.getMaxReservations()
        self.numbesteffortres = 0
        self.endcliplease = None
        
    def schedule(self, requests):
        # Cancel best-effort requests that have to be cancelled
        cancelled = self.queue.purgeCancelled()
        if len(cancelled) > 0:
            info("Cancelled leases %s" % cancelled, constants.SCHED, self.rm.time)
            for leaseID in cancelled:
                self.rm.stats.decrQueueSize(leaseID)
        
        self.processReservations()
        
        # Process exact requests
        for r in requests:
            info("LEASE-%i Processing request (EXACT)" % r.leaseID, constants.SCHED, self.rm.time)
            info("LEASE-%i Start   %s" % (r.leaseID, r.start), constants.SCHED, self.rm.time)
            info("LEASE-%i End     %s" % (r.leaseID, r.end), constants.SCHED, self.rm.time)
            info("LEASE-%i RealEnd %s" % (r.leaseID, r.prematureend), constants.SCHED, self.rm.time)
            info("LEASE-%i ResReq  %s" % (r.leaseID, r.resreq), constants.SCHED, self.rm.time)
            try:
                self.scheduleExactLease(r)
                self.scheduledleases.add(r)
                self.rm.stats.incrAccepted(r.leaseID)
            except SchedException, msg:
                self.rm.stats.incrRejected(r.leaseID)
                info("LEASE-%i Scheduling exception: %s" % (r.leaseID, msg), constants.SCHED, self.rm.time)
               
        done = False
        newqueue = ds.Queue(self)
        while not done and not self.isQueueEmpty():
            if self.numbesteffortres == self.maxres and self.slottable.isFull(self.rm.time):
                info("Used up all reservations and slot table is full. Skipping rest of queue.", constants.SCHED, self.rm.time)
                done = True
            else:
                r = self.queue.dequeue()
                try:
                    info("LEASE-%i Processing request (BEST-EFFORT)" % r.leaseID, constants.SCHED, self.rm.time)
                    info("LEASE-%i Maxdur  %s" % (r.leaseID, r.maxdur), constants.SCHED, self.rm.time)
                    info("LEASE-%i Remdur  %s" % (r.leaseID, r.remdur), constants.SCHED, self.rm.time)
                    info("LEASE-%i Realdur %s" % (r.leaseID, r.realremdur), constants.SCHED, self.rm.time)
                    info("LEASE-%i ResReq  %s" % (r.leaseID, r.resreq), constants.SCHED, self.rm.time)
                    self.scheduleBestEffortLease(r)
                    self.scheduledleases.add(r)
                    self.rm.stats.decrQueueSize(r.leaseID)
                    self.rm.stats.stopQueueWait(r.leaseID)
                except SchedException, msg:
                    # Put back on queue
                    newqueue.enqueue(r)
                    info("LEASE-%i Scheduling exception: %s" % (r.leaseID, msg), constants.SCHED, self.rm.time)
                    if not self.rm.config.isBackfilling():
                        done = True
                except CancelException, msg:
                    # Don't do anything. This effectively cancels the lease.
                    self.rm.stats.decrQueueSize(r.leaseID)
                    
        newqueue.q += self.queue.q 
        self.queue = newqueue

        self.processReservations()        
    
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
                    self.handleEndSuspend(l, rr)
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    self.handleEndResume(l, rr)
        
        for l in starting:
            rrs = l.getStartingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    pass
                elif isinstance(rr,ds.VMResourceReservation):
                    self.handleStartVM(l, rr)                    
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    self.handleStartSuspend(l, rr)
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    self.handleStartResume(l, rr)

    def handleStartVM(self, l, rr):
        info("LEASE-%i Start of handleStartVM" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        if l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            if isinstance(l,ds.BestEffortLease):
                self.rm.stats.startExec(l.leaseID)            
            # TODO: Enactment
        elif l.state == constants.LEASE_STATE_SUSPENDED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
        l.printContents()
        debug("LEASE-%i End of handleStartVM" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndVM(self, l, rr):
        info("LEASE-%i Start of handleEndVM" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        prematureend = (rr.realend != None and rr.realend < rr.end)
        if prematureend:
            info("LEASE-%i This is a premature end." % l.leaseID, constants.SCHED, self.rm.time)
        if isinstance(l,ds.BestEffortLease):
            l.remdur -= self.rm.time - rr.start
            l.realremdur -= self.rm.time - rr.start
        if rr.oncomplete == constants.ONCOMPLETE_ENDLEASE:
            l.state = constants.LEASE_STATE_DONE
            rr.state = constants.RES_STATE_DONE
            if not prematureend:
                rr.realend = rr.end
            else:
                self.slottable.updateEndTimes(rr.db_rsp_ids, rr.realend)
                self.slottable.commit()
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            if isinstance(l,ds.BestEffortLease):
                self.rm.stats.incrBestEffortCompleted(l.leaseID)            
        elif rr.oncomplete == constants.ONCOMPLETE_SUSPEND:
            if isinstance(l,ds.BestEffortLease):
                if not prematureend:
                    rr.realend = rr.end
                    rr.state = constants.RES_STATE_DONE
                else:
                    l.state = constants.LEASE_STATE_DONE
                    rr.state = constants.RES_STATE_DONE
                    rrs = l.nextRRs(rr)
                    for r in rrs:
                        l.removeRR(r)
                    self.completedleases.add(l)
                    self.scheduledleases.remove(l)
                    self.rm.stats.incrBestEffortCompleted(l.leaseID)            
                    self.slottable.updateEndTimes(rr.db_rsp_ids, rr.realend)
                    self.slottable.commit()
                    # TODO: Clean up next reservations
        
        if isinstance(l,ds.BestEffortLease):
            if rr.backfillres == True:
                self.numbesteffortres -= 1
        if prematureend and self.rm.config.isBackfilling():
            self.reevaluateSchedule(l, True)
        l.printContents()
        debug("LEASE-%i End of handleEndVM" % l.leaseID, constants.SCHED, self.rm.time)
        

    def handleStartSuspend(self, l, rr):
        info("LEASE-%i Start of handleStartSuspend" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        rr.state = constants.RES_STATE_ACTIVE
        l.printContents()
        debug("LEASE-%i End of handleStartSuspend" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndSuspend(self, l, rr):
        info("LEASE-%i Start of handleEndSuspend" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        rr.state = constants.RES_STATE_DONE
        l.state = constants.LEASE_STATE_SUSPENDED
        self.scheduledleases.remove(l)
        self.queue.enqueueInOrder(l)
        self.rm.stats.incrQueueSize(l.leaseID)
        l.printContents()
        debug("LEASE-%i End of handleEndSuspend" % l.leaseID, constants.SCHED, self.rm.time)

    def handleStartResume(self, l, rr):
        info("LEASE-%i Start of handleStartResume" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        rr.state = constants.RES_STATE_ACTIVE
        l.printContents()
        debug("LEASE-%i End of handleStartResume" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndResume(self, l, rr):
        info("LEASE-%i Start of handleEndResume" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        rr.state = constants.RES_STATE_DONE
        l.printContents()
        debug("LEASE-%i End of handleEndResume" % l.leaseID, constants.SCHED, self.rm.time)
    
    def scheduleExactLease(self, req):
        try:
            (mappings, preemptions, transfers, db_rsp_ids) = self.slottable.fitExact(req.leaseID, req.start, req.end, req.vmimage, req.numnodes, req.resreq, prematureend=req.prematureend, preemptible=False, canpreempt=True)
            if len(preemptions) > 0:
                info("Must preempt the following: %s" % preemptions, constants.SCHED, self.rm.time)
                leases = self.slottable.findLeasesToPreempt(preemptions, req.start, req.end)
                for l in leases:
                    self.preempt(self.scheduledleases.getLease(l), time=req.start)
            
            # Schedule image transfers
            dotransfer = False
            
            if dotransfer:
                req.state = constants.LEASE_STATE_SCHEDULED
            else:
                req.state = constants.LEASE_STATE_DEPLOYED            
            
            
            # Add resource reservations
            vmrr = ds.VMResourceReservation(req, req.start, req.end, req.prematureend, mappings, constants.ONCOMPLETE_ENDLEASE, False, db_rsp_ids)
            vmrr.state = constants.RES_STATE_SCHEDULED
            req.appendRR(vmrr)
            
            self.slottable.commit()
        except SlotFittingException, msg:
            self.slottable.rollback()
            raise SchedException, "The requested exact lease is infeasible. Reason: %s" % msg

    def scheduleBestEffortLease(self, req):
        # Determine earliest start time in each node
        # This depends on image transfer schedule. For now, the earliest
        # start time is now (no image transfers)
        
        numnodes = self.rm.config.getNumPhysicalNodes()
        earliest = dict([(node+1, [self.rm.time,constants.TRANSFER_NO]) for node in range(numnodes)])
        suspendable = self.rm.config.isSuspensionAllowed()
        try:
            mustresume = (req.state == constants.LEASE_STATE_SUSPENDED)
            canreserve = self.canReserveBestEffort()
            (mappings, start, end, realend, resumetime, suspendtime, reservation, db_rsp_ids, resume_rsp_id, suspend_rsp_id) = self.slottable.fitBestEffort(req, earliest, canreserve, suspendable=suspendable, mustresume=mustresume)
            if req.maxqueuetime != None:
                self.slottable.rollback()
                msg = "Lease %i is being scheduled, but is meant to be cancelled at %s" % (req.leaseID, req.maxqueuetime)
                warning(msg, constants.SCHED, self.rm.time)
                raise CancelException, msg
            
            # Schedule image transfers
            dotransfer = False
            
            if req.state == constants.LEASE_STATE_PENDING:
                if dotransfer:
                    req.state = constants.LEASE_STATE_SCHEDULED
                else:
                    req.state = constants.LEASE_STATE_DEPLOYED            

            # Add resource reservations
            if resumetime != None:
                resmrr = ds.ResumptionResourceReservation(req, start-resumetime, start, mappings, [resume_rsp_id])
                resmrr.state = constants.RES_STATE_SCHEDULED
                req.appendRR(resmrr)

            if suspendtime != None:
                oncomplete = constants.ONCOMPLETE_SUSPEND
            else:
                oncomplete = constants.ONCOMPLETE_ENDLEASE

            vmrr = ds.VMResourceReservation(req, start, end, realend, mappings, oncomplete, reservation, db_rsp_ids)
            vmrr.state = constants.RES_STATE_SCHEDULED
            req.appendRR(vmrr)

            if suspendtime != None:
                susprr = ds.SuspensionResourceReservation(req, end, end + suspendtime, mappings, [suspend_rsp_id])
                susprr.state = constants.RES_STATE_SCHEDULED
                req.appendRR(susprr)
           
            if reservation:
                self.numbesteffortres += 1
            
            self.slottable.commit()        
        except SlotFittingException, msg:
            self.slottable.rollback()
            raise SchedException, "The requested best-effort lease is infeasible. Reason: %s" % msg
        
    def preempt(self, req, time):
        info("Preempting lease %i at time %s." % (req.leaseID, time), constants.SCHED, self.rm.time)
        edebug("Lease before preemption:", constants.SCHED, self.rm.time)
        req.printContents()
        vmrr, susprr  = req.getLastVMRR()
        if vmrr.state == constants.RES_STATE_SCHEDULED and vmrr.start >= time:
            debug("The lease has not yet started. Removing reservation and resubmitting to queue.", constants.SCHED, self.rm.time)
            req.state = constants.LEASE_STATE_PENDING
            if vmrr.backfillres == True:
                self.numbesteffortres -= 1
            req.removeRR(vmrr)
            if susprr != None:
                req.removeRR(susprr)
            self.scheduledleases.remove(req)
            self.queue.enqueueInOrder(req)
            self.rm.stats.incrQueueSize(req.leaseID)
        else:
            if self.rm.config.isSuspensionAllowed():
                debug("The lease will be suspended while running.", constants.SCHED, self.rm.time)
                self.slottable.suspend(req, time)
            else:
                debug("The lease has to be cancelled and resubmitted.", constants.SCHED, self.rm.time)
                req.state = constants.LEASE_STATE_PENDING
                if vmrr.backfillres == True:
                    self.numbesteffortres -= 1
                req.removeRR(vmrr)
                if susprr != None:
                    req.removeRR(susprr)
                if req.state == constants.LEASE_STATE_SUSPENDED:
                    resmrr = lease.prevRR(vmrr)
                    req.removeRR(resmrr)
                self.scheduledleases.remove(req)
                self.queue.enqueueInOrder(req)
                self.rm.stats.incrQueueSize(req.leaseID)
        edebug("Lease after preemption:", constants.SCHED, self.rm.time)
        req.printContents()
        
    def reevaluateSchedule(self, endinglease, checkreal):
        vmrr, susprr = endinglease.getLastVMRR()
        if checkreal:
            end = vmrr.realend
        else:
            end = vmrr.end
        nodes = vmrr.nodes.values()
        debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes,end), constants.SCHED, self.rm.time)
        leases = self.scheduledleases.getNextLeasesScheduledInNodes(end, nodes)
        leases = [l for l in leases if isinstance(l,ds.BestEffortLease)]
        for l in leases:
            debug("Found lease %i" % l.leaseID, constants.SCHED, self.rm.time)
            l.printContents()
            self.slottable.slideback(l, end)
        for l in leases:
            self.reevaluateSchedule(l, False)
            
        
    def findEarliestStartingTimes(self, imageURI, imageSize, time):
        pass
    
    def existsScheduledLeases(self):
        return not self.scheduledleases.isEmpty()
    
    def isQueueEmpty(self):
        return self.queue.isEmpty()
    
    def enqueue(self, req):
        self.queue.enqueue(req)
        
    def canReserveBestEffort(self):
        return self.numbesteffortres < self.maxres
