import workspace.haizea.resourcemanager.datastruct as ds
from workspace.haizea.resourcemanager.slottable import SlotTable, SlotFittingException
from workspace.haizea.common.log import info, debug, warning, edebug
import workspace.haizea.common.constants as constants
import copy
from mx.DateTime import TimeDelta

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
        self.transfersEDF = []
        self.transfersFIFO = []
        self.completedTransfers = []
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
                    self.handleEndFileTransfer(l,rr)
                elif isinstance(rr,ds.VMResourceReservation):
                    self.handleEndVM(l, rr)
                elif isinstance(rr,ds.SuspensionResourceReservation):
                    self.handleEndSuspend(l, rr)
                elif isinstance(rr,ds.ResumptionResourceReservation):
                    self.handleEndResume(l, rr)
                self.handleEndRR(l, rr)
        
        for l in starting:
            rrs = l.getStartingReservations(self.rm.time)
            for rr in rrs:
                if isinstance(rr,ds.FileTransferResourceReservation):
                    self.handleStartFileTransfer(l,rr)
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
            # TODO: More enactment
            
            # Check that the image is available
            # If we're reusing images, this might require creating
            # a tainted copy
            if self.rm.config.getTransferType() != constants.TRANSFER_NONE:
                for (vnode,pnode) in rr.nodes.items():
                    # TODO: Add some error checking here
                    self.rm.enactment.checkImage(pnode, l.leaseID, vnode, l.vmimage)
                    l.vmimagemap[vnode] = pnode

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
                rr.end = rr.realend
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            for vnode,pnode in l.vmimagemap.items():
                self.rm.enactment.removeImage(pnode, l.leaseID, vnode)
            if isinstance(l,ds.BestEffortLease):
                self.rm.stats.incrBestEffortCompleted(l.leaseID)
                self.rm.stats.addBoundedSlowdown(l.leaseID, l.getSlowdown(self.rm.time))        
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
                        self.slottable.removeReservation(r)
                    self.completedleases.add(l)
                    self.scheduledleases.remove(l)
                    for vnode,pnode in l.vmimagemap.items():
                        self.rm.enactment.removeImage(pnode, l.leaseID, vnode)
                    self.rm.stats.incrBestEffortCompleted(l.leaseID)   
                    self.rm.stats.addBoundedSlowdown(l.leaseID, l.getSlowdown(self.rm.time))         
        
        if isinstance(l,ds.BestEffortLease):
            if rr.backfillres == True:
                self.numbesteffortres -= 1
        if prematureend and self.rm.config.isBackfilling():
            self.reevaluateSchedule(l, rr.nodes.values(), self.rm.time, [])
        l.printContents()
        debug("LEASE-%i End of handleEndVM" % l.leaseID, constants.SCHED, self.rm.time)
        
    def handleStartFileTransfer(self, l, rr):
        info("LEASE-%i Start of handleStartFileTransfer" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        if l.state == constants.LEASE_STATE_SCHEDULED or l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_DEPLOYING
            rr.state = constants.RES_STATE_ACTIVE
            # TODO: Enactment
        elif l.state == constants.LEASE_STATE_SUSPENDED:
            pass
            # TODO: Migrating
        l.printContents()
        debug("LEASE-%i End of handleStartFileTransfer" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndFileTransfer(self, l, rr):
        info("LEASE-%i Start of handleEndFileTransfer" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        if l.state == constants.LEASE_STATE_DEPLOYING:
            l.state = constants.LEASE_STATE_DEPLOYED
            rr.state = constants.RES_STATE_DONE
            for physnode in rr.transfers:
                vnodes = rr.transfers[physnode]
                
                # Update VM Image maps
                for leaseID,v in vnodes:
                    lease = self.scheduledleases.getLease(leaseID)
                    lease.vmimagemap[v] = physnode
                    
                # Find out timeout of image. It will be the latest end time of all the
                # leases being used by that image.
                leases = [l for (l,v) in vnodes]
                maxend=None
                for leaseID in leases:
                    l = self.scheduledleases.getLease(leaseID)
                    end = l.getEnd()
                    if maxend==None or end>maxend:
                        maxend=end
                self.rm.enactment.addImageToNode(physnode, rr.file, l.vmimagesize, vnodes, timeout=maxend)
        elif l.state == constants.LEASE_STATE_SUSPENDED:
            pass
            # TODO: Migrating
        l.printContents()
        debug("LEASE-%i End of handleEndFileTransfer" % l.leaseID, constants.SCHED, self.rm.time)


    def handleStartSuspend(self, l, rr):
        info("LEASE-%i Start of handleStartSuspend" % l.leaseID, constants.SCHED, self.rm.time)
        l.printContents()
        rr.state = constants.RES_STATE_ACTIVE
        for vnode,pnode in rr.nodes.items():
            self.rm.enactment.addRAMFileToNode(pnode, l.leaseID, vnode, l.resreq.res[constants.RES_MEM])
            l.memimagemap[vnode] = pnode
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
        for vnode,pnode in rr.nodes.items():
            self.rm.enactment.removeRAMFileFromNode(pnode, l.leaseID, vnode)
        l.printContents()
        debug("LEASE-%i End of handleEndResume" % l.leaseID, constants.SCHED, self.rm.time)

    def handleEndRR(self, l, rr):
        self.slottable.removeReservation(rr)
    
    def scheduleExactLease(self, req):
        try:
            (nodeassignment, res, preemptions) = self.slottable.fitExact(req, preemptible=False, canpreempt=True)
            if len(preemptions) > 0:
                info("Must preempt the following: %s" % preemptions, constants.SCHED, self.rm.time)
                leases = self.slottable.findLeasesToPreempt(preemptions, req.start, req.end)
                for l in leases:
                    self.preempt(self.scheduledleases.getLease(l), time=req.start)
            
            # Schedule image transfers
            transfertype = self.rm.config.getTransferType()
            reusealg = self.rm.config.getReuseAlg()
            avoidredundant = self.rm.config.isAvoidingRedundantTransfers()
            
            if transfertype == constants.TRANSFER_NONE:
                req.state = constants.LEASE_STATE_DEPLOYED
            else:
                req.state = constants.LEASE_STATE_SCHEDULED
                
                if avoidredundant:
                    pass
                    #TODO
                    
                musttransfer = {}
                mustpool = {}
                for (vnode,pnode) in nodeassignment:
                    leaseID = req.leaseID
                    vnode = transfer[0]
                    pnode = transfer[1]
                    info("Scheduling image transfer of '%s' from vnode %i to physnode %i" % (req.vmimage, vnode, pnode), constants.SCHED, self.rm.time)

                    if reusealg == constants.REUSE_COWPOOL:
                        if self.rm.enactment.isInPool(pnode,req.vmimage, req.start):
                            info("No need to schedule an image transfer (reusing an image in pool)", constants.SCHED, self.rm.time)
                            mustpool[vnode] = pnode                            
                        else:
                            info("Need to schedule a transfer.", constants.SCHED, self.rm.time)
                            musttransfer[vnode] = pnode
                    else:
                        info("Need to schedule a transfer.", constants.SCHED, self.rm.time)
                        musttransfer[vnode] = pnode

                if len(musttransfer) == 0:
                    req.state = constants.LEASE_STATE_DEPLOYED
                else:
                    if transfertype == constants.TRANSFER_UNICAST:
                        # Dictionary of transfer RRs. Key is the physical node where
                        # the image is being transferred to
                        transferRRs = {}
                        for vnode,pnode in musttransfer:
                            if transferRRs.has_key(physnode):
                                # We've already scheduled a transfer to this node. Reuse it.
                                info("No need to schedule an image transfer (reusing an existing transfer)", constants.SCHED, self.rm.time)
                                transferRR = transferRR[physnode]
                                transferRR.piggyback(leaseID, vnode, physnode, req.end)
                            else:
                                filetransfer = self.scheduleImageTransferEDF(req, {vnode:physnode})                 
                                transferRRs[physnode] = filetransfer
                                req.appendRR(filetransfer)
                    elif transfertype == constants.TRANSFER_MULTICAST:
                        filetransfer = self.scheduleImageTransferEDF(req, musttransfer)
                        req.appendRR(filetransfer)
 
            # No chance of scheduling exception at this point. It's safe
            # to add entries to the pools
            if reusealg == constants.REUSE_COWPOOL:
                for (vnode,pnode) in mustpool.items():
                    self.rm.enactment.addToPool(pnode, req.vmimage, leaseID, vnode, req.start)
 
            # Add resource reservations
            vmrr = ds.VMResourceReservation(req, req.start, req.end, req.prematureend, nodeassignment, res, constants.ONCOMPLETE_ENDLEASE, False)
            vmrr.state = constants.RES_STATE_SCHEDULED
            req.appendRR(vmrr)
            self.slottable.addReservation(vmrr)
        except SlotFittingException, msg:
            raise SchedException, "The requested exact lease is infeasible. Reason: %s" % msg

    def scheduleBestEffortLease(self, req):
        # Determine earliest start time in each node
        if req.state == constants.LEASE_STATE_PENDING:
            # Figure out earliest start times based on
            # image schedule and reusable images
            earliest = self.findEarliestStartingTimes(req)
        elif req.state == constants.LEASE_STATE_SUSPENDED:
            # No need to transfer images from repository
            # (only intra-node transfer)
            earliest = dict([(node+1, [self.rm.time,constants.REQTRANSFER_NO, None]) for node in range(req.numnodes)])
            
        susptype = self.rm.config.getSuspensionType()
        if susptype == constants.SUSPENSION_NONE:
            suspendable = False
            preemptible = True
        elif susptype == constants.SUSPENSION_ALL:
            suspendable = True
            preemptible = True
        elif susptype == constants.SUSPENSION_SERIAL:
            if req.numnodes == 1:
                suspendable = True
                preemptible = True
            else:
                suspendable = False
                preemptible = True
        canmigrate = self.rm.config.isMigrationAllowed()
        try:
            mustresume = (req.state == constants.LEASE_STATE_SUSPENDED)
            canreserve = self.canReserveBestEffort()
            (resmrr, vmrr, susprr, reservation) = self.slottable.fitBestEffort(req, earliest, canreserve, suspendable=suspendable, preemptible=preemptible, canmigrate=canmigrate, mustresume=mustresume)
            if req.maxqueuetime != None:
                self.slottable.rollback()
                msg = "Lease %i is being scheduled, but is meant to be cancelled at %s" % (req.leaseID, req.maxqueuetime)
                warning(msg, constants.SCHED, self.rm.time)
                raise CancelException, msg
            
            # Schedule image transfers
            transfertype = self.rm.config.getTransferType()
            reusealg = self.rm.config.getReuseAlg()
            avoidredundant = self.rm.config.isAvoidingRedundantTransfers()
            
            if req.state == constants.LEASE_STATE_PENDING:
                if transfertype == constants.TRANSFER_NONE:
                    req.state = constants.LEASE_STATE_DEPLOYED
                else:
                    req.state = constants.LEASE_STATE_SCHEDULED
                    transferRRs = []
                    musttransfer = {}
                    piggybacking = []
                    for (vnode, pnode) in mappings.items():
                        reqtransfer = earliest[pnode][1]
                        if reqtransfer == constants.REQTRANSFER_COWPOOL:
                            # Add to pool
                            info("Reusing image for V%i->P%i." % (vnode, pnode), constants.SCHED, self.rm.time)
                            self.rm.enactment.addToPool(pnode, req.vmimage, req.leaseID, vnode, end)
                        elif reqtransfer == constants.REQTRANSFER_PIGGYBACK:
                            # We can piggyback on an existing transfer
                            transferRR = earliest[pnode][2]
                            transferRR.piggyback(req.leaseID, vnode, pnode)
                            info("Piggybacking transfer for V%i->P%i on existing transfer in lease %i." % (vnode, pnode, transferRR.lease.leaseID), constants.SCHED, self.rm.time)
                            piggybacking.append(transferRR)
                        else:
                            # Transfer
                            musttransfer[vnode] = pnode
                            info("Must transfer V%i->P%i." % (vnode, pnode), constants.SCHED, self.rm.time)
                    if len(musttransfer)>0:
                        transferRRs = self.scheduleImageTransferFIFO(req, musttransfer)
                        endtransfer = transferRRs[-1].end
                        req.imagesavail = endtransfer
                    else:
                        # TODO: Not strictly correct. Should mark the lease
                        # as deployed when piggybacked transfers have concluded
                        req.state = constants.LEASE_STATE_DEPLOYED
                    if len(piggybacking) > 0: 
                        endtimes = [t.end for t in piggybacking]
                        if len(musttransfer) > 0:
                            endtimes.append(endtransfer)
                        req.imagesavail = max(endtimes)
                    if len(musttransfer)==0 and len(piggybacking)==0:
                        req.state = constants.LEASE_STATE_DEPLOYED
                        req.imagesavail = self.rm.time
                    for rr in transferRRs:
                        req.appendRR(rr)
            elif req.state == constants.LEASE_STATE_SUSPENDED:
                # TODO: This would be more correctly handled in the RR handle functions.
                # Update VM image mappings, since we might be resuming
                # in different nodes.
                for vnode,pnode in req.vmimagemap.items():
                    self.rm.enactment.removeImage(pnode, req.leaseID, vnode)
                req.vmimagemap = vmrr.nodes
                for vnode,pnode in req.vmimagemap.items():
                    self.rm.enactment.addTaintedImageToNode(pnode, req.vmimage, req.vmimagesize, req.leaseID, vnode)
                # Update RAM file mappings
                for vnode,pnode in req.memimagemap.items():
                    self.rm.enactment.removeRAMFileFromNode(pnode, req.leaseID, vnode)
                for vnode,pnode in vmrr.nodes.items():
                    self.rm.enactment.addRAMFileToNode(pnode, req.leaseID, vnode, req.resreq.res[constants.RES_MEM])
                    req.memimagemap[vnode] = pnode
                    
            # Add resource reservations
            if resmrr != None:
                req.appendRR(resmrr)
                self.slottable.addReservation(resmrr)
            req.appendRR(vmrr)
            self.slottable.addReservation(vmrr)
            if susprr != None:
                req.appendRR(susprr)
                self.slottable.addReservation(susprr)
           
            if reservation:
                self.numbesteffortres += 1
            
        except SlotFittingException, msg:
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
            for vnode,pnode in req.vmimagemap.items():
                self.rm.enactment.removeImage(pnode, req.leaseID, vnode)
            self.removeFromFIFOTransfers(req.leaseID)
            req.vmimagemap = {}
            self.scheduledleases.remove(req)
            self.queue.enqueueInOrder(req)
            self.rm.stats.incrQueueSize(req.leaseID)
        else:
            susptype = self.rm.config.getSuspensionType()
            if susptype == constants.SUSPENSION_ALL or (req.numnodes == 1 and susptype == constants.SUSPENSION_SERIAL):
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
                for vnode,pnode in req.vmimagemap.items():
                    self.rm.enactment.removeImage(pnode, req.leaseID, vnode)
                self.removeFromFIFOTransfers(req.leaseID)
                req.vmimagemap = {}
                self.scheduledleases.remove(req)
                self.queue.enqueueInOrder(req)
                self.rm.stats.incrQueueSize(req.leaseID)
        edebug("Lease after preemption:", constants.SCHED, self.rm.time)
        req.printContents()
        
    def reevaluateSchedule(self, endinglease, nodes, endtime, checkedleases):
        debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes,endtime), constants.SCHED, self.rm.time)        
        leases = self.scheduledleases.getNextLeasesScheduledInNodes(endtime, nodes)
        leases = [l for l in leases if isinstance(l,ds.BestEffortLease) and not l in checkedleases]
        for l in leases:
            debug("Found lease %i" % l.leaseID, constants.SCHED, self.rm.time)
            l.printContents()
            # Earliest time can't be earlier than time when images will be
            # available in node
            earliest = max(endtime, l.imagesavail)
            self.slottable.slideback(l, earliest)
            checkedleases.append(l)
        #for l in leases:
        #    vmrr, susprr = l.getLastVMRR()
        #    self.reevaluateSchedule(l, vmrr.nodes.values(), vmrr.end, checkedleases)
            
        
    def findEarliestStartingTimes(self, req):
        numnodes = self.rm.config.getNumPhysicalNodes()
        transfertype = self.rm.config.getTransferType()        
        reusealg = self.rm.config.getReuseAlg()
        avoidredundant = self.rm.config.isAvoidingRedundantTransfers()
        imgTransferTime=self.estimateTransferTime(req.vmimagesize)
        
        # Figure out starting time assuming we have to transfer the image
        nextfifo = self.getNextFIFOTransferTime()
        
        if transfertype == constants.TRANSFER_NONE:
            earliest = dict([(node+1, [self.rm.time,constants.REQTRANSFER_NO, None]) for node in range(numnodes)])
        else:
            # Find worst-case earliest start time
            if req.numnodes == 1:
                startTime = nextfifo + imgTransferTime
                earliest = dict([(node+1, [startTime,constants.REQTRANSFER_YES]) for node in range(numnodes)])                
            else:
                # Unlike the previous case, we may have to find a new start time
                # for all the nodes.
                if transfertype == constants.TRANSFER_UNICAST:
                    pass
                    # TODO: If transferring each image individually, this will
                    # make determining what images can be reused more complicated.
                if transfertype == constants.TRANSFER_MULTICAST:
                    startTime = nextfifo + imgTransferTime
                    earliest = dict([(node+1, [startTime,constants.REQTRANSFER_YES]) for node in range(numnodes)])                                    # TODO: Take into account reusable images
            
            # Check if we can reuse images
            if reusealg==constants.REUSE_COWPOOL:
                nodeswithimg = self.rm.enactment.getNodesWithImgInPool(req.vmimage)
                for node in nodeswithimg:
                    earliest[node] = [self.rm.time, constants.REQTRANSFER_COWPOOL]
            
                    
            # Check if we can avoid redundant transfers
            if avoidredundant:
                if transfertype == constants.TRANSFER_UNICAST:
                    pass
                    # TODO
                if transfertype == constants.TRANSFER_MULTICAST:                
                    # We can only piggyback on transfers that haven't started yet
                    transfers = [t for t in self.transfersFIFO if t.state == constants.RES_STATE_SCHEDULED]
                    for t in transfers:
                        if t.file == req.vmimage:
                            startTime = t.end
                            if startTime > self.rm.time:
                                for n in earliest:
                                    if startTime < earliest[n]:
                                        earliest[n] = [startTime, constants.REQTRANSFER_PIGGYBACK, t]

        return earliest

    def scheduleImageTransferEDF(self, req, vnodes):
        # Estimate image transfer time 
        imgTransferTime=self.estimateTransferTime(req.vmimagesize)
        
        # Determine start time
        activetransfers = [t for t in self.transfersEDF if t.state == constants.RES_STATE_ACTIVE]
        if len(activetransfers) > 0:
            startTime = activetransfers[-1].end
        else:
            startTime = self.rm.time
        
        transfermap = dict([(copy.copy(t), t) for t in self.transfersEDF if t.state == constants.RES_STATE_SCHEDULED])
        newtransfers = transfermap.keys()
        
        newtransfer = ds.FileTransferResourceReservation(req)
        newtransfer.deadline = req.start
        newtransfer.state = constants.RES_STATE_SCHEDULED
        newtransfer.file = req.vmimage
        for vnode,pnode in vnodes.items():
            newtransfer.piggyback(req.leaseID, vnode, pnode)
        newtransfers.append(newtransfer)

        def comparedates(x,y):
            dx=x.deadline
            dy=y.deadline
            if dx>dy:
                return 1
            elif dx==dy:
                # If deadlines are equal, we break the tie by order of arrival
                # (currently, we just check if this is the new transfer)
                if x == newtransfer:
                    return 1
                elif y == newtransfer:
                    return -1
                else:
                    return 0
            else:
                return -1
        
        # Order transfers by deadline
        newtransfers.sort(comparedates)

        # Compute start times and make sure that deadlines are met
        fits = True
        for t in newtransfers:
            if t == newtransfer:
                duration = imgTransferTime
            else:
                duration = t.end - t.start
                
            t.start = startTime
            t.end = startTime + duration
            if t.end > t.deadline:
                fits = False
                break
            startTime = t.end
             
        if not fits:
             raise SchedException, "Adding this VW results in an unfeasible image transfer schedule."

        # Push image transfers as close as possible to their deadlines. 
        feasibleEndTime=newtransfers[-1].deadline
        for t in reversed(newtransfers):
            if t == newtransfer:
                duration = imgTransferTime
            else:
                duration = t.end - t.start
    
            newEndTime=min([t.deadline,feasibleEndTime])
            t.end=newEndTime
            newStartTime=newEndTime-duration
            t.start=newStartTime
            feasibleEndTime=newStartTime
        
        # Make changes   
        for t in newtransfers:
            if t == newtransfer:
                rsp_id = self.slottable.addImageTransfer(req, t)
                t.db_rsp_ids = [rsp_id]
                self.transfersEDF.append(t)
            else:
                t2 = transfermap[t]
                t2.start = t.start
                t2.end = t.end
                t2.updateDBentry()            
        
        return newtransfer
    
    def scheduleImageTransferFIFO(self, req, reqtransfers):
        # Estimate image transfer time 
        imgTransferTime=self.estimateTransferTime(req.vmimagesize)
        startTime = self.getNextFIFOTransferTime()
        transfertype = self.rm.config.getTransferType()        

        newtransfers = []
        
        if transfertype == constants.TRANSFER_UNICAST:
            pass
            # TODO: If transferring each image individually, this will
            # make determining what images can be reused more complicated.
        if transfertype == constants.TRANSFER_MULTICAST:
            # Time to transfer is imagesize / bandwidth, regardless of 
            # number of nodes
            newtransfer = ds.FileTransferResourceReservation(req)
            newtransfer.start = startTime
            newtransfer.end = startTime+imgTransferTime
            newtransfer.deadline = None
            newtransfer.state = constants.RES_STATE_SCHEDULED
            newtransfer.file = req.vmimage
            for vnode in reqtransfers:
                physnode = reqtransfers[vnode]
                newtransfer.piggyback(req.leaseID, vnode, physnode)
            rsp_id = self.slottable.addImageTransfer(req, newtransfer)
            newtransfer.db_rsp_ids = [rsp_id]
            newtransfers.append(newtransfer)
            
        self.transfersFIFO += newtransfers
        
        return newtransfers
    
    def getNextFIFOTransferTime(self):
        transfers = [t for t in self.transfersFIFO if t.state != constants.RES_STATE_DONE]
        if len(transfers) > 0:
            startTime = transfers[-1].end
        else:
            startTime = self.rm.time
        return startTime

    def removeFromFIFOTransfers(self, leaseID):
        transfers = [t for t in self.transfersFIFO if t.state != constants.RES_STATE_DONE]
        toremove = []
        for t in transfers:
            for pnode in t.transfers:
                leases = [l for l,v in t.transfers[pnode]]
                if leaseID in leases:
                    newtransfers = [(l,v) for l,v in t.transfers[pnode] if l!=leaseID]
                    t.transfers[pnode] = newtransfers
            # Check if the transfer has to be cancelled
            a = sum([len(l) for l in t.transfers.values()])
            if a == 0:
                t.lease.removeRR(t)
                toremove.append(t)
        for t in toremove:
            self.transfersFIFO.remove(t)
    
    def estimateTransferTime(self, imgsize):
        forceTransferTime = self.rm.config.getForceTransferTime()
        if forceTransferTime != None:
            return forceTransferTime
        else:      
            bandwidth = self.rm.config.getBandwidth()
            bandwidthMBs = bandwidth / 8
            seconds = imgsize / bandwidthMBs
            return TimeDelta(seconds=seconds)    

    
    def existsScheduledLeases(self):
        return not self.scheduledleases.isEmpty()
    
    def isQueueEmpty(self):
        return self.queue.isEmpty()
    
    def enqueue(self, req):
        self.queue.enqueue(req)
        
    def canReserveBestEffort(self):
        return self.numbesteffortres < self.maxres
