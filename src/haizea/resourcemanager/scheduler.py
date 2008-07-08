# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
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


"""This module provides the main classes for Haizea's scheduler, particularly
the Scheduler class. Note that this class includes high-level scheduling
constructs, and that most of the magic happens in the slottable module. The
deployment scheduling code (everything that has to be done to prepare a lease)
happens in the modules inside the haizea.resourcemanager.deployment package.

This module provides the following classes:

* SchedException: A scheduling exception
* ReservationEventHandler: A simple wrapper class
* Scheduler: Do I really need to spell this one out for you?
"""

import haizea.resourcemanager.datastruct as ds
import haizea.common.constants as constants
from haizea.resourcemanager.slottable import SlotTable, SlotFittingException
from haizea.resourcemanager.deployment.unmanaged import UnmanagedDeployment
from haizea.resourcemanager.deployment.predeployed import PredeployedImagesDeployment
from haizea.resourcemanager.deployment.imagetransfer import ImageTransferDeployment

class SchedException(Exception):
    """A simple exception class used for scheduling exceptions"""
    pass

class ReservationEventHandler(object):
    """A wrapper for reservation event handlers.
    
    Reservations (in the slot table) can start and they can end. This class
    provides a convenient wrapper around the event handlers for these two
    events (see Scheduler.__register_handler for details on event handlers)
    """
    def __init__(self, on_start, on_end):
        self.on_start = on_start
        self.on_end = on_end

class Scheduler(object):
    """The Haizea Scheduler
    
    Public methods:
    schedule -- The scheduling function
    process_reservations -- Processes starting/ending reservations at a given time
    enqueue -- Queues a best-effort request
    is_queue_empty -- Is the queue empty?
    exists_scheduled_leases -- Are there any leases scheduled?
    
    Private methods:
    __schedule_ar_lease -- Schedules an AR lease
    __schedule_besteffort_lease -- Schedules a best-effort lease
    __preempt -- Preempts a lease
    __reevaluate_schedule -- Reevaluate the schedule (used after resources become
                             unexpectedly unavailable)
    _handle_* -- Reservation event handlers
    
    """
    def __init__(self, rm):
        self.rm = rm
        self.logger = self.rm.logger
        self.slottable = SlotTable(self)
        self.queue = ds.Queue(self)
        self.scheduledleases = ds.LeaseTable(self)
        self.completedleases = ds.LeaseTable(self)
        self.rejectedleases = ds.LeaseTable(self)

        deploy_type = self.rm.config.get_lease_deployment_type()
        if deploy_type == constants.DEPLOYMENT_UNMANAGED:
            self.deployment = UnmanagedDeployment(self)
        elif deploy_type == constants.DEPLOYMENT_PREDEPLOY:
            self.deployment = PredeployedImagesDeployment(self)
        elif deploy_type == constants.DEPLOYMENT_TRANSFER:
            self.deployment = ImageTransferDeployment(self)
            
        self.handlers = {}
        
        self.register_handler(type     = ds.VMResourceReservation, 
                              on_start = Scheduler._handle_start_vm,
                              on_end   = Scheduler._handle_end_vm)

        self.register_handler(type     = ds.SuspensionResourceReservation, 
                              on_start = Scheduler._handle_start_suspend,
                              on_end   = Scheduler._handle_end_suspend)

        self.register_handler(type     = ds.ResumptionResourceReservation, 
                              on_start = Scheduler._handle_start_resume,
                              on_end   = Scheduler._handle_end_resume)
            
        self.maxres = self.rm.config.getMaxReservations()
        self.numbesteffortres = 0
    
    def schedule(self, requests, nexttime):        
        if self.rm.config.getNodeSelectionPolicy() == constants.NODESELECTION_AVOIDPREEMPT:
            avoidpreempt = True
        else:
            avoidpreempt = False
        
        # Process AR requests
        for lease_req in requests:
            self.rm.logger.debug("LEASE-%i Processing request (AR)" % lease_req.leaseID, constants.SCHED)
            self.rm.logger.debug("LEASE-%i Start    %s" % (lease_req.leaseID, lease_req.start), constants.SCHED)
            self.rm.logger.debug("LEASE-%i Duration %s" % (lease_req.leaseID, lease_req.duration), constants.SCHED)
            self.rm.logger.debug("LEASE-%i ResReq   %s" % (lease_req.leaseID, lease_req.resreq), constants.SCHED)
            self.rm.logger.info("Received AR lease request #%i, %i nodes from %s to %s." % (lease_req.leaseID, lease_req.numnodes, lease_req.start.requested, lease_req.start.requested + lease_req.duration.requested), constants.SCHED)
            
            accepted = False
            try:
                self.schedule_ar_lease(lease_req, avoidpreempt=avoidpreempt, nexttime=nexttime)
                self.scheduledleases.add(lease_req)
                self.rm.stats.incrCounter(constants.COUNTER_ARACCEPTED, lease_req.leaseID)
                accepted = True
            except SchedException, msg:
                # If our first try avoided preemption, try again
                # without avoiding preemption.
                # TODO: Roll this into the exact slot fitting algorithm
                if avoidpreempt:
                    try:
                        self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.leaseID, msg), constants.SCHED)
                        self.rm.logger.debug("LEASE-%i Trying again without avoiding preemption" % lease_req.leaseID, constants.SCHED)
                        self.schedule_ar_lease(lease_req, nexttime, avoidpreempt=False)
                        self.scheduledleases.add(lease_req)
                        self.rm.stats.incrCounter(constants.COUNTER_ARACCEPTED, lease_req.leaseID)
                        accepted = True
                    except SchedException, msg:
                        self.rm.stats.incrCounter(constants.COUNTER_ARREJECTED, lease_req.leaseID)
                        self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.leaseID, msg), constants.SCHED)
                else:
                    self.rm.stats.incrCounter(constants.COUNTER_ARREJECTED, lease_req.leaseID)
                    self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.leaseID, msg), constants.SCHED)
            if accepted:
                self.rm.logger.info("AR lease request #%i has been accepted." % lease_req.leaseID, constants.SCHED)
            else:
                self.rm.logger.info("AR lease request #%i has been rejected." % lease_req.leaseID, constants.SCHED)
                

        done = False
        newqueue = ds.Queue(self)
        while not done and not self.is_queue_empty():
            if self.numbesteffortres == self.maxres and self.slottable.isFull(nexttime):
                self.rm.logger.debug("Used up all reservations and slot table is full. Skipping rest of queue.", constants.SCHED)
                done = True
            else:
                lease_req = self.queue.dequeue()
                try:
                    self.rm.logger.info("Next request in the queue is lease %i. Attempting to schedule..." % lease_req.leaseID, constants.SCHED)
                    self.rm.logger.debug("LEASE-%i Processing request (BEST-EFFORT)" % lease_req.leaseID, constants.SCHED)
                    self.rm.logger.debug("LEASE-%i Duration: %s" % (lease_req.leaseID, lease_req.duration), constants.SCHED)
                    self.rm.logger.debug("LEASE-%i ResReq  %s" % (lease_req.leaseID, lease_req.resreq), constants.SCHED)
                    self.schedule_besteffort_lease(lease_req, nexttime)
                    self.scheduledleases.add(lease_req)
                    self.rm.stats.decrCounter(constants.COUNTER_QUEUESIZE, lease_req.leaseID)
                except SchedException, msg:
                    # Put back on queue
                    newqueue.enqueue(lease_req)
                    self.rm.logger.debug("LEASE-%i Scheduling exception: %s" % (lease_req.leaseID, msg), constants.SCHED)
                    self.rm.logger.info("Lease %i could not be scheduled at this time." % lease_req.leaseID, constants.SCHED)
                    if not self.rm.config.isBackfilling():
                        done = True
                    
        newqueue.q += self.queue.q 
        self.queue = newqueue 
    
    def process_reservations(self, nowtime):
        starting = [l for l in self.scheduledleases.entries.values() if l.hasStartingReservations(nowtime)]
        ending = [l for l in self.scheduledleases.entries.values() if l.hasEndingReservations(nowtime)]
        for l in ending:
            rrs = l.getEndingReservations(nowtime)
            for rr in rrs:
                self._handle_end_rr(l, rr)
                self.handlers[type(rr)].on_end(self, l, rr)
        
        for l in starting:
            rrs = l.getStartingReservations(nowtime)
            for rr in rrs:
                self.handlers[type(rr)].on_start(self, l, rr)

        util = self.slottable.getUtilization(nowtime)
        self.rm.stats.appendStat(constants.COUNTER_CPUUTILIZATION, util)
        # TODO: Should be moved to rm.py
        self.rm.stats.tick()
        
        
    def register_handler(self, type, on_start, on_end):
        handler = ReservationEventHandler(on_start=on_start, on_end=on_end)
        self.handlers[type] = handler        
    
    
    def enqueue(self, lease_req):
        """Queues a best-effort lease request"""
        self.rm.stats.incrCounter(constants.COUNTER_QUEUESIZE, lease_req.leaseID)
        self.queue.enqueue(lease_req)
        self.rm.logger.info("Received (and queueing) best-effort lease request #%i, %i nodes for %s." % (lease_req.leaseID, lease_req.numnodes, lease_req.duration.requested), constants.SCHED)


    def is_queue_empty(self):
        """Return True is the queue is empty, False otherwise"""
        return self.queue.isEmpty()

    
    def exists_scheduled_leases(self):
        """Return True if there are any leases scheduled in the future"""
        return not self.scheduledleases.isEmpty()    

    def notify_premature_end_vm(self, l, rr):
        self.rm.logger.info("LEASE-%i The VM has ended prematurely." % l.leaseID, constants.SCHED)
        self._handle_end_rr(l, rr)
        if rr.oncomplete == constants.ONCOMPLETE_SUSPEND:
            rrs = l.nextRRs(rr)
            for r in rrs:
                l.removeRR(r)
                self.slottable.removeReservation(r)
        rr.oncomplete = constants.ONCOMPLETE_ENDLEASE
        rr.end = self.rm.clock.get_time()
        self._handle_end_vm(l, rr)
        nexttime = self.rm.clock.getNextSchedulableTime()
        if self.rm.config.isBackfilling():
            # We need to reevaluate the schedule to see if there are any future
            # reservations that we can slide back.
            self.reevaluateSchedule(l, rr.nodes.values(), nexttime, [])
    
    
    def schedule_ar_lease(self, lease_req, nexttime, avoidpreempt=True):
        start = lease_req.start.requested
        end = lease_req.start.requested + lease_req.duration.requested
        try:
            (nodeassignment, res, preemptions) = self.slottable.fitExact(lease_req, preemptible=False, canpreempt=True, avoidpreempt=avoidpreempt)
            
            if len(preemptions) > 0:
                leases = self.slottable.findLeasesToPreempt(preemptions, start, end)
                self.rm.logger.info("Must preempt leases %s to make room for AR lease #%i" % ([l.leaseID for l in leases], lease_req.leaseID), constants.SCHED)
                for lease in leases:
                    self.preempt(lease, time=start)
            

            # Add VM resource reservations
            vmrr = ds.VMResourceReservation(lease_req, start, end, nodeassignment, res, constants.ONCOMPLETE_ENDLEASE, False)
            vmrr.state = constants.RES_STATE_SCHEDULED
            lease_req.appendRR(vmrr)

            # Schedule deployment overhead
            self.deployment.schedule(lease_req, vmrr, nexttime)
            
            # Commit reservation to slot table
            # (we don't do this until the very end because the deployment overhead
            # scheduling could still throw an exception)
            self.slottable.addReservation(vmrr)
        except SlotFittingException, msg:
            raise SchedException, "The requested AR lease is infeasible. Reason: %s" % msg

    def schedule_besteffort_lease(self, req, nexttime):
        # Determine earliest start time in each node
        if req.state == constants.LEASE_STATE_PENDING:
            # Figure out earliest start times based on
            # image schedule and reusable images
            earliest = self.deployment.find_earliest_starting_times(req, nexttime)
        elif req.state == constants.LEASE_STATE_SUSPENDED:
            # No need to transfer images from repository
            # (only intra-node transfer)
            earliest = dict([(node+1, [nexttime, constants.REQTRANSFER_NO, None]) for node in range(req.numnodes)])
            
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
            
            self.deployment.schedule(req, vmrr, nexttime)
            
            # Schedule image transfers
            if req.state == constants.LEASE_STATE_SUSPENDED:
                # TODO: This would be more correctly handled in the RR handle functions.
                # Update VM image mappings, since we might be resuming
                # in different nodes.
                for vnode, pnode in req.vmimagemap.items():
                    self.rm.resourcepool.removeImage(pnode, req.leaseID, vnode)
                req.vmimagemap = vmrr.nodes
                for vnode, pnode in req.vmimagemap.items():
                    self.rm.resourcepool.addTaintedImageToNode(pnode, req.diskImageID, req.diskImageSize, req.leaseID, vnode)
                # Update RAM file mappings
                for vnode, pnode in req.memimagemap.items():
                    self.rm.resourcepool.removeRAMFileFromNode(pnode, req.leaseID, vnode)
                for vnode, pnode in vmrr.nodes.items():
                    self.rm.resourcepool.addRAMFileToNode(pnode, req.leaseID, vnode, req.resreq.getByType(constants.RES_MEM))
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
                
            req.printContents()
            
        except SlotFittingException, msg:
            raise SchedException, "The requested best-effort lease is infeasible. Reason: %s" % msg
        
    def preempt(self, req, time):
        self.rm.logger.info("Preempting lease #%i..." % (req.leaseID), constants.SCHED)
        self.rm.logger.edebug("Lease before preemption:", constants.SCHED)
        req.printContents()
        vmrr, susprr  = req.getLastVMRR()
        suspendresumerate = self.rm.resourcepool.info.getSuspendResumeRate()
        
        if vmrr.state == constants.RES_STATE_SCHEDULED and vmrr.start >= time:
            self.rm.logger.info("... lease #%i has been cancelled and requeued." % req.leaseID, constants.SCHED)
            self.rm.logger.debug("Lease was set to start in the middle of the preempting lease.", constants.SCHED)
            req.state = constants.LEASE_STATE_PENDING
            if vmrr.backfillres == True:
                self.numbesteffortres -= 1
            req.removeRR(vmrr)
            self.slottable.removeReservation(vmrr)
            if susprr != None:
                req.removeRR(susprr)
                self.slottable.removeReservation(susprr)
            for vnode, pnode in req.vmimagemap.items():
                self.rm.resourcepool.removeImage(pnode, req.leaseID, vnode)
            self.deployment.cancel_deployment(req)
            req.vmimagemap = {}
            self.scheduledleases.remove(req)
            self.queue.enqueueInOrder(req)
            self.rm.stats.incrCounter(constants.COUNTER_QUEUESIZE, req.leaseID)
        else:
            susptype = self.rm.config.getSuspensionType()
            timebeforesuspend = time - vmrr.start
            # TODO: Determine if it is in fact the initial VMRR or not. Right now
            # we conservatively overestimate
            canmigrate = self.rm.config.isMigrationAllowed()
            suspendthreshold = req.getSuspendThreshold(initial=False, suspendrate=suspendresumerate, migrating=canmigrate)
            # We can't suspend if we're under the suspend threshold
            suspendable = timebeforesuspend >= suspendthreshold
            if suspendable and (susptype == constants.SUSPENSION_ALL or (req.numnodes == 1 and susptype == constants.SUSPENSION_SERIAL)):
                self.rm.logger.info("... lease #%i will be suspended at %s." % (req.leaseID, time), constants.SCHED)
                self.slottable.suspend(req, time)
            else:
                self.rm.logger.info("... lease #%i has been cancelled and requeued (cannot be suspended)" % req.leaseID, constants.SCHED)
                req.state = constants.LEASE_STATE_PENDING
                if vmrr.backfillres == True:
                    self.numbesteffortres -= 1
                req.removeRR(vmrr)
                self.slottable.removeReservation(vmrr)
                if susprr != None:
                    req.removeRR(susprr)
                    self.slottable.removeReservation(susprr)
                if req.state == constants.LEASE_STATE_SUSPENDED:
                    resmrr = req.prevRR(vmrr)
                    req.removeRR(resmrr)
                    self.slottable.removeReservation(resmrr)
                for vnode, pnode in req.vmimagemap.items():
                    self.rm.resourcepool.removeImage(pnode, req.leaseID, vnode)
                self.deployment.cancel_deployment(req)
                req.vmimagemap = {}
                self.scheduledleases.remove(req)
                self.queue.enqueueInOrder(req)
                self.rm.stats.incrCounter(constants.COUNTER_QUEUESIZE, req.leaseID)
        self.rm.logger.edebug("Lease after preemption:", constants.SCHED)
        req.printContents()
        
    def reevaluateSchedule(self, endinglease, nodes, nexttime, checkedleases):
        self.rm.logger.debug("Reevaluating schedule. Checking for leases scheduled in nodes %s after %s" %(nodes, nexttime), constants.SCHED) 
        leases = self.scheduledleases.getNextLeasesScheduledInNodes(nexttime, nodes)
        leases = [l for l in leases if isinstance(l, ds.BestEffortLease) and not l in checkedleases]
        for l in leases:
            self.rm.logger.debug("Found lease %i" % l.leaseID, constants.SCHED)
            l.printContents()
            # Earliest time can't be earlier than time when images will be
            # available in node
            earliest = max(nexttime, l.imagesavail)
            self.slottable.slideback(l, earliest)
            checkedleases.append(l)
        #for l in leases:
        #    vmrr, susprr = l.getLastVMRR()
        #    self.reevaluateSchedule(l, vmrr.nodes.values(), vmrr.end, checkedleases)
          
            

    #-------------------------------------------------------------------#
    #                                                                   #
    #                  SLOT TABLE EVENT HANDLERS                        #
    #                                                                   #
    #-------------------------------------------------------------------#

    def _handle_start_vm(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleStartVM" % l.leaseID, constants.SCHED)
        l.printContents()
        if l.state == constants.LEASE_STATE_DEPLOYED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            
            try:
                self.rm.resourcepool.startVMs(l, rr)
                # The next two lines have to be moved somewhere more
                # appropriate inside the resourcepool module
                for (vnode, pnode) in rr.nodes.items():
                    l.vmimagemap[vnode] = pnode
            except Exception, e:
                self.rm.logger.error("ERROR when starting VMs.", constants.SCHED)
                raise

        elif l.state == constants.LEASE_STATE_SUSPENDED:
            l.state = constants.LEASE_STATE_ACTIVE
            rr.state = constants.RES_STATE_ACTIVE
            # No enactment to do here, since all the suspend/resume actions are
            # handled during the suspend/resume RRs
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RUN)
        self.rm.logger.debug("LEASE-%i End of handleStartVM" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Started VMs for lease %i on nodes %s" % (l.leaseID, rr.nodes.values()), constants.SCHED)

    def _handle_end_vm(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleEndVM" % l.leaseID, constants.SCHED)
        self.rm.logger.edebug("LEASE-%i Before:" % l.leaseID, constants.SCHED)
        l.printContents()
        diff = self.rm.clock.get_time() - rr.start
        l.duration.accumulateDuration(diff)
        rr.state = constants.RES_STATE_DONE
        if rr.oncomplete == constants.ONCOMPLETE_ENDLEASE:
            self.rm.resourcepool.stopVMs(l, rr)
            l.state = constants.LEASE_STATE_DONE
            l.duration.actual = l.duration.accumulated
            self.completedleases.add(l)
            self.scheduledleases.remove(l)
            for vnode, pnode in l.vmimagemap.items():
                self.rm.resourcepool.removeImage(pnode, l.leaseID, vnode)
            if isinstance(l, ds.BestEffortLease):
                self.rm.stats.incrCounter(constants.COUNTER_BESTEFFORTCOMPLETED, l.leaseID)
       
        if isinstance(l, ds.BestEffortLease):
            if rr.backfillres == True:
                self.numbesteffortres -= 1
        self.rm.logger.edebug("LEASE-%i After:" % l.leaseID, constants.SCHED)
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE)
        self.rm.logger.debug("LEASE-%i End of handleEndVM" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Stopped VMs for lease %i on nodes %s" % (l.leaseID, rr.nodes.values()), constants.SCHED)

    def _handle_start_suspend(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleStartSuspend" % l.leaseID, constants.SCHED)
        l.printContents()
        rr.state = constants.RES_STATE_ACTIVE
        self.rm.resourcepool.suspendVMs(l, rr)
        for vnode, pnode in rr.nodes.items():
            self.rm.resourcepool.addRAMFileToNode(pnode, l.leaseID, vnode, l.resreq.getByType(constants.RES_MEM))
            l.memimagemap[vnode] = pnode
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_SUSPEND)
        self.rm.logger.debug("LEASE-%i End of handleStartSuspend" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Suspending lease %i..." % (l.leaseID), constants.SCHED)

    def _handle_end_suspend(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleEndSuspend" % l.leaseID, constants.SCHED)
        l.printContents()
        # TODO: React to incomplete suspend
        self.rm.resourcepool.verifySuspend(l, rr)
        rr.state = constants.RES_STATE_DONE
        l.state = constants.LEASE_STATE_SUSPENDED
        self.scheduledleases.remove(l)
        self.queue.enqueueInOrder(l)
        self.rm.stats.incrCounter(constants.COUNTER_QUEUESIZE, l.leaseID)
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE)
        self.rm.logger.debug("LEASE-%i End of handleEndSuspend" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Lease %i suspended." % (l.leaseID), constants.SCHED)

    def _handle_start_resume(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleStartResume" % l.leaseID, constants.SCHED)
        l.printContents()
        self.rm.resourcepool.resumeVMs(l, rr)
        rr.state = constants.RES_STATE_ACTIVE
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_VM_RESUME)
        self.rm.logger.debug("LEASE-%i End of handleStartResume" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Resuming lease %i..." % (l.leaseID), constants.SCHED)

    def _handle_end_resume(self, l, rr):
        self.rm.logger.debug("LEASE-%i Start of handleEndResume" % l.leaseID, constants.SCHED)
        l.printContents()
        # TODO: React to incomplete resume
        self.rm.resourcepool.verifyResume(l, rr)
        rr.state = constants.RES_STATE_DONE
        for vnode, pnode in rr.nodes.items():
            self.rm.resourcepool.removeRAMFileFromNode(pnode, l.leaseID, vnode)
        l.printContents()
        self.updateNodeVMState(rr.nodes.values(), constants.DOING_IDLE)
        self.rm.logger.debug("LEASE-%i End of handleEndResume" % l.leaseID, constants.SCHED)
        self.rm.logger.info("Resumed lease %i" % (l.leaseID), constants.SCHED)

    def _handle_end_rr(self, l, rr):
        self.slottable.removeReservation(rr)



    def __enqueue_in_order(self, req):
        self.rm.stats.incrCounter(constants.COUNTER_QUEUESIZE, req.leaseID)
        self.queue.enqueueInOrder(req)

    
    
        
    def canReserveBestEffort(self):
        return self.numbesteffortres < self.maxres
    
                        
    def updateNodeVMState(self, nodes, state):
        for n in nodes:
            self.rm.resourcepool.getNode(n).vm_doing = state

    def updateNodeTransferState(self, nodes, state):
        for n in nodes:
            self.rm.resourcepool.getNode(n).transfer_doing = state
