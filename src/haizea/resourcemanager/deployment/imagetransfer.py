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

import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
from haizea.resourcemanager.deployment.base import DeploymentBase, DeploymentSchedException
from haizea.resourcemanager.datastruct import ResourceReservationBase, ARLease, BestEffortLease

import copy

class ImageTransferDeployment(DeploymentBase):
    def __init__(self, scheduler):
        DeploymentBase.__init__(self, scheduler)
        self.transfersEDF = []
        self.transfersFIFO = []
        self.completedTransfers = []
        
        self.scheduler.register_handler(type     = FileTransferResourceReservation, 
                                        on_start = ImageTransferDeployment.handle_start_filetransfer,
                                        on_end   = ImageTransferDeployment.handle_end_filetransfer)

    def schedule(self, lease, vmrr, nexttime):
        if isinstance(lease, ARLease):
            self.schedule_for_ar(lease, vmrr, nexttime)
        elif isinstance(lease, BestEffortLease):
            self.schedule_for_besteffort(lease, vmrr, nexttime)
            
    def cancel_deployment(self, lease):
        if isinstance(lease, BestEffortLease):
            self.__remove_from_fifo_transfers(lease.leaseID)
        
    def schedule_for_ar(self, lease, vmrr, nexttime):
        config = self.scheduler.rm.config
        mechanism = config.get_transfer_mechanism()
        reusealg = config.getReuseAlg()
        avoidredundant = config.isAvoidingRedundantTransfers()
        
        lease.state = constants.LEASE_STATE_SCHEDULED
        
        if avoidredundant:
            pass # TODO
            
        musttransfer = {}
        mustpool = {}
        nodeassignment = vmrr.nodes
        start = lease.start.requested
        end = lease.start.requested + lease.duration.requested
        for (vnode, pnode) in nodeassignment.items():
            leaseID = lease.leaseID
            self.logger.debug("Scheduling image transfer of '%s' from vnode %i to physnode %i" % (lease.diskImageID, vnode, pnode), constants.SCHED)

            if reusealg == constants.REUSE_IMAGECACHES:
                if self.resourcepool.isInPool(pnode, lease.diskImageID, start):
                    self.logger.debug("No need to schedule an image transfer (reusing an image in pool)", constants.SCHED)
                    mustpool[vnode] = pnode                            
                else:
                    self.logger.debug("Need to schedule a transfer.", constants.SCHED)
                    musttransfer[vnode] = pnode
            else:
                self.logger.debug("Need to schedule a transfer.", constants.SCHED)
                musttransfer[vnode] = pnode

        if len(musttransfer) == 0:
            lease.state = constants.LEASE_STATE_DEPLOYED
        else:
            if mechanism == constants.TRANSFER_UNICAST:
                # Dictionary of transfer RRs. Key is the physical node where
                # the image is being transferred to
                transferRRs = {}
                for vnode, pnode in musttransfer:
                    if transferRRs.has_key(pnode):
                        # We've already scheduled a transfer to this node. Reuse it.
                        self.logger.debug("No need to schedule an image transfer (reusing an existing transfer)", constants.SCHED)
                        transferRR = transferRRs[pnode]
                        transferRR.piggyback(leaseID, vnode, pnode, end)
                    else:
                        filetransfer = self.scheduleImageTransferEDF(lease, {vnode:pnode}, nexttime)                 
                        transferRRs[pnode] = filetransfer
                        lease.appendRR(filetransfer)
            elif mechanism == constants.TRANSFER_MULTICAST:
                filetransfer = self.scheduleImageTransferEDF(lease, musttransfer, nexttime)
                lease.appendRR(filetransfer)
 
        # No chance of scheduling exception at this point. It's safe
        # to add entries to the pools
        if reusealg == constants.REUSE_IMAGECACHES:
            for (vnode, pnode) in mustpool.items():
                self.resourcepool.addToPool(pnode, lease.diskImageID, leaseID, vnode, start)

    def schedule_for_besteffort(self, lease, vmrr, nexttime):
        config = self.scheduler.rm.config
        mechanism = config.get_transfer_mechanism()
        reusealg = config.getReuseAlg()
        avoidredundant = config.isAvoidingRedundantTransfers()
        earliest = self.find_earliest_starting_times(lease, nexttime)
        lease.state = constants.LEASE_STATE_SCHEDULED
        transferRRs = []
        musttransfer = {}
        piggybacking = []
        for (vnode, pnode) in vmrr.nodes.items():
            reqtransfer = earliest[pnode][1]
            if reqtransfer == constants.REQTRANSFER_COWPOOL:
                # Add to pool
                self.logger.debug("Reusing image for V%i->P%i." % (vnode, pnode), constants.SCHED)
                self.resourcepool.addToPool(pnode, lease.diskImageID, lease.leaseID, vnode, vmrr.end)
            elif reqtransfer == constants.REQTRANSFER_PIGGYBACK:
                # We can piggyback on an existing transfer
                transferRR = earliest[pnode][2]
                transferRR.piggyback(lease.leaseID, vnode, pnode)
                self.logger.debug("Piggybacking transfer for V%i->P%i on existing transfer in lease %i." % (vnode, pnode, transferRR.lease.leaseID), constants.SCHED)
                piggybacking.append(transferRR)
            else:
                # Transfer
                musttransfer[vnode] = pnode
                self.logger.debug("Must transfer V%i->P%i." % (vnode, pnode), constants.SCHED)
        if len(musttransfer)>0:
            transferRRs = self.scheduleImageTransferFIFO(lease, musttransfer, nexttime)
            endtransfer = transferRRs[-1].end
            lease.imagesavail = endtransfer
        else:
            # TODO: Not strictly correct. Should mark the lease
            # as deployed when piggybacked transfers have concluded
            lease.state = constants.LEASE_STATE_DEPLOYED
        if len(piggybacking) > 0: 
            endtimes = [t.end for t in piggybacking]
            if len(musttransfer) > 0:
                endtimes.append(endtransfer)
            lease.imagesavail = max(endtimes)
        if len(musttransfer)==0 and len(piggybacking)==0:
            lease.state = constants.LEASE_STATE_DEPLOYED
            lease.imagesavail = nexttime
        for rr in transferRRs:
            lease.appendRR(rr)
        

    def find_earliest_starting_times(self, lease_req, nexttime):
        nodIDs = [n.nod_id for n in self.resourcepool.getNodes()]  
        config = self.scheduler.rm.config
        mechanism = config.get_transfer_mechanism()       
        reusealg = config.getReuseAlg()
        avoidredundant = config.isAvoidingRedundantTransfers()
        
        # Figure out starting time assuming we have to transfer the image
        nextfifo = self.getNextFIFOTransferTime(nexttime)
        
        imgTransferTime=lease_req.estimateImageTransferTime()
        
        # Find worst-case earliest start time
        if lease_req.numnodes == 1:
            startTime = nextfifo + imgTransferTime
            earliest = dict([(node, [startTime, constants.REQTRANSFER_YES]) for node in nodIDs])                
        else:
            # Unlike the previous case, we may have to find a new start time
            # for all the nodes.
            if mechanism == constants.TRANSFER_UNICAST:
                pass
                # TODO: If transferring each image individually, this will
                # make determining what images can be reused more complicated.
            if mechanism == constants.TRANSFER_MULTICAST:
                startTime = nextfifo + imgTransferTime
                earliest = dict([(node, [startTime, constants.REQTRANSFER_YES]) for node in nodIDs])                                    # TODO: Take into account reusable images
        
        # Check if we can reuse images
        if reusealg==constants.REUSE_IMAGECACHES:
            nodeswithimg = self.resourcepool.getNodesWithImgInPool(lease_req.diskImageID)
            for node in nodeswithimg:
                earliest[node] = [nexttime, constants.REQTRANSFER_COWPOOL]
        
                
        # Check if we can avoid redundant transfers
        if avoidredundant:
            if mechanism == constants.TRANSFER_UNICAST:
                pass
                # TODO
            if mechanism == constants.TRANSFER_MULTICAST:                
                # We can only piggyback on transfers that haven't started yet
                transfers = [t for t in self.transfersFIFO if t.state == constants.RES_STATE_SCHEDULED]
                for t in transfers:
                    if t.file == lease_req.diskImageID:
                        startTime = t.end
                        if startTime > nexttime:
                            for n in earliest:
                                if startTime < earliest[n]:
                                    earliest[n] = [startTime, constants.REQTRANSFER_PIGGYBACK, t]

        return earliest

    def scheduleImageTransferEDF(self, req, vnodes, nexttime):
        # Estimate image transfer time 
        imgTransferTime=req.estimateImageTransferTime()
        bandwidth = self.resourcepool.imagenode_bandwidth

        # Determine start time
        activetransfers = [t for t in self.transfersEDF if t.state == constants.RES_STATE_ACTIVE]
        if len(activetransfers) > 0:
            startTime = activetransfers[-1].end
        else:
            startTime = nexttime
        
        transfermap = dict([(copy.copy(t), t) for t in self.transfersEDF if t.state == constants.RES_STATE_SCHEDULED])
        newtransfers = transfermap.keys()
        
        res = {}
        resimgnode = ds.ResourceTuple.createEmpty()
        resimgnode.setByType(constants.RES_NETOUT, bandwidth)
        resnode = ds.ResourceTuple.createEmpty()
        resnode.setByType(constants.RES_NETIN, bandwidth)
        res[self.slottable.EDFnode] = resimgnode
        for n in vnodes.values():
            res[n] = resnode
        
        newtransfer = FileTransferResourceReservation(req, res)
        newtransfer.deadline = req.start.requested
        newtransfer.state = constants.RES_STATE_SCHEDULED
        newtransfer.file = req.diskImageID
        for vnode, pnode in vnodes.items():
            newtransfer.piggyback(req.leaseID, vnode, pnode)
        newtransfers.append(newtransfer)

        def comparedates(x, y):
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
             raise DeploymentSchedException, "Adding this VW results in an unfeasible image transfer schedule."

        # Push image transfers as close as possible to their deadlines. 
        feasibleEndTime=newtransfers[-1].deadline
        for t in reversed(newtransfers):
            if t == newtransfer:
                duration = imgTransferTime
            else:
                duration = t.end - t.start
    
            newEndTime=min([t.deadline, feasibleEndTime])
            t.end=newEndTime
            newStartTime=newEndTime-duration
            t.start=newStartTime
            feasibleEndTime=newStartTime
        
        # Make changes   
        for t in newtransfers:
            if t == newtransfer:
                self.slottable.addReservation(t)
                self.transfersEDF.append(t)
            else:
                tOld = transfermap[t]
                self.transfersEDF.remove(tOld)
                self.transfersEDF.append(t)
                self.slottable.updateReservationWithKeyChange(tOld, t)
        
        return newtransfer
    
    def scheduleImageTransferFIFO(self, req, reqtransfers, nexttime):
        # Estimate image transfer time 
        imgTransferTime=req.estimateImageTransferTime()
        config = self.scheduler.rm.config
        mechanism = config.get_transfer_mechanism()  
        startTime = self.getNextFIFOTransferTime(nexttime)
        bandwidth = self.resourcepool.imagenode_bandwidth
        
        newtransfers = []
        
        if mechanism == constants.TRANSFER_UNICAST:
            pass
            # TODO: If transferring each image individually, this will
            # make determining what images can be reused more complicated.
        if mechanism == constants.TRANSFER_MULTICAST:
            # Time to transfer is imagesize / bandwidth, regardless of 
            # number of nodes
            res = {}
            resimgnode = ds.ResourceTuple.createEmpty()
            resimgnode.setByType(constants.RES_NETOUT, bandwidth)
            resnode = ds.ResourceTuple.createEmpty()
            resnode.setByType(constants.RES_NETIN, bandwidth)
            res[self.slottable.FIFOnode] = resimgnode
            for n in reqtransfers.values():
                res[n] = resnode
            newtransfer = FileTransferResourceReservation(req, res)
            newtransfer.start = startTime
            newtransfer.end = startTime+imgTransferTime
            newtransfer.deadline = None
            newtransfer.state = constants.RES_STATE_SCHEDULED
            newtransfer.file = req.diskImageID
            for vnode in reqtransfers:
                physnode = reqtransfers[vnode]
                newtransfer.piggyback(req.leaseID, vnode, physnode)
            self.slottable.addReservation(newtransfer)
            newtransfers.append(newtransfer)
            
        self.transfersFIFO += newtransfers
        
        return newtransfers
    
    def getNextFIFOTransferTime(self, nexttime):
        transfers = [t for t in self.transfersFIFO if t.state != constants.RES_STATE_DONE]
        if len(transfers) > 0:
            startTime = transfers[-1].end
        else:
            startTime = nexttime
        return startTime

    def __remove_from_fifo_transfers(self, leaseID):
        transfers = [t for t in self.transfersFIFO if t.state != constants.RES_STATE_DONE]
        toremove = []
        for t in transfers:
            for pnode in t.transfers:
                leases = [l for l, v in t.transfers[pnode]]
                if leaseID in leases:
                    newtransfers = [(l, v) for l, v in t.transfers[pnode] if l!=leaseID]
                    t.transfers[pnode] = newtransfers
            # Check if the transfer has to be cancelled
            a = sum([len(l) for l in t.transfers.values()])
            if a == 0:
                t.lease.removeRR(t)
                self.slottable.removeReservation(t)
                toremove.append(t)
        for t in toremove:
            self.transfersFIFO.remove(t)

    @staticmethod
    def handle_start_filetransfer(sched, lease, rr):
        sched.rm.logger.debug("LEASE-%i Start of handleStartFileTransfer" % lease.leaseID, constants.SCHED)
        lease.printContents()
        if lease.state == constants.LEASE_STATE_SCHEDULED or lease.state == constants.LEASE_STATE_DEPLOYED:
            lease.state = constants.LEASE_STATE_DEPLOYING
            rr.state = constants.RES_STATE_ACTIVE
            # TODO: Enactment
        elif lease.state == constants.LEASE_STATE_SUSPENDED:
            pass # This shouldn't happen
        lease.printContents()
        sched.updateNodeTransferState(rr.transfers.keys(), constants.DOING_TRANSFER)
        sched.logger.debug("LEASE-%i End of handleStartFileTransfer" % lease.leaseID, constants.SCHED)
        sched.logger.info("Starting image transfer for lease %i" % (lease.leaseID), constants.SCHED)

    @staticmethod
    def handle_end_filetransfer(sched, lease, rr):
        sched.rm.logger.debug("LEASE-%i Start of handleEndFileTransfer" % lease.leaseID, constants.SCHED)
        lease.printContents()
        if lease.state == constants.LEASE_STATE_DEPLOYING:
            lease.state = constants.LEASE_STATE_DEPLOYED
            rr.state = constants.RES_STATE_DONE
            for physnode in rr.transfers:
                vnodes = rr.transfers[physnode]
                
                # Update VM Image maps
                for leaseID, v in vnodes:
                    lease = sched.scheduledleases.getLease(leaseID)
                    lease.vmimagemap[v] = physnode
                    
                # Find out timeout of image. It will be the latest end time of all the
                # leases being used by that image.
                leases = [l for (l, v) in vnodes]
                maxend=None
                for leaseID in leases:
                    l = sched.scheduledleases.getLease(leaseID)
                    end = lease.getEnd()
                    if maxend==None or end>maxend:
                        maxend=end
                # TODO: ENACTMENT: Verify the image was transferred correctly
                sched.rm.resourcepool.addImageToNode(physnode, rr.file, lease.diskImageSize, vnodes, timeout=maxend)
        elif lease.state == constants.LEASE_STATE_SUSPENDED:
            pass
            # TODO: Migrating
        lease.printContents()
        sched.updateNodeTransferState(rr.transfers.keys(), constants.DOING_IDLE)
        sched.rm.logger.debug("LEASE-%i End of handleEndFileTransfer" % lease.leaseID, constants.SCHED)
        sched.rm.logger.info("Completed image transfer for lease %i" % (lease.leaseID), constants.SCHED)

class FileTransferResourceReservation(ResourceReservationBase):
    def __init__(self, lease, res, start=None, end=None):
        ResourceReservationBase.__init__(self, lease, start, end, res)
        self.deadline = None
        self.file = None
        # Dictionary of  physnode -> [ (leaseID, vnode)* ]
        self.transfers = {}

    def printContents(self, loglevel="EXTREMEDEBUG"):
        ResourceReservationBase.printContents(self, loglevel)
        self.logger.log(loglevel, "Type           : FILE TRANSFER", constants.DS)
        self.logger.log(loglevel, "Deadline       : %s" % self.deadline, constants.DS)
        self.logger.log(loglevel, "File           : %s" % self.file, constants.DS)
        self.logger.log(loglevel, "Transfers      : %s" % self.transfers, constants.DS)
        
    def piggyback(self, leaseID, vnode, physnode):
        if self.transfers.has_key(physnode):
            self.transfers[physnode].append((leaseID, vnode))
        else:
            self.transfers[physnode] = [(leaseID, vnode)]
            
    def isPreemptible(self):
        return False       