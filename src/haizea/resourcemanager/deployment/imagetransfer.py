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
from haizea.resourcemanager.datastruct import ResourceReservation, Lease, ARLease, BestEffortLease
from haizea.common.utils import estimate_transfer_time

import copy

class ImageTransferDeployment(DeploymentBase):
    def __init__(self, scheduler):
        DeploymentBase.__init__(self, scheduler)
        
        # TODO: The following two should be merged into
        # something like this:
        #    self.imageNode = self.info.getImageNode()
        self.fifo_node = self.resourcepool.deployment.get_fifo_node()
        self.edf_node = self.resourcepool.deployment.get_edf_node()
        
        self.transfers_edf = []
        self.transfers_fifo = []
        self.completed_transfers = []

        config = self.scheduler.rm.config
        self.reusealg = config.get("diskimage-reuse")
        if self.reusealg == constants.REUSE_IMAGECACHES:
            self.maxcachesize = config.get("diskimage-cache-size")
        else:
            self.maxcachesize = None
        
        self.imagenode_bandwidth = self.resourcepool.deployment.get_bandwidth()
        
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
            self.__remove_from_fifo_transfers(lease.id)
        
    def schedule_for_ar(self, lease, vmrr, nexttime):
        config = self.scheduler.rm.config
        mechanism = config.get("transfer-mechanism")
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")
        
        lease.state = Lease.STATE_SCHEDULED
        
        if avoidredundant:
            pass # TODO
            
        musttransfer = {}
        mustpool = {}
        nodeassignment = vmrr.nodes
        start = lease.start.requested
        end = lease.start.requested + lease.duration.requested
        for (vnode, pnode) in nodeassignment.items():
            lease_id = lease.id
            self.logger.debug("Scheduling image transfer of '%s' from vnode %i to physnode %i" % (lease.diskimage_id, vnode, pnode))

            if reusealg == constants.REUSE_IMAGECACHES:
                if self.resourcepool.exists_reusable_image(pnode, lease.diskimage_id, start):
                    self.logger.debug("No need to schedule an image transfer (reusing an image in pool)")
                    mustpool[vnode] = pnode                            
                else:
                    self.logger.debug("Need to schedule a transfer.")
                    musttransfer[vnode] = pnode
            else:
                self.logger.debug("Need to schedule a transfer.")
                musttransfer[vnode] = pnode

        if len(musttransfer) == 0:
            lease.state = Lease.STATE_READY
        else:
            if mechanism == constants.TRANSFER_UNICAST:
                # Dictionary of transfer RRs. Key is the physical node where
                # the image is being transferred to
                transferRRs = {}
                for vnode, pnode in musttransfer:
                    if transferRRs.has_key(pnode):
                        # We've already scheduled a transfer to this node. Reuse it.
                        self.logger.debug("No need to schedule an image transfer (reusing an existing transfer)")
                        transferRR = transferRRs[pnode]
                        transferRR.piggyback(lease_id, vnode, pnode, end)
                    else:
                        filetransfer = self.schedule_imagetransfer_edf(lease, {vnode:pnode}, nexttime)                 
                        transferRRs[pnode] = filetransfer
                        lease.appendRR(filetransfer)
            elif mechanism == constants.TRANSFER_MULTICAST:
                filetransfer = self.schedule_imagetransfer_edf(lease, musttransfer, nexttime)
                lease.append_rr(filetransfer)
 
        # No chance of scheduling exception at this point. It's safe
        # to add entries to the pools
        if reusealg == constants.REUSE_IMAGECACHES:
            for (vnode, pnode) in mustpool.items():
                self.resourcepool.add_mapping_to_existing_reusable_image(pnode, lease.diskimage_id, lease.id, vnode, start)

    def schedule_for_besteffort(self, lease, vmrr, nexttime):
        config = self.scheduler.rm.config
        mechanism = config.get("transfer-mechanism")
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")
        earliest = self.find_earliest_starting_times(lease, nexttime)
        lease.state = Lease.STATE_SCHEDULED
        transferRRs = []
        musttransfer = {}
        piggybacking = []
        for (vnode, pnode) in vmrr.nodes.items():
            reqtransfer = earliest[pnode][1]
            if reqtransfer == constants.REQTRANSFER_COWPOOL:
                # Add to pool
                self.logger.debug("Reusing image for V%i->P%i." % (vnode, pnode))
                self.resourcepool.add_mapping_to_existing_reusable_image(pnode, lease.diskimage_id, lease.id, vnode, vmrr.end)
            elif reqtransfer == constants.REQTRANSFER_PIGGYBACK:
                # We can piggyback on an existing transfer
                transferRR = earliest[pnode][2]
                transferRR.piggyback(lease.id, vnode, pnode)
                self.logger.debug("Piggybacking transfer for V%i->P%i on existing transfer in lease %i." % (vnode, pnode, transferRR.lease.id))
                piggybacking.append(transferRR)
            else:
                # Transfer
                musttransfer[vnode] = pnode
                self.logger.debug("Must transfer V%i->P%i." % (vnode, pnode))
        if len(musttransfer)>0:
            transferRRs = self.schedule_imagetransfer_fifo(lease, musttransfer, nexttime)
            endtransfer = transferRRs[-1].end
            lease.imagesavail = endtransfer
        else:
            # TODO: Not strictly correct. Should mark the lease
            # as deployed when piggybacked transfers have concluded
            lease.state = Lease.STATE_READY
        if len(piggybacking) > 0: 
            endtimes = [t.end for t in piggybacking]
            if len(musttransfer) > 0:
                endtimes.append(endtransfer)
            lease.imagesavail = max(endtimes)
        if len(musttransfer)==0 and len(piggybacking)==0:
            lease.state = Lease.STATE_READY
            lease.imagesavail = nexttime
        for rr in transferRRs:
            lease.append_rr(rr)
        

    def find_earliest_starting_times(self, lease_req, nexttime):
        nodIDs = [n.nod_id for n in self.resourcepool.get_nodes()]  
        config = self.scheduler.rm.config
        mechanism = config.get("transfer-mechanism")
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")
        
        # Figure out starting time assuming we have to transfer the image
        nextfifo = self.get_next_fifo_transfer_time(nexttime)
        
        imgTransferTime=self.estimate_image_transfer_time(lease_req, self.imagenode_bandwidth)
        
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
            nodeswithimg = self.resourcepool.get_nodes_with_reusable_image(lease_req.diskimage_id)
            for node in nodeswithimg:
                earliest[node] = [nexttime, constants.REQTRANSFER_COWPOOL]
        
                
        # Check if we can avoid redundant transfers
        if avoidredundant:
            if mechanism == constants.TRANSFER_UNICAST:
                pass
                # TODO
            if mechanism == constants.TRANSFER_MULTICAST:                
                # We can only piggyback on transfers that haven't started yet
                transfers = [t for t in self.transfers_fifo if t.state == ResourceReservation.STATE_SCHEDULED]
                for t in transfers:
                    if t.file == lease_req.diskImageID:
                        startTime = t.end
                        if startTime > nexttime:
                            for n in earliest:
                                if startTime < earliest[n]:
                                    earliest[n] = [startTime, constants.REQTRANSFER_PIGGYBACK, t]

        return earliest

    def schedule_imagetransfer_edf(self, req, vnodes, nexttime):
        # Estimate image transfer time 
        bandwidth = self.resourcepool.deployment.get_bandwidth()
        imgTransferTime=self.estimate_image_transfer_time(req, bandwidth)

        # Determine start time
        activetransfers = [t for t in self.transfers_edf if t.state == ResourceReservation.STATE_ACTIVE]
        if len(activetransfers) > 0:
            startTime = activetransfers[-1].end
        else:
            startTime = nexttime
        
        transfermap = dict([(copy.copy(t), t) for t in self.transfers_edf if t.state == ResourceReservation.STATE_SCHEDULED])
        newtransfers = transfermap.keys()
        
        res = {}
        resimgnode = ds.ResourceTuple.create_empty()
        resimgnode.set_by_type(constants.RES_NETOUT, bandwidth)
        resnode = ds.ResourceTuple.create_empty()
        resnode.set_by_type(constants.RES_NETIN, bandwidth)
        res[self.edf_node.nod_id] = resimgnode
        for n in vnodes.values():
            res[n] = resnode
        
        newtransfer = FileTransferResourceReservation(req, res)
        newtransfer.deadline = req.start.requested
        newtransfer.state = ResourceReservation.STATE_SCHEDULED
        newtransfer.file = req.diskimage_id
        for vnode, pnode in vnodes.items():
            newtransfer.piggyback(req.id, vnode, pnode)
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
                self.transfers_edf.append(t)
            else:
                tOld = transfermap[t]
                self.transfers_edf.remove(tOld)
                self.transfers_edf.append(t)
                self.slottable.updateReservationWithKeyChange(tOld, t)
        
        return newtransfer
    
    def schedule_imagetransfer_fifo(self, req, reqtransfers, nexttime):
        # Estimate image transfer time 
        bandwidth = self.imagenode_bandwidth
        imgTransferTime=self.estimate_image_transfer_time(req, bandwidth)
        config = self.scheduler.rm.config
        mechanism = config.get("transfer-mechanism")
        startTime = self.get_next_fifo_transfer_time(nexttime)
        
        newtransfers = []
        
        if mechanism == constants.TRANSFER_UNICAST:
            pass
            # TODO: If transferring each image individually, this will
            # make determining what images can be reused more complicated.
        if mechanism == constants.TRANSFER_MULTICAST:
            # Time to transfer is imagesize / bandwidth, regardless of 
            # number of nodes
            res = {}
            resimgnode = ds.ResourceTuple.create_empty()
            resimgnode.set_by_type(constants.RES_NETOUT, bandwidth)
            resnode = ds.ResourceTuple.create_empty()
            resnode.set_by_type(constants.RES_NETIN, bandwidth)
            res[self.fifo_node.nod_id] = resimgnode
            for n in reqtransfers.values():
                res[n] = resnode
            newtransfer = FileTransferResourceReservation(req, res)
            newtransfer.start = startTime
            newtransfer.end = startTime+imgTransferTime
            newtransfer.deadline = None
            newtransfer.state = ResourceReservation.STATE_SCHEDULED
            newtransfer.file = req.diskimage_id
            for vnode in reqtransfers:
                physnode = reqtransfers[vnode]
                newtransfer.piggyback(req.id, vnode, physnode)
            self.slottable.addReservation(newtransfer)
            newtransfers.append(newtransfer)
            
        self.transfers_fifo += newtransfers
        
        return newtransfers
    
    def estimate_image_transfer_time(self, lease, bandwidth):
        from haizea.resourcemanager.rm import ResourceManager
        config = ResourceManager.get_singleton().config
        forceTransferTime = config.get("force-imagetransfer-time")
        if forceTransferTime != None:
            return forceTransferTime
        else:      
            return estimate_transfer_time(lease.diskimage_size, bandwidth)    
    
    def get_next_fifo_transfer_time(self, nexttime):
        transfers = [t for t in self.transfers_fifo if t.state != ResourceReservation.STATE_DONE]
        if len(transfers) > 0:
            startTime = transfers[-1].end
        else:
            startTime = nexttime
        return startTime

    def __remove_from_fifo_transfers(self, lease_id):
        transfers = [t for t in self.transfers_fifo if t.state != ResourceReservation.STATE_DONE]
        toremove = []
        for t in transfers:
            for pnode in t.transfers:
                leases = [l for l, v in t.transfers[pnode]]
                if lease_id in leases:
                    newtransfers = [(l, v) for l, v in t.transfers[pnode] if l!=lease_id]
                    t.transfers[pnode] = newtransfers
            # Check if the transfer has to be cancelled
            a = sum([len(l) for l in t.transfers.values()])
            if a == 0:
                t.lease.removeRR(t)
                self.slottable.removeReservation(t)
                toremove.append(t)
        for t in toremove:
            self.transfers_fifo.remove(t)

    @staticmethod
    def handle_start_filetransfer(sched, lease, rr):
        sched.rm.logger.debug("LEASE-%i Start of handleStartFileTransfer" % lease.id)
        lease.print_contents()
        if lease.state == Lease.STATE_SCHEDULED or lease.state == Lease.STATE_READY:
            lease.state = Lease.STATE_PREPARING
            rr.state = ResourceReservation.STATE_ACTIVE
            # TODO: Enactment
        lease.print_contents()
        sched.updateNodeTransferState(rr.transfers.keys(), constants.DOING_TRANSFER, lease.id)
        sched.logger.debug("LEASE-%i End of handleStartFileTransfer" % lease.id)
        sched.logger.info("Starting image transfer for lease %i" % (lease.id))

    @staticmethod
    def handle_end_filetransfer(sched, lease, rr):
        sched.rm.logger.debug("LEASE-%i Start of handleEndFileTransfer" % lease.id)
        lease.print_contents()
        if lease.state == Lease.STATE_PREPARING:
            lease.state = Lease.STATE_READY
            rr.state = ResourceReservation.STATE_DONE
            for physnode in rr.transfers:
                vnodes = rr.transfers[physnode]
                
                # Update VM Image maps
                for lease_id, v in vnodes:
                    lease = sched.scheduledleases.get_lease(lease_id)
                    lease.vmimagemap[v] = physnode
                    
                # Find out timeout of image. It will be the latest end time of all the
                # leases being used by that image.
                leases = [l for (l, v) in vnodes]
                maxend=None
                for lease_id in leases:
                    l = sched.scheduledleases.get_lease(lease_id)
                    end = lease.get_endtime()
                    if maxend==None or end>maxend:
                        maxend=end
                # TODO: ENACTMENT: Verify the image was transferred correctly
                sched.deployment.add_diskimages(physnode, rr.file, lease.diskimage_size, vnodes, timeout=maxend)

        lease.print_contents()
        sched.updateNodeTransferState(rr.transfers.keys(), constants.DOING_IDLE, lease.id)
        sched.rm.logger.debug("LEASE-%i End of handleEndFileTransfer" % lease.id)
        sched.rm.logger.info("Completed image transfer for lease %i" % (lease.id))
        
    def add_diskimages(self, pnode_id, diskimage_id, diskimage_size, vnodes, timeout):
        self.logger.debug("Adding image for leases=%s in nod_id=%i" % (vnodes, pnode_id))

        pnode = self.resourcepool.get_node(pnode_id)

        if self.reusealg == constants.REUSE_NONE:
            for (lease_id, vnode) in vnodes:
                self.resourcepool.add_diskimage(pnode_id, diskimage_id, diskimage_size, lease_id, vnode)
        elif self.reusealg == constants.REUSE_IMAGECACHES:
            # Sometimes we might find that the image is already deployed
            # (although unused). In that case, don't add another copy to
            # the pool. Just "reactivate" it.
            if pnode.exists_reusable_image(diskimage_id):
                for (lease_id, vnode) in vnodes:
                    pnode.add_mapping_to_existing_reusable_image(diskimage_id, lease_id, vnode, timeout)
            else:
                if self.maxcachesize == constants.CACHESIZE_UNLIMITED:
                    can_add_to_cache = True
                else:
                    # We may have to remove images from the cache
                    cachesize = pnode.get_reusable_images_size()
                    reqsize = cachesize + diskimage_size
                    if reqsize > self.maxcachesize:
                        # Have to shrink cache
                        desiredsize = self.maxcachesize - diskimage_size
                        self.logger.debug("Adding the image would make the size of pool in node %i = %iMB. Will try to bring it down to %i" % (pnode_id, reqsize, desiredsize))
                        pnode.print_files()
                        success = pnode.purge_downto(self.maxcachesize)
                        if not success:
                            can_add_to_cache = False
                        else:
                            can_add_to_cache = True
                    else:
                        can_add_to_cache = True
                        
                if can_add_to_cache:
                    self.resourcepool.add_reusable_image(pnode_id, diskimage_id, diskimage_size, vnodes, timeout)
                else:
                    # This just means we couldn't add the image
                    # to the pool. We will have to create disk images to be used
                    # only by these leases
                    self.logger.debug("Unable to add to pool. Must create individual disk images instead.")
                    for (lease_id, vnode) in vnodes:
                        self.resourcepool.add_diskimage(pnode_id, diskimage_id, diskimage_size, lease_id, vnode)
                    
        pnode.print_files()
        
        
    def check(self, lease, vmrr):
        # Check that all the required disk images are available.
        # Note that it is the enactment module's responsibility to
        # mark an image as correctly deployed. The check we do here
        # is (1) to catch scheduling errors (i.e., the image transfer
        # was not scheduled) and (2) to create disk images if
        # we can reuse a reusable image in the node'.
        # TODO: However, we're assuming CoW, which means the enactment
        # must support it too. If we can't assume CoW, we would have to
        # make a copy of the master image (which takes time), and should
        # be scheduled.
        
        for (vnode, pnode_id) in vmrr.nodes.items():
            pnode = self.resourcepool.get_node(pnode_id)
            
            diskimage = pnode.get_diskimage(lease.id, vnode, lease.diskimage_id)
            if self.reusealg == constants.REUSE_NONE:
                if diskimage == None:
                    raise Exception, "ERROR: No image for L%iV%i is on node %i" % (lease.id, vnode, pnode)
            elif self.reusealg == constants.REUSE_IMAGECACHES:
                reusable_image = pnode.get_reusable_image(lease.diskimage_id, lease_id=lease.id, vnode=vnode)
                if reusable_image == None:
                    # Not necessarily an error. Maybe the pool was full, and
                    # we had to fall back on creating a tainted image right
                    # when the image was transferred. We have to check this.
                    if diskimage == None:
                        raise Exception, "ERROR: Image for L%iV%i is not in pool on node %i, and there is no tainted image" % (lease.id, vnode, pnode_id)
                else:
                    # Create tainted image
                    self.resourcepool.add_diskimage(pnode_id, lease.diskimage_id, lease.diskimage_size, lease.id, vnode)
                    # ENACTMENT
                    # self.storage.createCopyFromCache(pnode, lease.diskImageSize)

    def cleanup(self, lease, vmrr):
        for vnode, pnode in lease.vmimagemap.items():
            self.resourcepool.remove_diskimage(pnode, lease.id, vnode)

class FileTransferResourceReservation(ResourceReservation):
    def __init__(self, lease, res, start=None, end=None):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.deadline = None
        self.file = None
        # Dictionary of  physnode -> [ (lease_id, vnode)* ]
        self.transfers = {}

    def print_contents(self, loglevel="VDEBUG"):
        ResourceReservation.print_contents(self, loglevel)
        self.logger.log(loglevel, "Type           : FILE TRANSFER")
        self.logger.log(loglevel, "Deadline       : %s" % self.deadline)
        self.logger.log(loglevel, "File           : %s" % self.file)
        self.logger.log(loglevel, "Transfers      : %s" % self.transfers)
        
    def piggyback(self, lease_id, vnode, physnode):
        if self.transfers.has_key(physnode):
            self.transfers[physnode].append((lease_id, vnode))
        else:
            self.transfers[physnode] = [(lease_id, vnode)]
            
    def is_preemptible(self):
        return False       