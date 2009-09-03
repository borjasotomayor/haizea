# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
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
from haizea.core.scheduler.preparation_schedulers import PreparationScheduler
from haizea.core.scheduler.slottable import ResourceReservation
from haizea.core.scheduler import MigrationResourceReservation, InconsistentLeaseStateError
from haizea.core.leases import Lease, Capacity, UnmanagedSoftwareEnvironment
from haizea.core.scheduler import ReservationEventHandler, NotSchedulableException, EarliestStartingTime
from haizea.common.utils import estimate_transfer_time, get_config
from mx.DateTime import TimeDelta

import bisect
import logging

class ImageTransferPreparationScheduler(PreparationScheduler):
    def __init__(self, slottable, resourcepool, deployment_enact):
        PreparationScheduler.__init__(self, slottable, resourcepool, deployment_enact)
        
        self.imagenode = self.deployment_enact.get_imagenode()
        
        self.transfers = []
        self.completed_transfers = []

        config = get_config()
        self.reusealg = config.get("diskimage-reuse")
        if self.reusealg == constants.REUSE_IMAGECACHES:
            self.maxcachesize = config.get("diskimage-cache-size")
        else:
            self.maxcachesize = None
        
        self.imagenode_bandwidth = self.deployment_enact.get_bandwidth()
        
        self.handlers ={}
        self.handlers[FileTransferResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = ImageTransferPreparationScheduler._handle_start_filetransfer,
                                on_end   = ImageTransferPreparationScheduler._handle_end_filetransfer)

        self.handlers[DiskImageMigrationResourceReservation] = ReservationEventHandler(
                                sched    = self,
                                on_start = ImageTransferPreparationScheduler._handle_start_migrate,
                                on_end   = ImageTransferPreparationScheduler._handle_end_migrate)

    def schedule(self, lease, vmrr, earliest):
        if type(lease.software) == UnmanagedSoftwareEnvironment:
            return [], True
        if lease.get_type() == Lease.ADVANCE_RESERVATION:
            return self.__schedule_deadline(lease, vmrr, earliest)
        elif lease.get_type() in (Lease.BEST_EFFORT, Lease.IMMEDIATE):
            return self.__schedule_asap(lease, vmrr, earliest)

    def schedule_migration(self, lease, vmrr, nexttime):
        if type(lease.software) == UnmanagedSoftwareEnvironment:
            return []
        
        # This code is the same as the one in vm_scheduler
        # Should be factored out
        last_vmrr = lease.get_last_vmrr()
        vnode_migrations = dict([(vnode, (last_vmrr.nodes[vnode], vmrr.nodes[vnode])) for vnode in vmrr.nodes])
        
        mustmigrate = False
        for vnode in vnode_migrations:
            if vnode_migrations[vnode][0] != vnode_migrations[vnode][1]:
                mustmigrate = True
                break
            
        if not mustmigrate:
            return []

        if get_config().get("migration") == constants.MIGRATE_YES_NOTRANSFER:
            start = nexttime
            end = nexttime
            res = {}
            migr_rr = DiskImageMigrationResourceReservation(lease, start, end, res, vmrr, vnode_migrations)
            migr_rr.state = ResourceReservation.STATE_SCHEDULED
            return [migr_rr]

        # Figure out what migrations can be done simultaneously
        migrations = []
        while len(vnode_migrations) > 0:
            pnodes = set()
            migration = {}
            for vnode in vnode_migrations:
                origin = vnode_migrations[vnode][0]
                dest = vnode_migrations[vnode][1]
                if not origin in pnodes and not dest in pnodes:
                    migration[vnode] = vnode_migrations[vnode]
                    pnodes.add(origin)
                    pnodes.add(dest)
            for vnode in migration:
                del vnode_migrations[vnode]
            migrations.append(migration)
        
        # Create migration RRs
        start = max(last_vmrr.post_rrs[-1].end, nexttime)
        bandwidth = self.resourcepool.info.get_migration_bandwidth()
        migr_rrs = []
        for m in migrations:
            mb_to_migrate = lease.software.image_size * len(m.keys())
            migr_time = estimate_transfer_time(mb_to_migrate, bandwidth)
            end = start + migr_time
            res = {}
            for (origin,dest) in m.values():
                resorigin = Capacity([constants.RES_NETOUT])
                resorigin.set_quantity(constants.RES_NETOUT, bandwidth)
                resdest = Capacity([constants.RES_NETIN])
                resdest.set_quantity(constants.RES_NETIN, bandwidth)
                res[origin] = self.slottable.create_resource_tuple_from_capacity(resorigin)
                res[dest] = self.slottable.create_resource_tuple_from_capacity(resdest)                
            migr_rr = DiskImageMigrationResourceReservation(lease, start, start + migr_time, res, vmrr, m)
            migr_rr.state = ResourceReservation.STATE_SCHEDULED
            migr_rrs.append(migr_rr)
            start = end
        
        return migr_rrs

    def estimate_migration_time(self, lease):
        migration = get_config().get("migration")
        if migration == constants.MIGRATE_YES:
            vmrr = lease.get_last_vmrr()
            images_in_pnode = dict([(pnode,0) for pnode in set(vmrr.nodes.values())])
            for (vnode,pnode) in vmrr.nodes.items():
                images_in_pnode[pnode] += lease.software.image_size
            max_to_transfer = max(images_in_pnode.values())
            bandwidth = self.resourcepool.info.get_migration_bandwidth()
            return estimate_transfer_time(max_to_transfer, bandwidth)
        elif migration == constants.MIGRATE_YES_NOTRANSFER:
            return TimeDelta(seconds=0)

    def find_earliest_starting_times(self, lease, nexttime):
        node_ids = [node.id for node in self.resourcepool.get_nodes()]  
        config = get_config()
        mechanism = config.get("transfer-mechanism")
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")
        
        if type(lease.software) == UnmanagedSoftwareEnvironment:
            earliest = {}
            for node in node_ids:
                earliest[node] = EarliestStartingTime(nexttime, EarliestStartingTime.EARLIEST_NOPREPARATION)
            return earliest
        
        # Figure out earliest times assuming we have to transfer the images
        transfer_duration = self.__estimate_image_transfer_time(lease, self.imagenode_bandwidth)
        if mechanism == constants.TRANSFER_UNICAST:
            transfer_duration *= lease.numnodes
        start = self.__get_next_transfer_slot(nexttime, transfer_duration)
        earliest = {}
        for node in node_ids:
            earliest[node] = ImageTransferEarliestStartingTime(start + transfer_duration, ImageTransferEarliestStartingTime.EARLIEST_IMAGETRANSFER)
            earliest[node].transfer_start = start
                
        # Check if we can reuse images
        if reusealg == constants.REUSE_IMAGECACHES:
            nodeswithimg = self.resourcepool.get_nodes_with_reusable_image(lease.software.image_id)
            for node in nodeswithimg:
                earliest[node].time = nexttime
                earliest[node].type = ImageTransferEarliestStartingTime.EARLIEST_REUSE
        
                
        # Check if we can avoid redundant transfers
        if avoidredundant:
            if mechanism == constants.TRANSFER_UNICAST:
                # Piggybacking not supported if unicasting 
                # each individual image
                pass
            if mechanism == constants.TRANSFER_MULTICAST:                
                # We can only piggyback on transfers that haven't started yet
                transfers = [t for t in self.transfers if t.state == ResourceReservation.STATE_SCHEDULED]
                for t in transfers:
                    if t.file == lease.software.image_id:
                        start = t.end
                        if start > nexttime:
                            for n in earliest:
                                if start < earliest[n].time:
                                    earliest[n].time = start
                                    earliest[n].type = ImageTransferEarliestStartingTime.EARLIEST_PIGGYBACK
                                    earliest[n].piggybacking_on = t

        return earliest
            
    def cancel_preparation(self, lease):
        toremove = self.__remove_transfers(lease)     
        for t in toremove:
            t.lease.remove_preparationrr(t)
            self.slottable.remove_reservation(t)
        self.__remove_files(lease)
        
    def cleanup(self, lease):                
        self.__remove_files(lease)
  
        
    def __schedule_deadline(self, lease, vmrr, earliest):
        config = get_config()
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")
        is_ready = False
            
        musttransfer = {}
        mustpool = {}
        nodeassignment = vmrr.nodes
        start = lease.start.requested
        end = lease.start.requested + lease.duration.requested
        for (vnode, pnode) in nodeassignment.items():
            lease_id = lease.id
            self.logger.debug("Scheduling image transfer of '%s' for vnode %i to physnode %i" % (lease.software.image_id, vnode, pnode))

            if reusealg == constants.REUSE_IMAGECACHES:
                if self.resourcepool.exists_reusable_image(pnode, lease.software.image_id, start):
                    self.logger.debug("No need to schedule an image transfer (reusing an image in pool)")
                    mustpool[vnode] = pnode                            
                else:
                    self.logger.debug("Need to schedule a transfer.")
                    musttransfer[vnode] = pnode
            else:
                self.logger.debug("Need to schedule a transfer.")
                musttransfer[vnode] = pnode

        if len(musttransfer) == 0:
            is_ready = True
        else:
            try:
                transfer_rrs = self.__schedule_imagetransfer_edf(lease, musttransfer, earliest)
            except NotSchedulableException, exc:
                raise
 
        # No chance of scheduling exception at this point. It's safe
        # to add entries to the pools
        if reusealg == constants.REUSE_IMAGECACHES:
            for (vnode, pnode) in mustpool.items():
                self.resourcepool.add_mapping_to_existing_reusable_image(pnode, lease.diskimage_id, lease.id, vnode, start)
                self.resourcepool.add_diskimage(pnode, lease.diskimage_id, lease.diskimage_size, lease.id, vnode)
                
        return transfer_rrs, is_ready


    def __schedule_asap(self, lease, vmrr, earliest):
        config = get_config()
        reusealg = config.get("diskimage-reuse")
        avoidredundant = config.get("avoid-redundant-transfers")

        is_ready = False

        transfer_rrs = []
        musttransfer = {}
        piggybacking = []
        for (vnode, pnode) in vmrr.nodes.items():
            earliest_type = earliest[pnode].type
            if earliest_type == ImageTransferEarliestStartingTime.EARLIEST_REUSE:
                # Add to pool
                self.logger.debug("Reusing image for V%i->P%i." % (vnode, pnode))
                self.resourcepool.add_mapping_to_existing_reusable_image(pnode, lease.software.image_id, lease.id, vnode, vmrr.end)
                self.resourcepool.add_diskimage(pnode, lease.software.image_id, lease.software.image_size, lease.id, vnode)
            elif earliest_type == ImageTransferEarliestStartingTime.EARLIEST_PIGGYBACK:
                # We can piggyback on an existing transfer
                transfer_rr = earliest[pnode].piggybacking_on
                transfer_rr.piggyback(lease.id, vnode, pnode)
                self.logger.debug("Piggybacking transfer for V%i->P%i on existing transfer in lease %i." % (vnode, pnode, transfer_rr.lease.id))
                piggybacking.append(transfer_rr)
            else:
                # Transfer
                musttransfer[vnode] = pnode
                self.logger.debug("Must transfer V%i->P%i." % (vnode, pnode))

        if len(musttransfer)>0:
            transfer_rrs = self.__schedule_imagetransfer_fifo(lease, musttransfer, earliest)
            
        if len(musttransfer)==0 and len(piggybacking)==0:
            is_ready = True
            
        return transfer_rrs, is_ready


    def __schedule_imagetransfer_edf(self, lease, musttransfer, earliest):
        # Estimate image transfer time 
        bandwidth = self.deployment_enact.get_bandwidth()
        config = get_config()
        mechanism = config.get("transfer-mechanism")
        transfer_duration = self.__estimate_image_transfer_time(lease, bandwidth)
        if mechanism == constants.TRANSFER_UNICAST:
            transfer_duration *= len(musttransfer)

        # Determine start time
        start = self.__get_last_transfer_slot(lease.start.requested, transfer_duration)

        res = {}
        resimgnode = Capacity([constants.RES_NETOUT])
        resimgnode.set_quantity(constants.RES_NETOUT, bandwidth)
        resnode = Capacity([constants.RES_NETIN])
        resnode.set_quantity(constants.RES_NETIN, bandwidth)
        res[self.imagenode.id] = self.slottable.create_resource_tuple_from_capacity(resimgnode)
        for pnode in musttransfer.values():
            res[pnode] = self.slottable.create_resource_tuple_from_capacity(resnode)
        
        newtransfer = FileTransferResourceReservation(lease, res)
        newtransfer.deadline = lease.start.requested
        newtransfer.state = ResourceReservation.STATE_SCHEDULED
        newtransfer.file = lease.software.image_id
        newtransfer.start = start
        newtransfer.end = start + transfer_duration
        for vnode, pnode in musttransfer.items():
            newtransfer.piggyback(lease.id, vnode, pnode)
        
        bisect.insort(self.transfers, newtransfer)
        
        return [newtransfer]
    
    def __schedule_imagetransfer_fifo(self, lease, musttransfer, earliest):
        # Estimate image transfer time 
        bandwidth = self.imagenode_bandwidth
        config = get_config()
        mechanism = config.get("transfer-mechanism")
        
        # The starting time is the first available slot, which was
        # included in the "earliest" dictionary.
        pnodes = musttransfer.values()
        start = earliest[pnodes[0]].transfer_start
        transfer_duration = self.__estimate_image_transfer_time(lease, bandwidth)
        
        res = {}
        resimgnode = Capacity([constants.RES_NETOUT])
        resimgnode.set_quantity(constants.RES_NETOUT, bandwidth)
        resnode = Capacity([constants.RES_NETIN])
        resnode.set_quantity(constants.RES_NETIN, bandwidth)
        res[self.imagenode.id] = self.slottable.create_resource_tuple_from_capacity(resimgnode)
        for n in musttransfer.values():
            res[n] = self.slottable.create_resource_tuple_from_capacity(resnode)
         
        newtransfer = FileTransferResourceReservation(lease, res)
        newtransfer.start = start
        if mechanism == constants.TRANSFER_UNICAST:
            newtransfer.end = start + (len(musttransfer) * transfer_duration)
        if mechanism == constants.TRANSFER_MULTICAST:
            newtransfer.end = start + transfer_duration
        
        newtransfer.deadline = None
        newtransfer.state = ResourceReservation.STATE_SCHEDULED
        newtransfer.file = lease.software.image_id
        for vnode, pnode in musttransfer.items():
            newtransfer.piggyback(lease.id, vnode, pnode)
            
        bisect.insort(self.transfers, newtransfer)
        
        return [newtransfer]
    
    
    def __estimate_image_transfer_time(self, lease, bandwidth):
        config = get_config()
        force_transfer_time = config.get("force-imagetransfer-time")
        if force_transfer_time != None:
            return force_transfer_time
        else:      
            return estimate_transfer_time(lease.software.image_size, bandwidth)    
    
    
    def __get_next_transfer_slot(self, nexttime, required_duration):
        # This can probably be optimized by using one of the many
        # "list of holes" algorithms out there
        if len(self.transfers) == 0:
            return nexttime
        elif nexttime + required_duration <= self.transfers[0].start:
            return nexttime
        else:
            for i in xrange(len(self.transfers) - 1):
                if self.transfers[i].end != self.transfers[i+1].start:
                    hole_duration = self.transfers[i+1].start - self.transfers[i].end
                    if hole_duration >= required_duration:
                        return self.transfers[i].end
            return self.transfers[-1].end
        
        
    def __get_last_transfer_slot(self, deadline, required_duration):
        # This can probably be optimized by using one of the many
        # "list of holes" algorithms out there
        if len(self.transfers) == 0:
            return deadline - required_duration
        elif self.transfers[-1].end + required_duration <= deadline:
            return deadline - required_duration
        else:
            for i in xrange(len(self.transfers) - 1, 0, -1):
                if self.transfers[i].start != self.transfers[i-1].end:
                    hole_duration = self.transfers[i].start - self.transfers[i-1].end
                    if hole_duration >= required_duration:
                        return self.transfers[i].start - required_duration
            return self.transfers[0].start - required_duration

    def __remove_transfers(self, lease):
        toremove = []
        for t in self.transfers:
            for pnode in t.transfers:
                leases = [l for l, v in t.transfers[pnode]]
                if lease in leases:
                    newtransfers = [(l, v) for l, v in t.transfers[pnode] if l!=lease]
                    t.transfers[pnode] = newtransfers
            # Check if the transfer has to be cancelled
            a = sum([len(l) for l in t.transfers.values()])
            if a == 0:
                toremove.append(t)
        for t in toremove:
            self.transfers.remove(t)
            
        return toremove
    
    def __remove_files(self, lease):
        for vnode, pnode in lease.get_last_vmrr().nodes.items():
            self.resourcepool.remove_diskimage(pnode, lease.id, vnode)         

    @staticmethod
    def _handle_start_filetransfer(sched, lease, rr):
        sched.logger.debug("LEASE-%i Start of handleStartFileTransfer" % lease.id)
        lease.print_contents()
        lease_state = lease.get_state()
        if lease_state == Lease.STATE_SCHEDULED or lease_state == Lease.STATE_READY:
            lease.set_state(Lease.STATE_PREPARING)
            rr.state = ResourceReservation.STATE_ACTIVE
            # TODO: Enactment
        else:
            raise InconsistentLeaseStateError(lease, doing = "starting a file transfer")
            
        # TODO: Check for piggybacking
        
        lease.print_contents()
        sched.logger.debug("LEASE-%i End of handleStartFileTransfer" % lease.id)
        sched.logger.info("Starting image transfer for lease %i" % (lease.id))

    @staticmethod
    def _handle_end_filetransfer(sched, lease, rr):
        sched.logger.debug("LEASE-%i Start of handleEndFileTransfer" % lease.id)
        lease.print_contents()
        lease_state = lease.get_state()
        if lease_state == Lease.STATE_PREPARING:
            lease.set_state(Lease.STATE_READY)
            rr.state = ResourceReservation.STATE_DONE
            for physnode in rr.transfers:
                vnodes = rr.transfers[physnode]
 
#                # Find out timeout of image. It will be the latest end time of all the
#                # leases being used by that image.
#                leases = [l for (l, v) in vnodes]
#                maxend=None
#                for lease_id in leases:
#                    l = sched.leases.get_lease(lease_id)
#                    end = lease.get_endtime()
#                    if maxend==None or end>maxend:
#                        maxend=end
                maxend = None
                # TODO: ENACTMENT: Verify the image was transferred correctly
                sched._add_diskimages(physnode, rr.file, lease.software.image_size, vnodes, timeout=maxend)
        else:
            raise InconsistentLeaseStateError(lease, doing = "ending a file transfer")

        sched.transfers.remove(rr)
        lease.print_contents()
        sched.logger.debug("LEASE-%i End of handleEndFileTransfer" % lease.id)
        sched.logger.info("Completed image transfer for lease %i" % (lease.id))

    def _handle_start_migrate(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleStartMigrate" % l.id)
        l.print_contents()
        rr.state = ResourceReservation.STATE_ACTIVE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleStartMigrate" % l.id)
        self.logger.info("Migrating lease %i..." % (l.id))

    def _handle_end_migrate(self, l, rr):
        self.logger.debug("LEASE-%i Start of handleEndMigrate" % l.id)
        l.print_contents()

        for vnode in rr.transfers:
            origin = rr.transfers[vnode][0]
            dest = rr.transfers[vnode][1]
            
            self.resourcepool.remove_diskimage(origin, l.id, vnode)
            self.resourcepool.add_diskimage(dest, l.software.image_id, l.software.image_size, l.id, vnode)
        
        rr.state = ResourceReservation.STATE_DONE
        l.print_contents()
        self.logger.debug("LEASE-%i End of handleEndMigrate" % l.id)
        self.logger.info("Migrated lease %i..." % (l.id))
        
    def _add_diskimages(self, pnode_id, diskimage_id, diskimage_size, vnodes, timeout):
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
                    # to the pool. We will have to make do with just adding the tainted images.
                    self.logger.debug("Unable to add to pool. Must create individual disk images directly instead.")
                    
            # Besides adding the image to the cache, we need to create a separate image for
            # this specific lease
            for (lease_id, vnode) in vnodes:
                self.resourcepool.add_diskimage(pnode_id, diskimage_id, diskimage_size, lease_id, vnode)
                    
        pnode.print_files()

class FileTransferResourceReservation(ResourceReservation):
    def __init__(self, lease, res, start=None, end=None):
        ResourceReservation.__init__(self, lease, start, end, res)
        self.deadline = None
        self.file = None
        # Dictionary of  physnode -> [ (lease_id, vnode)* ]
        self.transfers = {}

    def print_contents(self, loglevel="VDEBUG"):
        ResourceReservation.print_contents(self, loglevel)
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : FILE TRANSFER")
        logger.log(loglevel, "Deadline       : %s" % self.deadline)
        logger.log(loglevel, "File           : %s" % self.file)
        logger.log(loglevel, "Transfers      : %s" % self.transfers)
        
    def piggyback(self, lease_id, vnode, physnode):
        if self.transfers.has_key(physnode):
            self.transfers[physnode].append((lease_id, vnode))
        else:
            self.transfers[physnode] = [(lease_id, vnode)]
            
    def is_preemptible(self):
        return False       
    
    def __cmp__(self, rr):
        return cmp(self.start, rr.start)
    
class ImageTransferEarliestStartingTime(EarliestStartingTime):
    EARLIEST_IMAGETRANSFER = 2
    EARLIEST_REUSE = 3
    EARLIEST_PIGGYBACK = 4
    
    def __init__(self, time, type):
        EarliestStartingTime.__init__(self, time, type)
        self.transfer_start = None
        self.piggybacking_on = None

class DiskImageMigrationResourceReservation(MigrationResourceReservation):
    def __init__(self, lease, start, end, res, vmrr, transfers):
        MigrationResourceReservation.__init__(self, lease, start, end, res, vmrr, transfers)

    def print_contents(self, loglevel=constants.LOGLEVEL_VDEBUG):
        logger = logging.getLogger("LEASES")
        logger.log(loglevel, "Type           : DISK IMAGE MIGRATION")
        logger.log(loglevel, "Transfers      : %s" % self.transfers)
        ResourceReservation.print_contents(self, loglevel)
