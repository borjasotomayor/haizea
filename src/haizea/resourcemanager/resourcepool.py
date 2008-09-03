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

from haizea.common.utils import vnodemapstr
import haizea.common.constants as constants
import haizea.resourcemanager.enact.actions as actions
import logging 

class ResourcePool(object):
    def __init__(self, scheduler):
        self.rm = scheduler.rm
        self.logger = logging.getLogger("RESOURCEPOOL")
                
        self.info = None
        self.vm = None
        self.storage = None

        self.load_enactment_modules()
        
        self.nodes = self.info.get_nodes()
        
            
    def load_enactment_modules(self):
        mode = self.rm.config.get("mode")
        try:
            exec "import %s.%s as enact" % (constants.ENACT_PACKAGE, mode)
            self.info = enact.info(self) #IGNORE:E0602
            self.vm = enact.vm(self) #IGNORE:E0602
            self.storage = enact.storage(self) #IGNORE:E0602
        except Exception, msg:
            self.logger.error("Unable to load enactment modules for mode '%s'" % mode)
            raise                
        
        
    def start_vms(self, lease, rr):
        start_action = actions.VMEnactmentStartAction()
        start_action.from_rr(rr)
        
        # TODO: Get tainted image
        
        for (vnode, pnode) in rr.nodes.items():
            node = self.get_node(pnode)
            diskimage = node.get_diskimage(lease.id, vnode, lease.diskimage_id)
            start_action.vnodes[vnode].pnode = node.enactment_info
            start_action.vnodes[vnode].diskimage = diskimage.filename
            start_action.vnodes[vnode].resources = rr.resources_in_pnode[pnode]

        try:
            self.vm.start(start_action)
        except Exception, msg:
            self.logger.error("Enactment of start VM failed: %s" % msg)
            self.rm.fail_lease(lease.id)
        
    def stop_vms(self, lease, rr):
        stop_action = actions.VMEnactmentStopAction()
        stop_action.from_rr(rr)
        try:
            self.vm.stop(stop_action)
        except Exception, msg:
            self.logger.error("Enactment of end VM failed: %s" % msg)
            self.rm.fail_lease(lease)
         
    def suspend_vms(self, lease, rr):
        suspend_action = actions.VMEnactmentSuspendAction()
        suspend_action.from_rr(rr)
        try:
            self.vm.suspend(suspend_action)
        except Exception, msg:
            self.logger.error("Enactment of suspend VM failed: %s" % msg)
            self.rm.fail_lease(lease)
    
    def verify_suspend(self, lease, rr):
        verify_suspend_action = actions.VMEnactmentConfirmSuspendAction()
        verify_suspend_action.from_rr(rr)
        self.vm.verify_suspend(verify_suspend_action)
    
    # TODO
    # The following should be implemented to handle asynchronous
    # notifications of a suspend completing.
    #def suspendDone(self, lease, rr):
    #    pass
    
    def resume_vms(self, lease, rr):
        resume_action = actions.VMEnactmentResumeAction()
        resume_action.from_rr(rr)
        try:
            self.vm.resume(resume_action)
        except Exception, msg:
            self.logger.error("Enactment of resume VM failed: %s" % msg)
            self.rm.fail_lease(lease)
    
    def verify_resume(self, lease, rr):
        verify_resume_action = actions.VMEnactmentConfirmResumeAction()
        verify_resume_action.from_rr(rr)
        self.vm.verify_resume(verify_resume_action)    
        
    # TODO
    # The following should be implemented to handle asynchronous
    # notifications of a resume completing.
    #def resumeDone(self, lease, rr):
    #    pass

    # TODO
    # The following should be implemented to handle asynchronous
    # notifications of a VM ending
    #def notify_vm_done(self, lease, rr):
    #    pass
    
    def get_nodes(self):
        return self.nodes

    def get_num_nodes(self):
        return len(self.nodes)
        
    def get_node(self, nod_id):
        return self.nodes[nod_id-1]
        
    def add_diskimage(self, pnode, diskimage_id, imagesize, lease_id, vnode):
        self.logger.debug("Adding disk image for L%iV%i in pnode=%i" % (lease_id, vnode, pnode))
        self.get_node(pnode).print_files()
        imagefile = self.storage.resolve_to_file(lease_id, vnode, diskimage_id)
        img = DiskImageFile(imagefile, imagesize, lease_id, vnode, diskimage_id)
        self.get_node(pnode).add_file(img)
        self.get_node(pnode).print_files()
        self.rm.accounting.append_stat(constants.COUNTER_DISKUSAGE, self.get_max_disk_usage())
        return img
            
    def remove_diskimage(self, pnode, lease, vnode):
        node = self.get_node(pnode)
        node.print_files()

        self.logger.debug("Removing disk image for L%iV%i in node %i" % (lease, vnode, pnode))
        node.remove_diskimage(lease, vnode)

        node.print_files()
        
        self.rm.accounting.append_stat(constants.COUNTER_DISKUSAGE, self.get_max_disk_usage())    
        
    def add_ramfile(self, pnode, lease_id, vnode, size):
        node = self.get_node(pnode)
        self.logger.debug("Adding RAM file for L%iV%i in node %i" % (lease_id, vnode, pnode))
        node.print_files()
        f = RAMImageFile("RAM_L%iV%i" % (lease_id, vnode), size, lease_id, vnode)
        node.add_file(f)        
        node.print_files()
        self.rm.accounting.append_stat(constants.COUNTER_DISKUSAGE, self.get_max_disk_usage())

    def remove_ramfile(self, pnode, lease_id, vnode):
        node = self.get_node(pnode)
        self.logger.debug("Removing RAM file for L%iV%i in node %i" % (lease_id, vnode, pnode))
        node.print_files()
        node.remove_ramfile(lease_id, vnode)
        node.print_files()
        self.rm.accounting.append_stat(constants.COUNTER_DISKUSAGE, self.get_max_disk_usage())
        
    def get_max_disk_usage(self):
        return max([n.get_disk_usage() for n in self.nodes])
    
class Node(object):
    def __init__(self, resourcepool, nod_id, hostname, capacity):
        self.logger = logging.getLogger("RESOURCEPOOL")
        self.resourcepool = resourcepool
        self.nod_id = nod_id
        self.hostname = hostname
        self.capacity = capacity
        self.files = []

        # enactment-specific information
        self.enactment_info = None
        
        # Kludgy way of keeping track of utilization
        # TODO: Compute this information based on the lease reservations,
        # either on the fly or when Haizea stops running.
        self.transfer_doing = constants.DOING_IDLE
        self.vm_doing = constants.DOING_IDLE
        
    def get_capacity(self):
        return self.capacity
           
    def add_file(self, f):
        self.files.append(f)
        
    def get_diskimage(self, lease_id, vnode, diskimage_id):
        image = [f for f in self.files if isinstance(f, DiskImageFile) and 
                 f.diskimage_id == diskimage_id and 
                 f.lease_id == lease_id and
                 f.vnode == vnode]
        if len(image) == 0:
            return None
        elif len(image) == 1:
            return image[0]
        elif len(image) > 1:
            self.logger.warning("More than one tainted image for L%iV%i on node %i" % (lease_id, vnode, self.nod_id))
            return image[0]

    def remove_diskimage(self, lease_id, vnode):
        image = [f for f in self.files if isinstance(f, DiskImageFile) and 
                 f.lease_id == lease_id and
                 f.vnode == vnode]
        if len(image) > 0:
            image = image[0]
            self.files.remove(image)
            
    def remove_ramfile(self, lease_id, vnode):
        ramfile = [f for f in self.files if isinstance(f, RAMImageFile) and f.lease_id==lease_id and f.vnode==vnode]
        if len(ramfile) > 0:
            ramfile = ramfile[0]
            self.files.remove(ramfile)
                
        
    def get_disk_usage(self):
        return sum([f.filesize for f in self.files])


    def get_diskimages(self):
        return [f for f in self.files if isinstance(f, DiskImageFile)]
        
    def print_files(self):
        images = ""
        if len(self.files) > 0:
            images = ", ".join([str(img) for img in self.files])
        self.logger.vdebug("Node %i has %iMB %s" % (self.nod_id, self.get_disk_usage(), images))

    def get_state(self):
        if self.vm_doing == constants.DOING_IDLE and self.transfer_doing == constants.DOING_TRANSFER:
            return constants.DOING_TRANSFER_NOVM
        else:
            return self.vm_doing

    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        h = {}
        h["id"] = self.nod_id
        h["hostname"] = self.hostname
        h["cpu"] = self.capacity.get_by_type(constants.RES_CPU)
        h["mem"] = self.capacity.get_by_type(constants.RES_MEM)
                
        return h
        

        
class File(object):
    def __init__(self, filename, filesize):
        self.filename = filename
        self.filesize = filesize
        
class DiskImageFile(File):
    def __init__(self, filename, filesize, lease_id, vnode, diskimage_id):
        File.__init__(self, filename, filesize)
        self.lease_id = lease_id
        self.vnode = vnode
        self.diskimage_id = diskimage_id
                
    def __str__(self):
        return "(DISK L%iv%i %s %s)" % (self.lease_id, self.vnode, self.diskimage_id, self.filename)


class RAMImageFile(File):
    def __init__(self, filename, filesize, lease_id, vnode):
        File.__init__(self, filename, filesize)
        self.lease_id = lease_id
        self.vnode = vnode
                
    def __str__(self):
        return "(RAM L%iv%i %s)" % (self.lease_id, self.vnode, self.filename)
    
class ResourcePoolWithReusableImages(object):
    def __init__(self, scheduler):
        ResourcePool.__init__(self, scheduler)
    
    def remove_diskimage(self, pnode, lease, vnode):
        ResourcePool.remove_diskimage(self, pnode, lease, vnode)
        self.logger.debug("Removing pooled images for L%iV%i in node %i" % (lease, vnode, pnode))
        toremove = []
        for img in node.getPoolImages():
            if (lease, vnode) in img.mappings:
                img.mappings.remove((lease, vnode))
            node.printFiles()
            # Keep image around, even if it isn't going to be used
            # by any VMs. It might be reused later on.
            # It will be purged if space has to be made available
            # for other images
        for img in toremove:
            node.files.remove(img)
        node.printFiles()

    
    
class NodeWithReusableImages(Node):
    def __init__(self, resourcepool, nod_id, hostname, capacity):
        Node.__init__(self, resourcepool, nod_id, hostname, capacity)
        self.reusable_images = []

    def add_mapping_to_existing_reusable_image(self, imagefile, lease_id, vnode, timeout):
        for f in self.files:
            if f.filename == imagefile:
                f.add_mapping(lease_id, vnode)
                f.update_timeout(timeout)
                break  # Ugh
        self.print_files()
            
    def get_reusable_image(self, imagefile, after = None, lease_id=None, vnode=None):
        images = [i for i in self.reusable_images if i.filename == imagefile]
        if after != None:
            images = [i for i in images if i.timeout >= after]
        if lease_id != None and vnode != None:
            images = [i for i in images if i.has_mapping(lease_id, vnode)]
        if len(images)>0:
            return images[0]
        else:
            return None
        
    def exists_reusable_image(self, imagefile, after = None, lease_id=None, vnode=None):
        entry = self.get_reusable_image(imagefile, after = after, lease_id=lease_id, vnode=vnode)
        if entry == None:
            return False
        else:
            return True

    def get_reusable_images(self):
        return self.reusable_images

    def get_reusable_images_size(self):
        return sum([f.filesize for f in self.reusable_images])
    
    def purge_oldest_unused_image(self):
        unused = [img for img in self.reusable_images if not img.has_mappings()]
        if len(unused) == 0:
            return 0
        else:
            i = iter(unused)
            oldest = i.next()
            for img in i:
                if img.timeout < oldest.timeout:
                    oldest = img
            self.reusable_images.remove(oldest)
            return 1
    
    def purge_downto(self, target):
        done = False
        while not done:
            removed = self.purge_oldest_unused_image()
            if removed==0:
                done = True
                success = False
            elif removed == 1:
                if self.get_reusable_images_size() <= target:
                    done = True
                    success = True
        return success

class ReusableDiskImageFile(File):
    def __init__(self, filename, filesize, diskimage_id):
        File.__init__(self, filename, filesize)
        self.diskimage_id = diskimage_id
        self.mappings = set([])
        self.timeout = None
        
    def add_mapping(self, lease_id, vnode):
        self.mappings.add((lease_id, vnode))
        
    def has_mapping(self, lease_id, vnode):
        return (lease_id, vnode) in self.mappings
    
    def has_mappings(self):
        return len(self.mappings) > 0
        
    def update_timeout(self, timeout):
        if timeout > self.timeout:
            self.timeout = timeout
        
    def is_expired(self, curTime):
        if self.timeout == None:
            return False
        elif self.timeout > curTime:
            return True
        else:
            return False
        
    def __str__(self):
        if self.timeout == None:
            timeout = "NOTIMEOUT"
        else:
            timeout = self.timeout
        return "(REUSABLE %s %s %s %s)" % (vnodemapstr(self.mappings), self.diskimage_id, str(timeout), self.filename)

