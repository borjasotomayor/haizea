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

from haizea.common.utils import vnodemapstr
import haizea.common.constants as constants
import haizea.core.enact.actions as actions
from haizea.core.scheduler import EnactmentError
import logging 


class ResourcePool(object):
    def __init__(self, info_enact, vm_enact, deploy_enact):
        self.logger = logging.getLogger("RPOOL")
                
        self.info = info_enact
        self.vm = vm_enact
        # TODO: Ideally, deployment enactment shouldn't be here, specially since
        # it already "hangs" below the deployment modules. For now,
        # it does no harm, though.
        self.deployment = deploy_enact
        
        self.nodes = self.info.get_nodes()
        
    def start_vms(self, lease, rr):
        start_action = actions.VMEnactmentStartAction()
        start_action.from_rr(rr)
                
        for (vnode, pnode) in rr.nodes.items():
            node = self.get_node(pnode)
            #diskimage = node.get_diskimage(lease.id, vnode, lease.diskimage_id)
            start_action.vnodes[vnode].pnode = node.enactment_info
            #start_action.vnodes[vnode].diskimage = diskimage.filename
            start_action.vnodes[vnode].resources = rr.resources_in_pnode[pnode]

        try:
            self.vm.start(start_action)
        except EnactmentError, exc:
            self.logger.error("Enactment of start VM failed: %s" % exc.message)
            raise
        
    def stop_vms(self, lease, rr):
        stop_action = actions.VMEnactmentStopAction()
        stop_action.from_rr(rr)
        try:
            self.vm.stop(stop_action)
        except EnactmentError, exc:
            self.logger.error("Enactment of end VM failed: %s" % exc.message)
            raise
         
    def suspend_vms(self, lease, rr):
        # Add memory image files
        for vnode in rr.vnodes:
            pnode = rr.vmrr.nodes[vnode]
            self.add_ramfile(pnode, lease.id, vnode, lease.requested_resources[vnode].get_quantity(constants.RES_MEM))

        # Enact suspend
        suspend_action = actions.VMEnactmentSuspendAction()
        suspend_action.from_rr(rr)
        try:
            self.vm.suspend(suspend_action)
        except EnactmentError, exc:
            self.logger.error("Enactment of suspend VM failed: %s" % exc.message)
            raise
    
    def verify_suspend(self, lease, rr):
        verify_suspend_action = actions.VMEnactmentConfirmSuspendAction()
        verify_suspend_action.from_rr(rr)
        self.vm.verify_suspend(verify_suspend_action)
    
    def resume_vms(self, lease, rr):
        # Remove memory image files
        for vnode in rr.vnodes:
            pnode = rr.vmrr.nodes[vnode]
            self.remove_ramfile(pnode, lease.id, vnode)

        # Enact resume
        resume_action = actions.VMEnactmentResumeAction()
        resume_action.from_rr(rr)
        try:
            self.vm.resume(resume_action)
        except EnactmentError, exc:
            self.logger.error("Enactment of resume VM failed: %s" % exc.message)
            raise
    
    def verify_resume(self, lease, rr):
        verify_resume_action = actions.VMEnactmentConfirmResumeAction()
        verify_resume_action.from_rr(rr)
        self.vm.verify_resume(verify_resume_action)    
    
    def get_nodes(self):
        return self.nodes.values()
    
    # An auxiliary node is a host whose resources are going to be scheduled, but
    # where no VMs are actually going to run. For example, a disk image repository node.
    def get_aux_nodes(self):
        # TODO: We're only asking the deployment enactment module for auxiliary nodes.
        # There might be a scenario where the info enactment module also reports
        # auxiliary nodes.
        return self.deployment.get_aux_nodes()

    def get_num_nodes(self):
        return len(self.nodes)
        
    def get_node(self, node_id):
        return self.nodes[node_id]
        
    def add_diskimage(self, pnode, diskimage_id, imagesize, lease_id, vnode):
        self.logger.debug("Adding disk image for L%iV%i in pnode=%i" % (lease_id, vnode, pnode))
        
        self.logger.vdebug("Files BEFORE:")
        self.get_node(pnode).print_files()
        
        imagefile = self.deployment.resolve_to_file(lease_id, vnode, diskimage_id)
        img = DiskImageFile(imagefile, imagesize, lease_id, vnode, diskimage_id)
        self.get_node(pnode).add_file(img)

        self.logger.vdebug("Files AFTER:")
        self.get_node(pnode).print_files()
        
        return img
            
    def remove_diskimage(self, pnode, lease, vnode):
        node = self.get_node(pnode)
        node.print_files()

        self.logger.debug("Removing disk image for L%iV%i in node %i" % (lease, vnode, pnode))
        node.remove_diskimage(lease, vnode)

        node.print_files()
                
    def add_ramfile(self, pnode, lease_id, vnode, size):
        node = self.get_node(pnode)
        self.logger.debug("Adding RAM file for L%iV%i in node %i" % (lease_id, vnode, pnode))
        node.print_files()
        f = RAMImageFile("RAM_L%iV%i" % (lease_id, vnode), size, lease_id, vnode)
        node.add_file(f)        
        node.print_files()

    def remove_ramfile(self, pnode, lease_id, vnode):
        node = self.get_node(pnode)
        self.logger.debug("Removing RAM file for L%iV%i in node %i" % (lease_id, vnode, pnode))
        node.print_files()
        node.remove_ramfile(lease_id, vnode)
        node.print_files()
        
    def get_max_disk_usage(self):
        return max([n.get_disk_usage() for n in self.nodes.values()])
    
class ResourcePoolNode(object):
    def __init__(self, node_id, hostname, capacity):
        self.logger = logging.getLogger("RESOURCEPOOL")
        self.id = node_id
        self.hostname = hostname
        self.capacity = capacity
        self.files = []

        # enactment-specific information
        self.enactment_info = None
        
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
        self.logger.vdebug("Node %i files: %iMB %s" % (self.id, self.get_disk_usage(), images))

    def xmlrpc_marshall(self):
        # Convert to something we can send through XMLRPC
        h = {}
        h["id"] = self.id
        h["hostname"] = self.hostname
        h["cpu"] = self.capacity.get_quantity(constants.RES_CPU)
        h["mem"] = self.capacity.get_quantity(constants.RES_MEM)
                
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
    
class ResourcePoolWithReusableImages(ResourcePool):
    def __init__(self, info_enact, vm_enact, deploy_enact):
        ResourcePool.__init__(self, info_enact, vm_enact, deploy_enact)
        
        self.nodes = dict([(id,ResourcePoolNodeWithReusableImages.from_node(node)) for id, node in self.nodes.items()])
    
    def add_reusable_image(self, pnode, diskimage_id, imagesize, mappings, timeout):
        self.logger.debug("Adding reusable image for %s in pnode=%i" % (mappings, pnode))
        
        self.logger.vdebug("Files BEFORE:")
        self.get_node(pnode).print_files()
        
        imagefile = "reusable-%s" % diskimage_id
        img = ReusableDiskImageFile(imagefile, imagesize, diskimage_id, timeout)
        for (lease_id, vnode) in mappings:
            img.add_mapping(lease_id, vnode)

        self.get_node(pnode).add_reusable_image(img)

        self.logger.vdebug("Files AFTER:")
        self.get_node(pnode).print_files()
        
        return img
    
    def add_mapping_to_existing_reusable_image(self, pnode_id, diskimage_id, lease_id, vnode, timeout):
        self.get_node(pnode_id).add_mapping_to_existing_reusable_image(diskimage_id, lease_id, vnode, timeout)
    
    def remove_diskimage(self, pnode_id, lease, vnode):
        ResourcePool.remove_diskimage(self, pnode_id, lease, vnode)
        self.logger.debug("Removing cached images for L%iV%i in node %i" % (lease, vnode, pnode_id))
        for img in self.get_node(pnode_id).get_reusable_images():
            if (lease, vnode) in img.mappings:
                img.mappings.remove((lease, vnode))
            self.get_node(pnode_id).print_files()
            # Keep image around, even if it isn't going to be used
            # by any VMs. It might be reused later on.
            # It will be purged if space has to be made available
            # for other images
        
    def get_nodes_with_reusable_image(self, diskimage_id, after = None):
        return [n.id for n in self.get_nodes() if n.exists_reusable_image(diskimage_id, after=after)]

    def exists_reusable_image(self, pnode_id, diskimage_id, after):
        return self.get_node(pnode_id).exists_reusable_image(diskimage_id, after = after)
    
    
class ResourcePoolNodeWithReusableImages(ResourcePoolNode):
    def __init__(self, node_id, hostname, capacity):
        ResourcePoolNode.__init__(self, node_id, hostname, capacity)
        self.reusable_images = []

    @classmethod
    def from_node(cls, n):
        node = cls(n.id, n.hostname, n.capacity)
        node.enactment_info = n.enactment_info
        return node
    
    def add_reusable_image(self, f):
        self.reusable_images.append(f)

    def add_mapping_to_existing_reusable_image(self, diskimage_id, lease_id, vnode, timeout):
        for f in self.reusable_images:
            if f.diskimage_id == diskimage_id:
                f.add_mapping(lease_id, vnode)
                f.update_timeout(timeout)
                break  # Ugh
        self.print_files()
            
    def get_reusable_image(self, diskimage_id, after = None, lease_id=None, vnode=None):
        images = [i for i in self.reusable_images if i.diskimage_id == diskimage_id]
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

    def print_files(self):
        ResourcePoolNode.print_files(self)
        images = ""
        if len(self.reusable_images) > 0:
            images = ", ".join([str(img) for img in self.reusable_images])
        self.logger.vdebug("Node %i reusable images: %iMB %s" % (self.id, self.get_reusable_images_size(), images))

class ReusableDiskImageFile(File):
    def __init__(self, filename, filesize, diskimage_id, timeout):
        File.__init__(self, filename, filesize)
        self.diskimage_id = diskimage_id
        self.mappings = set([])
        self.timeout = timeout
        
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

