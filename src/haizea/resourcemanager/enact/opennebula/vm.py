from haizea.resourcemanager.enact.base import VMEnactmentBase
import haizea.common.constants as constants
import commands

class VMEnactment(VMEnactmentBase):
    def __init__(self, resourcepool):
        VMEnactmentBase.__init__(self, resourcepool)
        self.onevm = "/home/borja/bin/onevm"
        
    def start(self, vms):
        print vms
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        leaseID = vm[0]
        hostID = vm[2]
        image = vm[3]
        cpu = vm[4].get(constants.RES_CPU)
        memory = vm[4].get(constants.RES_MEM)
        vmid = vm[5]
        self.logger.debug("Received request to start VM for lease %i on host %i, image=%s, cpu=%i, mem=%i"
                     % (leaseID, hostID, image, cpu, memory), constants.ONE)
        
        cmd = "%s deploy %i %i" % (self.onevm, vmid, hostID)
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        if status == 0:
            self.logger.debug("Command returned succesfully.", constants.ONE)
        else:
            raise Exception, "Error when running onevm deploy"
        
        return vmid
    
    def stop(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        enactID = vm[3]
        cmd = "%s shutdown %i" % (self.onevm, enactID)
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        if status == 0:
            self.logger.debug("Command returned succesfully.", constants.ONE)
        else:
            raise Exception, "Error when running onevm shutdown"

    def suspend(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]

    def resume(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
