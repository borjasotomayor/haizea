from haizea.resourcemanager.enact.base import VMEnactmentBase
import haizea.common.constants as constants
import commands
from pysqlite2 import dbapi2 as sqlite

class VMEnactment(VMEnactmentBase):
    def __init__(self, resourcepool):
        VMEnactmentBase.__init__(self, resourcepool)
        self.onevm = "/home/borja/bin/onevm"
        
        self.conn = sqlite.connect(self.resourcepool.rm.config.getONEDB())
        self.conn.row_factory = sqlite.Row

        
    def start(self, vms):
        print vms
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        leaseID = vm[0]
        hostID = vm[2]
        image = vm[3]
        cpu = vm[4].getByType(constants.RES_CPU)
        memory = vm[4].getByType(constants.RES_MEM)
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
        enactID = vm[3]
        cmd = "%s suspend %i" % (self.onevm, enactID)
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        if status == 0:
            self.logger.debug("Command returned succesfully.", constants.ONE)
        else:
            raise Exception, "Error when running onevm shutdown"

    def resume(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        enactID = vm[3]
        cmd = "%s resume %i" % (self.onevm, enactID)
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        if status == 0:
            self.logger.debug("Command returned succesfully.", constants.ONE)
        else:
            raise Exception, "Error when running onevm shutdown"

    def verifySuspend(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        enactID = vm[3]
        cur = self.conn.cursor()
        cur.execute("select state from vmpool where oid = %i" % enactID)
        onevm = cur.fetchone()        
        state = onevm["state"]
        if state == 5:
            self.logger.debug("Suspend correct.", constants.ONE)
            return 0
        else:
            self.logger.warning("ONE did not complete suspend on time. State is %i" % state, constants.ONE)
            return 1
        
    def verifyResume(self, vms):
        if len(vms) > 1:
            raise Exception, "OpenNebula doesn't support more than 1 VM"
        vm = vms[0]
        enactID = vm[3]
        cur = self.conn.cursor()
        cur.execute("select state from vmpool where oid = %i" % enactID)
        vm = cur.fetchone()        
        state = vm["state"]
        if state == 3:
            self.logger.debug("Resume correct.", constants.ONE)
            return 0
        else:
            self.logger.warning("ONE did not complete resume on time. State is %i" % state, constants.ONE)
            return 1
