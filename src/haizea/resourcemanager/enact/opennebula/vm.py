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

        
    def runCommand(self, cmd):
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        return status

    def start(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            hostID = action.vnodes[vnode].pnode
            image = action.vnodes[vnode].diskimage
            cpu = action.vnodes[vnode].res.getByType(constants.RES_CPU)
            memory = action.vnodes[vnode].res.getByType(constants.RES_MEM)
            
            self.logger.debug("Received request to start VM for L%iV%i on host %i, image=%s, cpu=%i, mem=%i"
                         % (action.leaseHaizeaID, vnode, hostID, image, cpu, memory), constants.ONE)
            cmd = "%s deploy %i %i" % (self.onevm, vmid, hostID)
            status = self.runCommand(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm deploy"
            
    def stop(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            cmd = "%s shutdown %i" % (self.onevm, vmid)
            status = self.runCommand(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm shutdown"

    def suspend(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            cmd = "%s suspend %i" % (self.onevm, vmid)
            status = self.runCommand(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm suspend"
        
    def resume(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            cmd = "%s resume %i" % (self.onevm, vmid)
            status = self.runCommand(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm resume"

    def verifySuspend(self, action):
        # TODO: Do a single query
        result = 0
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            cur = self.conn.cursor()
            cur.execute("select state from vmpool where oid = %i" % vmid)
            onevm = cur.fetchone()        
            state = onevm["state"]
            if state == 5:
                self.logger.debug("Suspend of L%iV%i correct." % (action.leaseHaizeaID, vnode), constants.ONE)
            else:
                self.logger.warning("ONE did not complete suspend  of L%i%V%i on time. State is %i" % (action.leaseHaizeaID, vnode, state), constants.ONE)
                result = 1
        return result
        
    def verifyResume(self, action):
        # TODO: Do a single query
        result = 0
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactmentInfo
            cur = self.conn.cursor()
            cur.execute("select state from vmpool where oid = %i" % vmid)
            onevm = cur.fetchone()        
            state = onevm["state"]
            if state == 3:
                self.logger.debug("Suspend of L%iV%i correct." % (action.leaseHaizeaID, vnode), constants.ONE)
            else:
                self.logger.warning("ONE did not complete resume of L%i%V%i on time. State is %i" % (action.leaseHaizeaID, vnode, state), constants.ONE)
                result = 1
        return result

