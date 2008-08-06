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

from haizea.resourcemanager.enact.base import VMEnactmentBase
import haizea.common.constants as constants
import commands
from pysqlite2 import dbapi2 as sqlite

class VMEnactment(VMEnactmentBase):
    def __init__(self, resourcepool):
        VMEnactmentBase.__init__(self, resourcepool)
        self.onevm = self.resourcepool.rm.config.get("onevm")
        
        self.conn = sqlite.connect(self.resourcepool.rm.config.get("one.db"))
        self.conn.row_factory = sqlite.Row

        
    def run_command(self, cmd):
        self.logger.debug("Running command: %s" % cmd, constants.ONE)
        (status, output) = commands.getstatusoutput(cmd)
        self.logger.debug("Returned status=%i, output='%s'" % (status, output), constants.ONE)
        return status, output

    def start(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            hostID = action.vnodes[vnode].pnode
            image = action.vnodes[vnode].diskimage
            cpu = action.vnodes[vnode].resources.get_by_type(constants.RES_CPU)
            memory = action.vnodes[vnode].resources.get_by_type(constants.RES_MEM)
            
            self.logger.debug("Received request to start VM for L%iV%i on host %i, image=%s, cpu=%i, mem=%i"
                         % (action.lease_haizea_id, vnode, hostID, image, cpu, memory), constants.ONE)

            cmd = "%s deploy %i %i" % (self.onevm, vmid, hostID)
            status, output = self.run_command(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm deploy (status=%i, output='%s')" % (status, output)
            
    def stop(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            cmd = "%s shutdown %i" % (self.onevm, vmid)
            status, output = self.run_command(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm shutdown (status=%i, output='%s')" % (status, output)

    def suspend(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            cmd = "%s suspend %i" % (self.onevm, vmid)
            status, output = self.run_command(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm suspend (status=%i, output='%s')" % (status, output)
        
    def resume(self, action):
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            cmd = "%s resume %i" % (self.onevm, vmid)
            status, output = self.run_command(cmd)
            if status == 0:
                self.logger.debug("Command returned succesfully.", constants.ONE)
            else:
                raise Exception, "Error when running onevm resume (status=%i, output='%s')" % (status, output)

    def verifySuspend(self, action):
        # TODO: Do a single query
        result = 0
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            cur = self.conn.cursor()
            cur.execute("select state from vmpool where oid = %i" % vmid)
            onevm = cur.fetchone()        
            state = onevm["state"]
            if state == 5:
                self.logger.debug("Suspend of L%iV%i correct." % (action.lease_haizea_id, vnode), constants.ONE)
            else:
                self.logger.warning("ONE did not complete suspend  of L%iV%i on time. State is %i" % (action.lease_haizea_id, vnode, state), constants.ONE)
                result = 1
        return result
        
    def verifyResume(self, action):
        # TODO: Do a single query
        result = 0
        for vnode in action.vnodes:
            # Unpack action
            vmid = action.vnodes[vnode].enactment_info
            cur = self.conn.cursor()
            cur.execute("select state from vmpool where oid = %i" % vmid)
            onevm = cur.fetchone()        
            state = onevm["state"]
            if state == 3:
                self.logger.debug("Resume of L%iV%i correct." % (action.lease_haizea_id, vnode), constants.ONE)
            else:
                self.logger.warning("ONE did not complete resume of L%iV%i on time. State is %i" % (action.lease_haizea_id, vnode, state), constants.ONE)
                result = 1
        return result

