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

import threading
import logging 
from SimpleXMLRPCServer import SimpleXMLRPCServer

DEFAULT_HAIZEA_PORT = 42493

class StoppableSimpleXMLRPCServer(SimpleXMLRPCServer):
    allow_reuse_address = True

    def serve_forever(self):
        self.run = True
        self.socket.settimeout(1)
        while self.run:
            self.handle_request()

    def stop(self):
        self.run = False
        self.socket.close()

class RPCServer(object):
    def __init__(self, manager):
        self.manager = manager
        self.logger = logging.getLogger("RPCSERVER")
        self.port = DEFAULT_HAIZEA_PORT
        self.server = StoppableSimpleXMLRPCServer(("localhost", self.port), allow_none=True)
        self.server_thread = None
        self.register_rpc(self.test_func)
        self.register_rpc(self.cancel_lease)
        self.register_rpc(self.get_leases)
        self.register_rpc(self.get_lease)
        self.register_rpc(self.get_queue)
        self.register_rpc(self.get_hosts)
        self.register_rpc(self.notify_event)

    def start(self):
        # Start the XML-RPC server
        self.server_thread = threading.Thread( target = self.serve )
        self.server_thread.start()
        
    def stop(self):
        self.server.stop()
        self.server_thread.join()
        
    def register_rpc(self, func):
        self.server.register_function(func)
        
    def serve(self):
        self.logger.info("RPC server started on port %i" % self.port)
        self.server.serve_forever()        
        
    def test_func(self):
        self.logger.info("Test RPC function called")
        return 0
    
    def cancel_lease(self, lease_id):
        self.manager.cancel_lease(lease_id)
        return 0

    def get_leases(self):
        return [l.to_xml_string() for l in self.manager.scheduler.leases.get_leases()]

    def get_lease(self, lease_id):
        return 0

    def get_queue(self):
        return [l.to_xml_string() for l in self.manager.scheduler.queue]

    def get_hosts(self):
        return [h.xmlrpc_marshall() for h in self.manager.scheduler.vm_scheduler.resourcepool.nodes.values()]

    def notify_event(self, lease_id, enactment_id, event):
        pass