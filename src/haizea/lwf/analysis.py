# -------------------------------------------------------------------------- #
# Copyright 2006-2010, University of Chicago                                 #
# Copyright 2008-2010, Distributed Systems Architecture Group, Universidad   #
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
from mx.DateTime import DateTime
from haizea.core.leases import LeaseWorkload, Site


class LWFAnalyser(object):
    
    
    def __init__(self, lwffile, utilization_length):
        # Arbitrary start time
        self.starttime = DateTime(2006,11,25,13)
        
        self.workload = LeaseWorkload.from_xml_file(lwffile, self.starttime)
        self.site = Site.from_lwf_file(lwffile)
        
        if utilization_length == None:
            self.utilization_length = self.workload.get_leases()[-1].submit_time - self.starttime
        else:
            self.utilization_length = utilization_length
        print self.utilization_length
        
    def analyse(self):
        utilization = 0
        for lease in self.workload.get_leases():
            if lease.start.requested + lease.duration.requested > self.starttime + self.utilization_length:
                duration = self.starttime + self.utilization_length - lease.start.requested
            else: 
                duration = lease.duration.requested.seconds
            for res in lease.requested_resources.values():
                for i in range(1,res.get_ninstances("CPU") + 1):
                    utilization += res.get_quantity_instance("CPU", i) * duration
        
        if self.site != None:
            max_utilization = 0
            duration = self.utilization_length.seconds
            for res in self.site.nodes.get_all_nodes().values():
                for i in range(1,res.get_ninstances("CPU") + 1):
                    max_utilization += res.get_quantity_instance("CPU", i) * duration
            
        
        print "Utilization: %.2f%%" % (utilization / max_utilization)
        