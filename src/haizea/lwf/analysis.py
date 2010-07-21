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
from haizea.core.leases import LeaseWorkload, Site, UnmanagedSoftwareEnvironment,\
    DiskImageSoftwareEnvironment, LeaseAnnotations
import operator
from haizea.common.stats import percentile, print_percentiles, print_distribution

class LWFAnalyser(object):
    
    
    def __init__(self, lwffile, utilization_length, annotationfile, verbose = False):
        # Arbitrary start time
        self.starttime = DateTime(2006,11,25,13)
        
        self.workload = LeaseWorkload.from_xml_file(lwffile, self.starttime)
        self.site = Site.from_lwf_file(lwffile)
        
        if utilization_length == None:
            self.utilization_length = self.workload.get_leases()[-1].submit_time - self.starttime
        else:
            self.utilization_length = utilization_length

        if annotationfile != None:
            annotations = LeaseAnnotations.from_xml_file(annotationfile)
            annotations.apply_to_leases(self.workload.get_leases())
            
        self.verbose = verbose
        
    def analyse(self):
        requtilization = 0
        actutilization = 0
        software = {"Unmanaged": 0}
        nnodes = []
        reqdurations = []
        actdurations = []
        nleases = len(self.workload.get_leases())
        for lease in self.workload.get_leases():
            if lease.start.requested == "Unspecified":
                start = lease.submit_time
            else:
                start = lease.start.requested

            if start + lease.duration.requested > self.starttime + self.utilization_length:
                reqduration = (self.starttime + self.utilization_length - start).seconds
            else: 
                reqduration = lease.duration.requested.seconds

            if lease.duration.known != None:
                if start + lease.duration.known > self.starttime + self.utilization_length:
                    actduration = (self.starttime + self.utilization_length - start).seconds
                else: 
                    actduration = lease.duration.known.seconds
            else:
                actduration = reqduration
                            
            for res in lease.requested_resources.values():
                for i in range(1,res.get_ninstances("CPU") + 1):
                    requtilization += (res.get_quantity_instance("CPU", i) / 100.0) * reqduration
                    actutilization += (res.get_quantity_instance("CPU", i) / 100.0) * actduration

            nnodes.append(len(lease.requested_resources))
            reqdurations.append(lease.duration.requested.seconds)
            
            if lease.duration.known != None:
                actdurations.append(lease.duration.known.seconds)
            
            if isinstance(lease.software, UnmanagedSoftwareEnvironment):
                software["Unmanaged"] += 1
            elif isinstance(lease.software, DiskImageSoftwareEnvironment):
                image = lease.software.image_id
                software[image] = software.setdefault(image, 0) +1
                
        if self.site != None:
            max_utilization = 0
            duration = self.utilization_length.seconds
            for res in self.site.nodes.get_all_nodes().values():
                for i in range(1,res.get_ninstances("CPU") + 1):
                    max_utilization += (res.get_quantity_instance("CPU", i)/100.0) * duration
                    
        if self.verbose:
            reqdurd = {}
            nnodesd = {}
            for reqduration in reqdurations:
                reqdurd[reqduration] = reqdurd.setdefault(reqduration, 0) +1        
            for n in nnodes:
                nnodesd[n] = nnodesd.setdefault(n, 0) +1        
                    
                    
        print actutilization
        print max_utilization
        print "Requested utilization: %.2f%%" % ((requtilization / max_utilization) * 100.0)
        print "   Actual utilization: %.2f%%" % ((actutilization / max_utilization) * 100.0)
        print
        #sorted_images = sorted(software.iteritems(), key=operator.itemgetter(1), reverse=True)
        print "NODES"
        print "-----"
        print_percentiles(nnodes)
        print
        if self.verbose:
            print "NODES (distribution)"
            print "--------------------"
            print_distribution(nnodesd, nleases)
            print
        print "REQUESTED DURATIONS"
        print "-------------------"
        print_percentiles(reqdurations)
        print
        if self.verbose:
            print "REQUESTED DURATIONS (distribution)"
            print "----------------------------------"
            print_distribution(reqdurd, nleases)
            print
        print "ACTUAL DURATIONS"
        print "----------------"
        print_percentiles(actdurations)
        print
        print "IMAGES"
        print "------"
        print_distribution(software, nleases)
        #for image, count in sorted_images:
        #    print "%s: %i (%.2f%%)" % (image, count, (float(count)/nleases)*100)
        print        