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

import haizea.common.stats as stats
from haizea.core.leases import LeaseWorkload, LeaseAnnotation, LeaseAnnotations, Timestamp
from haizea.common.utils import round_datetime_delta
from mx.DateTime import TimeDelta
import ConfigParser

try:
    import xml.etree.ElementTree as ET
except ImportError:
    # Compatibility with Python <=2.4
    import elementtree.ElementTree as ET 

class FileGenerator(object):
    
    GENERAL_SEC = "general"

    TYPE_OPT = "type"
    DISTRIBUTION_OPT = "distribution"
    MIN_OPT = "min"
    MAX_OPT = "max"
    MEAN_OPT = "mu"
    STDEV_OPT = "sigma"
    ALPHA_OPT = "alpha"
    SCALE_OPT = "scale"
    INVERT_OPT = "invert"
    SEED_OPT = "seed"
    
    START_DELAY_SEC = "start-delay"
    START_ABSOLUTE = "absolute"
    START_DURATION = "multiple-of-duration"

    DEADLINE_STRETCH_SEC = "deadline-stretch"
    DEADLINE_DURATION = "multiple-of-duration"
    DEADLINE_SLOWDOWN = "original-slowdown"
    DEADLINE_ABSOLUTE = "absolute"
    
    RATE_SEC = "user-rate"
    
    NODES_SEC = "nodes"
    RESOURCES_OPT = "resources"
    
    SOFTWARE_SEC = "software"

    
    def __init__(self, outfile, conffile):
        self.outfile = outfile
        self.conffile = conffile
        
        conffile = open(conffile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(conffile)
        
        self.startdelay_dist = self._get_dist(FileGenerator.START_DELAY_SEC)
        self.deadlinestretch_dist = self._get_dist(FileGenerator.DEADLINE_STRETCH_SEC)
        self.rate_dist = self._get_dist(FileGenerator.RATE_SEC)

        self.start_type = self.config.get(FileGenerator.START_DELAY_SEC, FileGenerator.TYPE_OPT)
        self.deadline_type = self.config.get(FileGenerator.DEADLINE_STRETCH_SEC, FileGenerator.TYPE_OPT)
        
        
    def _get_start(self, type, lease):
        if self.startdelay_dist == None:
            return None, None
        else:
            delta = self.startdelay_dist.get()
            if type == FileGenerator.START_ABSOLUTE:
                start = round_datetime_delta(TimeDelta(seconds=delta))
            elif type == FileGenerator.START_DURATION:
                start = round_datetime_delta(delta * lease.duration.requested)
            return start, delta

    def _get_deadline(self, type, lease, start):
        if self.deadlinestretch_dist == None:
            return None, None
        else:
            if type in (FileGenerator.DEADLINE_DURATION, FileGenerator.DEADLINE_SLOWDOWN):
                if type == FileGenerator.DEADLINE_DURATION:
                    tau = self.deadlinestretch_dist.get()
                    
                elif type == FileGenerator.DEADLINE_SLOWDOWN:
                    runtime = float(lease.extras["SWF_runtime"])
                    waittime = float(lease.extras["SWF_waittime"])
                    if runtime < 10: runtime = 10
                    slowdown = (waittime + runtime) / runtime
    
                    min = self.deadlinestretch_dist.min
                    max = self.deadlinestretch_dist.max
                    tau = self.deadlinestretch_dist.get()
                    
                    tau = (slowdown - 1)*((tau-min) / (max-min))
    
                deadline = round_datetime_delta(start + (1 + tau)*lease.duration.requested)                
            elif type == FileGenerator.DEADLINE_ABSOLUTE:
                wait = self.deadlinestretch_dist.get()
                deadline = round_datetime_delta(start + TimeDelta(seconds=wait) + lease.duration.requested)  
                
                tau = ((deadline - start) / lease.duration.requested) - 1                    
                    
            return deadline, tau

    def _get_software(self, lease):
        return None # TODO
    
    def _get_rate(self, lease):
        if self.rate_dist == None:
            return None
        else:
            if lease.user_id == -1:
                return self.rate_dist.get()
            else:
                if self.user_rates.has_key(lease.user_id):
                    return self.user_rates[lease.user_id]
                else:
                    rate = self.rate_dist.get()
                    self.user_rates[lease.user_id] = rate
                    return rate
    
    def _get_attributes(self):
        attributes = {}
        attrs = self.config.get(FileGenerator.GENERAL_SEC, FileGenerator.ATTRIBUTES_OPT)
        attrs = attrs.split(",")
        for attr in attrs:
            (k,v) = attr.split("=")
            attributes[k] = v
            
        return attributes
    
    def _get_dist(self, section):
        if self.config.has_section(section):
            return self.__create_distribution_from_section(section)
        else:
            return None
        
    def __create_distribution_from_section(self, section):
        dist_type = self.config.get(section, FileGenerator.DISTRIBUTION_OPT)
        min = self.config.get(section, FileGenerator.MIN_OPT)
        max = self.config.get(section, FileGenerator.MAX_OPT)
        
        if min == "unbounded":
            min = float("inf")
        else:
            min = float(min)
        if max == "unbounded":
            max = float("inf")
        else:
            max = float(max)
            
        if dist_type == "uniform":
            dist = stats.UniformDistribution(min, max)
        elif dist_type == "normal":
            mu = self.config.getfloat(section, FileGenerator.MEAN_OPT)
            sigma = self.config.getfloat(section, FileGenerator.STDEV_OPT)
            dist = stats.BoundedNormalDistribution(min,max,mu,sigma)
        elif dist_type == "bounded-pareto" or dist_type == "truncated-pareto":
            alpha = self.config.getfloat(section, FileGenerator.ALPHA_OPT)
            if self.config.has_option(section, FileGenerator.INVERT_OPT):
                invert = self.config.getboolean(section, FileGenerator.INVERT_OPT)
            else:
                invert = False
            if dist_type == "bounded-pareto":
                dist = stats.BoundedParetoDistribution(min,max,alpha,invert)
            else:
                scale = self.config.getfloat(section, FileGenerator.SCALE_OPT)
                dist = stats.TruncatedParetoDistribution(min,max,scale,alpha,invert)
                
            
        if self.config.has_option(section, FileGenerator.SEED_OPT):
            seed = self.config.getint(section, FileGenerator.SEED_OPT)
            dist.seed(seed)

        return dist

class LWFGenerator(FileGenerator):
    
    NUMLEASES_SEC = "numleases"    
    
    NUMLEASES_TYPE_UTILIZATION = "utilization"    
    NUMLEASES_UTILIZATION_OPT = "utilization"    
    NUMLEASES_LAST_REQUEST_OPT = "last-request"
        
    NUMLEASES_TYPE_ABSOLUTE = "absolute"    
    
    def __init__(self, outfile, conffile):
        FileGenerator.__init__(self, outfile, conffile)    
    
    def generate(self):
        print "Hello, LWF generator"
    
class LWFAnnotationGenerator(FileGenerator):
    
    def __init__(self, lwffile, outfile, conffile):
        FileGenerator.__init__(self, outfile, conffile)
        self.lwffile = lwffile
        
    def generate(self):
        lease_workload = LeaseWorkload.from_xml_file(self.lwffile)
        leases = lease_workload.get_leases()
        annotations = {}
        
        for lease in leases:
            lease_id = lease.id
            extra = {}
            
            start, delta = self._get_start(self.start_type, lease)
            if start != None:
                start = Timestamp(start)
                extra["simul_start_delta"] = "%.2f" % delta
                
            deadline, tau = self._get_deadline(self.deadline_type, lease, start.requested)
            if deadline != None:
                extra["simul_deadline_tau"] = "%.2f" % tau
            
            software = self._get_software(lease)
            
            rate = self._get_rate(lease)

            if rate != None:
                extra["simul_userrate"] = "%.2f" % rate
            
            annotation = LeaseAnnotation(lease_id, start, deadline, software, extra)
            annotations[lease_id] = annotation
            
        attributes = self._get_attributes()
        
        annotations = LeaseAnnotations(annotations, attributes)
        
        tree = ET.ElementTree(annotations.to_xml())
        outfile = open(self.outfile, "w")
        tree.write(outfile)
        outfile.close()
