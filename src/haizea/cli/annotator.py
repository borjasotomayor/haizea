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

import haizea.common.stats as stats
from haizea.core.leases import LeaseWorkload, LeaseAnnotation, LeaseAnnotations, UnmanagedSoftwareEnvironment, Timestamp
from haizea.common.utils import round_datetime_delta
from haizea.cli import Command
from haizea.cli.optionparser import Option
from mx.DateTime import TimeDelta, DateTimeDelta
import ConfigParser

try:
    import xml.etree.ElementTree as ET
except ImportError:
    # Compatibility with Python <=2.4
    import elementtree.ElementTree as ET 

class haizea_lwf_annotate(Command):
    
    name = "haizea-lwf-annotate"
    
    GENERAL_SEC = "general"
    START_DELAY_SEC = "start-delay"
    DEADLINE_STRETCH_SEC = "deadline-stretch"
    RATE_SEC = "user-rate"
    
    ATTRIBUTES_OPT = "attributes"
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
    
    START_ABSOLUTE = "absolute"
    START_DURATION = "multiple-of-duration"

    DEADLINE_DURATION = "multiple-of-duration"
    DEADLINE_SLOWDOWN = "original-slowdown"
    DEADLINE_ABSOLUTE = "absolute"
    
    def __init__(self, argv):
        Command.__init__(self, argv)
        self.optparser.add_option(Option("-i", "--in", action="store",  type="string", dest="inf", required=True,
                                         help = """
                                         LWF file
                                         """))
        self.optparser.add_option(Option("-o", "--out", action="store", type="string", dest="outf", required=True,
                                         help = """
                                         Annotation file
                                         """))
        self.optparser.add_option(Option("-c", "--conf", action="store", type="string", dest="conf",
                                         help = """
                                         ...
                                         """))
        
        self.user_rates = {}
        
    def run(self):
        self.parse_options()      
        
        infile = self.opt.inf
        outfile = self.opt.outf
        conffile = self.opt.conf
        
        conffile = open(conffile, "r")
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(conffile)
        
        self.startdelay_dist = self.__get_dist(haizea_lwf_annotate.START_DELAY_SEC)
        self.deadlinestretch_dist = self.__get_dist(haizea_lwf_annotate.DEADLINE_STRETCH_SEC)
        self.rate_dist = self.__get_dist(haizea_lwf_annotate.RATE_SEC)

        start_type = self.config.get(haizea_lwf_annotate.START_DELAY_SEC, haizea_lwf_annotate.TYPE_OPT)
        deadline_type = self.config.get(haizea_lwf_annotate.DEADLINE_STRETCH_SEC, haizea_lwf_annotate.TYPE_OPT)

        lease_workload = LeaseWorkload.from_xml_file(infile)
        leases = lease_workload.get_leases()
        annotations = {}
        
        for lease in leases:
            lease_id = lease.id
            extra = {}
            
            start, delta = self.__get_start(start_type, lease)
            if start != None:
                start = Timestamp(start)
                extra["simul_start_delta"] = "%.2f" % delta
                
            deadline, tau = self.__get_deadline(deadline_type, lease, start.requested)
            if deadline != None:
                extra["simul_deadline_tau"] = "%.2f" % tau
            
            software = self.__get_software(lease)
            
            rate = self.__get_rate(lease)

            if rate != None:
                extra["simul_userrate"] = "%.2f" % rate
            
            annotation = LeaseAnnotation(lease_id, start, deadline, software, extra)
            annotations[lease_id] = annotation
            
        attributes = {}
        attrs = self.config.get(haizea_lwf_annotate.GENERAL_SEC, haizea_lwf_annotate.ATTRIBUTES_OPT)
        attrs = attrs.split(",")
        for attr in attrs:
            (k,v) = attr.split("=")
            attributes[k] = v
        
        annotations = LeaseAnnotations(annotations, attributes)
        
        tree = ET.ElementTree(annotations.to_xml())
        outfile = open(outfile, "w")
        tree.write(outfile)
        outfile.close()
        
        
    def __get_start(self, type, lease):
        if self.startdelay_dist == None:
            return None, None
        else:
            delta = self.startdelay_dist.get()
            if type == haizea_lwf_annotate.START_ABSOLUTE:
                start = round_datetime_delta(TimeDelta(seconds=delta))
            elif type == haizea_lwf_annotate.START_DURATION:
                start = round_datetime_delta(delta * lease.duration.requested)
            return start, delta

    def __get_deadline(self, type, lease, start):
        if self.deadlinestretch_dist == None:
            return None, None
        else:
            if type in (haizea_lwf_annotate.DEADLINE_DURATION, haizea_lwf_annotate.DEADLINE_SLOWDOWN):
                if type == haizea_lwf_annotate.DEADLINE_DURATION:
                    tau = self.deadlinestretch_dist.get()
                    
                elif type == haizea_lwf_annotate.DEADLINE_SLOWDOWN:
                    runtime = float(lease.extras["SWF_runtime"])
                    waittime = float(lease.extras["SWF_waittime"])
                    if runtime < 10: runtime = 10
                    slowdown = (waittime + runtime) / runtime
    
                    min = self.deadlinestretch_dist.min
                    max = self.deadlinestretch_dist.max
                    tau = self.deadlinestretch_dist.get()
                    
                    tau = (slowdown - 1)*((tau-min) / (max-min))
    
                deadline = round_datetime_delta(start + (1 + tau)*lease.duration.requested)                
            elif type == haizea_lwf_annotate.DEADLINE_ABSOLUTE:
                wait = self.deadlinestretch_dist.get()
                deadline = round_datetime_delta(start + TimeDelta(seconds=wait) + lease.duration.requested)  
                
                tau = ((deadline - start) / lease.duration.requested) - 1                    
                    
            return deadline, tau

    def __get_software(self, lease):
        return None # TODO
    
    def __get_rate(self, lease):
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
    
    def __get_dist(self, section):
        if self.config.has_section(section):
            return self.__create_distribution_from_section(section)
        else:
            return None
        
    def __create_distribution_from_section(self, section):
        dist_type = self.config.get(section, haizea_lwf_annotate.DISTRIBUTION_OPT)
        min = self.config.get(section, haizea_lwf_annotate.MIN_OPT)
        max = self.config.get(section, haizea_lwf_annotate.MAX_OPT)
        
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
            mu = self.config.getfloat(section, haizea_lwf_annotate.MEAN_OPT)
            sigma = self.config.getfloat(section, haizea_lwf_annotate.STDEV_OPT)
            dist = stats.BoundedNormalDistribution(min,max,mu,sigma)
        elif dist_type == "bounded-pareto" or dist_type == "truncated-pareto":
            alpha = self.config.getfloat(section, haizea_lwf_annotate.ALPHA_OPT)
            if self.config.has_option(section, haizea_lwf_annotate.INVERT_OPT):
                invert = self.config.getboolean(section, haizea_lwf_annotate.INVERT_OPT)
            else:
                invert = False
            if dist_type == "bounded-pareto":
                dist = stats.BoundedParetoDistribution(min,max,alpha,invert)
            else:
                scale = self.config.getfloat(section, haizea_lwf_annotate.SCALE_OPT)
                dist = stats.TruncatedParetoDistribution(min,max,scale,alpha,invert)
                
            
        if self.config.has_option(section, haizea_lwf_annotate.SEED_OPT):
            seed = self.config.getint(section, haizea_lwf_annotate.SEED_OPT)
            dist.seed(seed)

        return dist