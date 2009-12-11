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
from mx.DateTime import DateTimeDelta
import ConfigParser


class haizea_lwf_annotate(Command):
    
    name = "haizea-lwf-annotate"
    
    START_DELAY_SEC = "start-delay"
    DEADLINE_STRETCH_SEC = "deadline-stretch"
    MARKUP_SEC = "user-markup"
    
    DISTRIBUTION_OPT = "distribution"
    MIN_OPT = "min"
    MAX_OPT = "max"
    MEAN_OPT = "mu"
    STDEV_OPT = "sigma"
    ALPHA_OPT = "alpha"
    INVERT_OPT = "invert"
    SEED_OPT = "seed"
    
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
        
        self.user_markups = {}
        
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
        self.markup_dist = self.__get_dist(haizea_lwf_annotate.MARKUP_SEC)

        lease_workload = LeaseWorkload.from_xml_file(infile)
        leases = lease_workload.get_leases()
        annotations = {}
        
        for lease in leases:
            lease_id = lease.id
            
            start = self.__get_start(lease)
            if start != None:
                start = Timestamp(start)
                
            deadline = self.__get_deadline(lease, start.requested)
           
            software = self.__get_software(lease)
            
            markup = self.__get_markup(lease)
           
            extra = {}
            if markup != None:
                extra["simul_pricemarkup"] = "%.2f" % markup
            
            annotation = LeaseAnnotation(lease_id, start, deadline, software, extra)
            annotations[lease_id] = annotation
            
        annotations = LeaseAnnotations(annotations)
        
        print annotations.to_xml_string()
        
    def __get_start(self, lease):
        if self.startdelay_dist == None:
            return None
        else:
            delta = self.startdelay_dist.get()
            start = round_datetime_delta(delta * lease.duration.requested)
            return start

    def __get_deadline(self, lease, start):
        if self.deadlinestretch_dist == None:
            return None
        else:
            tau = self.deadlinestretch_dist.get()
            deadline = round_datetime_delta(start + (1 + tau)*lease.duration.requested)
            return deadline

    def __get_software(self, lease):
        return None # TODO
    
    def __get_markup(self, lease):
        if self.markup_dist == None:
            return None
        else:
            if self.user_markups.has_key(lease.user_id):
                return self.user_markups[lease.user_id]
            else:
                markup = self.markup_dist.get()
                self.user_markups[lease.user_id] = markup
                return markup
    
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
        elif dist_type == "bounded-pareto":
            alpha = self.config.getfloat(section, haizea_lwf_annotate.ALPHA_OPT)
            if self.config.has_option(section, haizea_lwf_annotate.INVERT_OPT):
                invert = self.config.getboolean(section, haizea_lwf_annotate.INVERT_OPT)
            else:
                invert = False
            dist = stats.BoundedParetoDistribution(min,max,alpha,invert)
            
        if self.config.has_option(section, haizea_lwf_annotate.SEED_OPT):
            seed = self.config.getint(section, haizea_lwf_annotate.SEED_OPT)
            dist.seed(seed)

        return dist