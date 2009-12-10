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

from haizea.common.stats import *
from haizea.core.leases import LeaseWorkload, LeaseAnnotation, LeaseAnnotations, UnmanagedSoftwareEnvironment
from haizea.cli import Command
from haizea.cli.optionparser import Option
from mx.DateTime import DateTimeDelta
import ConfigParser


class haizea_lwf_annotate(Command):
    
    name = "haizea-lwf-annotate"
    
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
        
    def run(self):
        self.parse_options()      
        
        infile = self.opt.inf
        outfile = self.opt.outf
        conffile = self.opt.conf
        
        conffile = open(conffile, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(conffile)

        lease_workload = LeaseWorkload.from_xml_file(infile)
        leases = lease_workload.get_leases()
        annotations = {}
        
        for lease in leases:
            lease_id = lease.id
            
            start = DateTimeDelta(0, 1)
            deadline = DateTimeDelta(0, 2)
            software = UnmanagedSoftwareEnvironment()
            extra = {"foo":"bar"}
            
            annotation = LeaseAnnotation(lease_id, start, deadline, software, extra)
            annotations[lease_id] = annotation
            
        annotations = LeaseAnnotations(annotations)
        
        print annotations.to_xml_string()
        
        
    def __createContinuousDistributionFromSection(self, section):
        distType = self.config.get(section, DISTRIBUTION_OPT)
        min = self.config.getfloat(section, MIN_OPT)
        max = self.config.get(section, MAX_OPT)
        if max == "unbounded":
            max = float("inf")
        if distType == "uniform":
            dist = stats.ContinuousUniformDistribution(min, max)
        elif distType == "normal":
            mu = self.config.getfloat(section, MEAN_OPT)
            sigma = self.config.getfloat(section, STDEV_OPT)
            dist = stats.ContinuousNormalDistribution(min,max,mu,sigma)
        elif distType == "pareto":
            pass 
        
        return dist