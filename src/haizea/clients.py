# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                       #
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

import haizea.common.constants as constants
from haizea.resourcemanager.rm import ResourceManager
from haizea.traces.generators import generateTrace, generateImages
from haizea.common.utils import Option, OptionParser, generateScripts
from haizea.common.config import RMConfig, RMMultiConfig, TraceConfig, GraphConfig, ImageConfig
from haizea.analysis.traces import analyzeARLeaseInjection
from haizea.analysis.misc import genpercentiles
import os.path

class Haizea(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        # Keeping this parameter for backwards compatibility
        # The preferred way of specifying the stats dir is through
        # the configuration file.
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir"))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = RMConfig.fromFile(configfile)
      
        # TODO: If has option, override in RM
        statsdir = opt.statsdir
        
        rm = ResourceManager(config)
    
        rm.start()

class Report(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        from haizea.analysis.report import Report

        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))
        p.add_option(Option("-t", "--html-only", action="store_true", dest="htmlonly"))
        p.add_option(Option("-m", "--mode", action="store", type="string", dest="mode", default="all"))

        opt, args = p.parse_args(argv)
        
        configfile=os.path.abspath(opt.conf)
        
        statsdir = opt.statsdir
        
        r = Report(configfile, statsdir, opt.htmlonly, opt.mode)
        r.generate()
        
class ReportSingle(object):
    def __init__(self, mode):
        self.mode = mode
    
    def run(self, argv):
        from haizea.analysis.report import Report
        from haizea.common.utils import genTraceInjName
        
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))
        p.add_option(Option("-t", "--html-only", action="store_true", dest="htmlonly"))
        p.add_option(Option("-p", "--profile", action="store", type="string", dest="profile"))
        p.add_option(Option("-r", "--trace", action="store", type="string", dest="trace", default=None))
        p.add_option(Option("-i", "--inj", action="store", type="string", dest="inj"))

        opt, args = p.parse_args(argv)
        
        configfile= os.path.abspath(opt.conf)
            
        statsdir = opt.statsdir
        
        if opt.trace != None:
            inj = opt.inj
            if inj == "None": inj = None
            trace = (opt.trace, inj, genTraceInjName(opt.trace,inj))
        else:
            trace = None
        
        r = Report(configfile, statsdir, opt.htmlonly, mode = self.mode)
        r.generate(onlyprofile=opt.profile, onlytrace=trace, configfilename = configfile)
                
class Graph(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        from haizea.analysis.report import Section
        
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        graphconfig = GraphConfig.fromFile(configfile)
        graphfile = configfile.split(".")[0]
            
        statsdir = opt.statsdir

        title = graphconfig.getTitle()
        titlex = graphconfig.getTitleX()
        titley = graphconfig.getTitleY()
        datafile = graphconfig.getDatafile()
        data = graphconfig.getDataEntries()
        graphtype = graphconfig.getGraphType()

        dirs = []
        for d in data:
            dirs.append((d.title, statsdir + "/" + d.dirname))
        
        s = Section(title, datafile, graphtype)
        
        s.loadData(dict(dirs), profilenames=[v[0] for v in dirs])
        s.generateGraph("./", filename=graphfile, titlex=titlex, titley=titley)
        
        
class GenPercentiles(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        from haizea.analysis.main import report

        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = RMMultiConfig.fromFile(configfile)
            
        statsdir = opt.statsdir

        genpercentiles(multiconfig, statsdir)


     
class TraceGenerator(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-g", "--guaranteeavg", action="store_true", dest="guaranteeavg"))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = TraceConfig.fromFile(configfile)
        
        tracefile = opt.tracefile

        generateTrace(config, tracefile, opt.guaranteeavg)     

class ImageGenerator(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--imagefile", action="store", type="string", dest="imagefile", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = ImageConfig.fromFile(configfile)
        
        imagefile = opt.imagefile

        generateImages(config, imagefile)           
        
class GenScripts(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True))
        p.add_option(Option("-m", "--only-missing", action="store_true",  dest="onlymissing"))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = RMMultiConfig.fromFile(configfile)
        
        dir = opt.dir

        generateScripts(configfile, multiconfig, dir, onlymissing=opt.onlymissing)
        
class InjectionAnalyzer(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-f", "--injectionfile", action="store", type="string", dest="injectionfile", required=True))

        opt, args = p.parse_args(argv)
        
        injectionfile = opt.injectionfile

        analyzeARLeaseInjection(injectionfile)  