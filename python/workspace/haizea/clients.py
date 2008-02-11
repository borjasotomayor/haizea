import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.traces.generators import generateTrace, generateImages
from workspace.haizea.common.utils import Option, OptionParser, generateScripts
from workspace.haizea.common.config import RMConfig, RMMultiConfig, TraceConfig, GraphConfig, ImageConfig
from workspace.haizea.analysis.traces import analyzeExactLeaseInjection
from workspace.haizea.analysis.misc import genpercentiles
import os.path



class Report(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        from workspace.haizea.analysis.report import Report

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
        from workspace.haizea.analysis.report import Report
        from workspace.haizea.common.utils import genTraceInjName
        
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
        from workspace.haizea.analysis.report import Section
        
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
        from workspace.haizea.analysis.main import report

        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = RMMultiConfig.fromFile(configfile)
            
        statsdir = opt.statsdir

        genpercentiles(multiconfig, statsdir)

class Simulate(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = RMConfig.fromFile(configfile)
      
        statsdir = opt.statsdir
        
        simulate(config, statsdir)
     
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

        analyzeExactLeaseInjection(injectionfile)  