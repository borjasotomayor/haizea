import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.traces.generators import generateTrace
from workspace.haizea.common.utils import Option, OptionParser, generateScripts
from workspace.haizea.common.config import RMConfig, RMMultiConfig, TraceConfig, GraphConfig
from workspace.haizea.analysis.traces import analyzeExactLeaseInjection
from workspace.haizea.analysis.misc import genpercentiles




class Report(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        from workspace.haizea.analysis.main import report

        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))
        p.add_option(Option("-t", "--html-only", action="store_true", dest="htmlonly"))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = RMMultiConfig.fromFile(configfile)
            
        statsdir = opt.statsdir

        report(multiconfig, statsdir, opt.htmlonly)
        
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
        s.generateGraph("./", titlex=titlex, titley=titley)
        
        
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
        
class GenScripts(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = RMMultiConfig.fromFile(configfile)
        
        dir = opt.dir

        generateScripts(configfile, multiconfig, dir)
        
class InjectionAnalyzer(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-f", "--injectionfile", action="store", type="string", dest="injectionfile", required=True))

        opt, args = p.parse_args(argv)
        
        injectionfile = opt.injectionfile

        analyzeExactLeaseInjection(injectionfile)  