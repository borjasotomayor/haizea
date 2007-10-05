import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.traces.generators import generateTrace
from workspace.haizea.common.utils import Option, OptionParser, generateScripts
from workspace.haizea.common.config import RMConfig, RMMultiConfig, TraceConfig
        
class Report(object):
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

        report(multiconfig, statsdir)

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

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = TraceConfig.fromFile(configfile)
        
        tracefile = opt.tracefile

        generateTrace(config, tracefile)     
        
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