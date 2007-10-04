import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.analysis.main import report
from workspace.haizea.common.utils import Option, OptionParser, generateScripts
from workspace.haizea.common.config import Config, MultiConfig
        
class Report(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = MultiConfig(configfile)
            
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
        config = Config()
        config.loadFile(configfile)
      
        statsdir = opt.statsdir
        
        simulate(config, statsdir)
        
class GenScripts(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = MultiConfig(configfile)
        
        dir = opt.dir

        generateScripts(configfile, multiconfig, dir)