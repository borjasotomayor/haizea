import workspace.haizea.common.constants as constants
from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.analysis.main import report
from workspace.haizea.common.utils import Option, OptionParser
from workspace.haizea.common.config import Config, MultiConfig
        
class Report(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-t", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-i", "--injectfile", action="store", type="string", dest="injectfile", default=None, required=False))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))
        p.add_option(Option("-r", "--reportdir", action="store", type="string", dest="reportdir", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        multiconfig = MultiConfig(configfile)
            
        tracefile=opt.tracefile
        injectedfile=opt.injectfile
        
        statsdir = opt.statsdir

        reportdir = opt.reportdir

        report(multiconfig, tracefile, injectedfile, statsdir, reportdir)

class Simulate(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-t", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-f", "--traceformat", action="store", type="string", dest="traceformat", required=True))
        p.add_option(Option("-i", "--injectfile", action="store", type="string", dest="injectfile", default=None, required=False))
        p.add_option(Option("-s", "--statsdir", action="store", type="string", dest="statsdir", required=True))
        p.add_option(Option("-p", "--profile", action="store", type="string", dest="profile", default=None, required=False))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        if opt.profile != None:
            multiconfig = MultiConfig(configfile)
            configs = multiconfig.getConfigs()
            config = [c for c in configs if c.getProfile()==opt.profile][0]
        else:
            config = Config()
            config.loadFile(configfile)
            
        tracefile=opt.tracefile
        if opt.traceformat == "csv":
            tracetype = constants.TRACE_CSV
        elif opt.traceformat == "gwf":
            tracetype = constants.TRACE_GSW
        injectedfile=opt.injectfile
        
        statsdir = opt.statsdir
        
        simulate(config, tracefile, tracetype, injectedfile, statsdir)