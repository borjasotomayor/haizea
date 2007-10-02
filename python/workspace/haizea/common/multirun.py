from workspace.haizea.resourcemanager.main import simulate
from workspace.haizea.analysis.main import report
from workspace.haizea.common.config import MultiConfig
from workspace.haizea.resourcemanager.datastruct import resetLeaseID
import workspace.haizea.common.constants as constants

def multirun(multiconfig, tracefile, tracetype, injectedfile, statsdir, reportdir):
    configs = multiconfig.getConfigs()
    
    for config in configs:
        print config.getProfile()
        simulate(config, tracefile, tracetype, injectedfile, statsdir)
        resetLeaseID()

    report(multiconfig, tracefile, injectedfile, statsdir, reportdir)

if __name__ == "__main__":
    configfile="../configfiles/test_multiple.conf"
    multiconfig = MultiConfig(configfile)
    tracefile="../traces/examples/test_beres2.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    reportdir="/home/borja/docs/uchicago/research/ipdps/results/report"    
    multirun(multiconfig, tracefile, constants.TRACE_CSV, injectedfile, statsdir, reportdir)