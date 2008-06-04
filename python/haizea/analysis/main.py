from workspace.haizea.common.config import RMMultiConfig
from workspace.haizea.analysis.report import Report
from workspace.haizea.common.utils import genDataDirName, genTraceInjName

def report(multiconfig, statsdir, htmlonly=False):
    r = Report(multiconfig, statsdir, htmlonly)
    r.generate()


if __name__ == "__main__":
    multiconfigfile="../configfiles/test_multiple.conf"
    multiconfig = MultiConfig(multiconfigfile)
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    reportdir="/home/borja/docs/uchicago/research/ipdps/results/report"
    report(multiconfig, tracefile, injectedfile, statsdir, reportdir)