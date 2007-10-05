from workspace.haizea.common.config import RMMultiConfig
from workspace.haizea.analysis.report import Report
from workspace.haizea.common.utils import genDataDirName, genTraceInjName

def report(multiconfig, statsdir):
    confs = multiconfig.getConfigsToReport()
    profiles = set([c.getProfile() for c in confs])
    tracefiles = set([c.getTracefile() for c in confs])
    injectfiles = set([c.getInjectfile() for c in confs])

    css = multiconfig.getCSS()
    outdir = multiconfig.getReportDir()
    
    r = Report(list(profiles), list(tracefiles), list(injectfiles), statsdir, outdir, css)
    r.generate()


if __name__ == "__main__":
    multiconfigfile="../configfiles/test_multiple.conf"
    multiconfig = MultiConfig(multiconfigfile)
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    reportdir="/home/borja/docs/uchicago/research/ipdps/results/report"
    report(multiconfig, tracefile, injectedfile, statsdir, reportdir)