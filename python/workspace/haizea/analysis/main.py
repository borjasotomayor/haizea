from workspace.haizea.common.config import MultiConfig
from workspace.haizea.analysis.report import MultiProfileReport
from workspace.haizea.common.utils import genDataDirName

def report(configfile, tracefile, injectedfile, statsdir, outdir):
    config = MultiConfig(configfile)
    profiles = config.getProfiles()
    css = config.getCSS()
    
    profilesdirs = dict([(p, statsdir + "/" + genDataDirName(p,tracefile,injectedfile)) for p in profiles])
    
    r = MultiProfileReport(profilesdirs, outdir, css)
    
    r.generate()


if __name__ == "__main__":
    configfile="../configfiles/test_multiple.conf"
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    reportdir="/home/borja/docs/uchicago/research/ipdps/results/report"
    report(configfile, tracefile, injectedfile, statsdir, reportdir)