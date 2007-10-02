from workspace.haizea.common.config import MultiConfig
from workspace.haizea.analysis.report import MultiProfileReport
from workspace.haizea.common.utils import genDataDirName

def report(multiconfig, tracefile, injectedfile, statsdir, outdir):

    profiles = multiconfig.getProfiles()
    css = multiconfig.getCSS()
    
    profilesdirs = dict([(p, statsdir + "/" + genDataDirName(p,tracefile,injectedfile)) for p in profiles])
    
    r = MultiProfileReport(profilesdirs, outdir, css)
    
    r.generate()


if __name__ == "__main__":
    multiconfigfile="../configfiles/test_multiple.conf"
    multiconfig = MultiConfig(multiconfigfile)
    tracefile="../traces/examples/test_besteffort.csv"
    injectedfile=None
    statsdir="/home/borja/docs/uchicago/research/ipdps/results"
    reportdir="/home/borja/docs/uchicago/research/ipdps/results/report"
    report(multiconfig, tracefile, injectedfile, statsdir, reportdir)