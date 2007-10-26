import math
import os
from workspace.haizea.common.utils import genDataDirName, genTraceInjName
from pickle import Unpickler
import workspace.haizea.common.constants as constants

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1


# TODO integrate this in report.py


def genpercentiles(config, statsdir):
    confs = config.getConfigsToReport()

    profiles = list(set([c.getProfile() for c in confs]))
    profiles.sort()

    tracefiles = set([c.getTracefile() for c in confs])
    injectfiles = set([c.getInjectfile() for c in confs])
    traces = []
    for t in tracefiles:
        for i in injectfiles:
            traces.append((t,i,genTraceInjName(t,i)))        

    outdir = config.getReportDir()

    datafiles = [ constants.EXECWAITFILE ]
    percentiles = [0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.30, 0.35,
                   0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75,
                   0.8, 0.85, 0.9, 0.95, 0.99, 1.0]
        
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        
    for datafile in datafiles:
        for p in profiles:
            print "Generating %s percentiles for profile %s" % (datafile,p)
            csv = open(outdir + "%s_%s_percentiles.csv" % (p,datafile), "w")
            headers = ["profiles"] + [`pct` for pct in percentiles]
            csv.write(",".join(headers) + "\n")
            tracesdirs = [(t[2], statsdir + "/" + genDataDirName(p,t[0],t[1])) for t in traces]
            tracesdirs = dict([(t,d) for t,d in tracesdirs if os.path.exists(d)])
            if len(tracesdirs) > 0:
                keys = tracesdirs.keys()
                keys.sort()
                for k in keys:
                    dir = tracesdirs[k]
                    file = open (dir + "/" + datafile, "r")
                    u = Unpickler(file)
                    data = u.load()
                    file.close()
                    sorted = [v[2] for v in data]
                    sorted.sort()
                    
                    line = [k]
                    for pct in percentiles:
                        line.append("%.3f" % percentile(sorted,pct))
                    csv.write(",".join(line) + "\n")
            csv.close()
                