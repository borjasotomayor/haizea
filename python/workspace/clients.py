import ConfigParser, sys
from workspace.traces import files, cooker, jazz
from workspace.graphing import graph
from workspace.util import stats, multirun
from workspace.util.miscutil import *


class TraceGraph(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-t", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-o", "--output", action="store", type="choice", dest="output", 
                     choices=["png","x11"], default="x11"))
        p.add_option(Option("-f", "--outputfile", action="store", type="string", dest="outputfile"))
        
        opt, args = p.parse_args(argv)
        
        
        #TODO: Lots of error checking
    
        #TODO: Customize what makes it into the figure
        
        trace = files.TraceFile.fromFile(opt.tracefile)
        fig = graph.Figure()
        schedGraph = trace.toScheduleGraph()
        imageGraph = trace.toImageHistogram()
        durGraph = trace.toDurationHistogram()
        fig.addGraph(imageGraph,2,2,1)
        fig.addGraph(durGraph,2,2,2)
        fig.addGraph(schedGraph,2,1,2)

        fig.plot()
    
        if opt.output == "x11":
            fig.show()
        elif opt.output == "png":
            pass

class SWF2Trace(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--swffile", action="store", type="string", dest="swffile", required=True))
        
        opt, args = p.parse_args(argv)
        c = cooker.SWF2TraceConf.fromFile(opt.conf)
        swf = files.SWFFile.fromFile(opt.swffile)
        
        trace = swf.toTrace(imageDist=c.imageDist, imageSizes=c.imageSizes, maxnodes=c.maxNodes, maxduration=c.maxDuration, range=c.range, queue=c.queue, partition=c.partition)
 
        trace.toFile(sys.stdout)

class Cooker(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-r", "--rejected-file", action="store", type="string", dest="rejectedFile", default=""))
        p.add_option(Option("-a", "--admission-control", action="store_true", dest="admission"))
        p.add_option(Option("-g", "--ratio-guarantee", action="store_true", dest="ratio"))
        p.add_option(Option("-m", "--max-attempts", action="store", type="int", dest="attempts", default=10))
        p.add_option(Option("-n", "--nodes", action="store", type="int", dest="nodes", default=-1))
        p.set_defaults(admission=False)

        opt, args = p.parse_args(argv)
        c = cooker.Cooker(opt.conf)
        
        done = False
        attempt = 1
        while not done and attempt <= opt.attempts:
            result = c.generateTrace()
            trace = result[0]
            totalDurationAR = result[1]
            totalDurationBatch = result[2]

            if opt.nodes>0:
                totalDuration = totalDurationAR + totalDurationBatch
                totalDuration = totalDuration / opt.nodes

                maxDuration = c.conf.traceDuration
                if totalDuration < maxDuration:
                    sys.stderr.write("ATTEMPT #%i: Insufficient duration. Expected >= %i   Real: %i\n" % (attempt, maxDuration, totalDuration))
                    attempt += 1
                    continue

                excessiveDuration = maxDuration * 1.05
                if totalDuration > excessiveDuration:
                    sys.stderr.write("ATTEMPT #%i: Excessive duration. Expected <= %.2f   Real: %i\n" % (attempt, excessiveDuration, totalDuration))
                    attempt += 1
                    continue
            
            arPercent = float(c.conf.config.get(cooker.GENERAL_SEC, cooker.AR_OPT))/100
            batchPercent = float(c.conf.config.get(cooker.GENERAL_SEC, cooker.BATCH_OPT))/100
            
            arPercentReal = float(totalDurationAR) / (totalDurationBatch+totalDurationAR)
            batchPercentReal = float(totalDurationBatch) / (totalDurationBatch+totalDurationAR)
            
            if abs(arPercent - arPercentReal) <= 0.015 or not opt.ratio:
                done = True
                if opt.admission:
                    bandwidth = c.conf.bandwidth
                    ac = cooker.OfflineAdmissionControl(trace, bandwidth, c.conf.admissioncontrol, c.conf.numNodesDist, c.conf.numc, c.conf.winsize)
                    (accepted, rejected) = ac.filterInfeasible()
                    accepted.toFile(sys.stdout)
                    if opt.rejectedFile != "" and len(rejected.entries) > 0:
                        file = open(opt.rejectedFile,"w")
                        rejected.toFile(file)                    
                else:
                    sys.stderr.write("FOUND: Expected: %f/%f   Real: %f/%f (diff=%.6f)\n" % (batchPercent, arPercent, batchPercentReal, arPercentReal, abs(arPercent - arPercentReal)))
                    trace.toFile(sys.stdout)
                    return 0
            else:
                sys.stderr.write("ATTEMPT #%i: Expected: %f/%f   Real: %f/%f\n" % (attempt, batchPercent, arPercent, batchPercentReal, arPercentReal))
                attempt += 1
        
        return 1
        
class EARSMultiRun(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-t", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-p", "--profile", action="store", type="string", dest="profile", default=None, required=False))

        opt, args = p.parse_args(argv)
        configfile=opt.conf
        tracefile=opt.tracefile
        profile=opt.profile
        
        file = open (configfile, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)        
        
        e = multirun.EARS(config, tracefile, profile)
        e.multirun()        
        
        
class EARSCompare(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-f", "--file", action="store", type="string", dest="file", required=True))
        p.add_option(Option("-c", "--challenger", action="store", type="string", dest="challenger", required=True))
        p.add_option(Option("-d", "--defender", action="store", type="string", dest="defender", required=True))
        p.add_option(Option("-g", "--gnuplot", action="store_true", dest="gnuplot"))

        opt, args = p.parse_args(argv)
        
        c = stats.EARSCompare(opt.file)
        
        r = c.compare(opt.challenger, opt.defender)
        
        if opt.gnuplot:
            xlabels = {}
            ylabels = {}
            zlabels = {}
            better = []
            worse = []
            equal = []
            for i,l in enumerate((xlabels,ylabels,zlabels)):
                aux = list(set([v[i] for v in r.keys()]))
                aux.sort()
                aux = dict([(v,aux.index(v)) for v in aux])
                l.update(aux)
            for k in r.keys():
                line = []
                line += k
                xnum = `xlabels[k[0]]`
                ynum = `ylabels[k[1]]`
                znum = `zlabels[k[2]]`
                line += (xnum,ynum,znum)
                line += [`r[k][opt.challenger]`]
                line += [`r[k][opt.defender]`]
                comparison = r[k]["comparison"]
                line += [`comparison`]
                pointsize = 1
                if abs(comparison) >= 0.20:
                    pointsize = 4
                elif abs(comparison) > 0:
                    pointsize = 1 + ((abs(comparison) * 5) * 3)
                elif abs(comparison) == 0:
                    pointsize = 1
                line += [`pointsize`]
                color = None
                if comparison > 0:
                    color = "0x00ff00"
                elif comparison < 0:
                    color = "Oxff0000"
                else:
                    color = "0xcccccc"
                line += [color]

                linestr = " ".join(line)
                if comparison > 0:
                    better += [linestr]
                elif comparison < 0:
                    worse += [linestr]
                else:
                    equal += [linestr]
            
            for line in better:
                print line
            print "\n"
            for line in worse:
                print line
            print "\n"
            for line in equal:
                print line
        else:
            print "VW duration,Resources requested by AR (%%),Proportion (Batch%%-AR%%),(1) %s,(2) %s,How much better is (1) than (2)?" % (opt.challenger, opt.defender)
            keys = r.keys()
            keys.sort()
            for k in keys:
                line=[]
                line+=k
                line += [`r[k][opt.challenger]`]
                line += [`r[k][opt.defender]`]
                comparison = r[k]["comparison"]
                comparison = "%.3f%%" % (comparison*100)
                line += [comparison]
                print ",".join(line)
                
                    
class Thermometer(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-t", "--trace", action="store", type="string", dest="trace", required=True))
        p.add_option(Option("-n", "--nodes", action="store", type="int", dest="nodes"))
        p.set_defaults(nodes=None)
        opt, args = p.parse_args(argv)
        
        trace = files.TraceFile.fromFile(opt.trace, entryType=files.TraceEntryV2)
        c = cooker.Thermometer(trace, opt.nodes)
        c.printStats()
        
class AdvanceAR(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-t", "--trace", action="store", type="string", dest="trace", required=True))
        
        opt, args = p.parse_args(argv)
        
        trace = files.TraceFile.fromFile(opt.trace, entryType=files.TraceEntryV2)
        trace = files.TraceFile.advanceAR(trace)
        trace.toFile(sys.stdout)

class Jazz2Trace(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--jazzfile", action="store", type="string", dest="jazzfile", required=True))
        p.add_option(Option("-n", "--normalize", action="store_true", dest="normalize"))
        p.add_option(Option("-d", "--debug", action="store_true", dest="debug"))
        
        opt, args = p.parse_args(argv)
        c = jazz.JazzConf(opt.conf)
        raw = jazz.RawLogFile(opt.jazzfile)
        
        jazzproc = jazz.LogFile(raw, c)
        
        if opt.debug:
            jazzproc.printRequests()
        else:
            trace = jazzproc.toTrace()
            if opt.normalize:
                trace.normalizeTimes()
            trace.toFile(sys.stdout)

 

if __name__ == "__main__":
    #tg = TraceGraph()
    #tg.run(sys.argv)
    
    s2t = SWF2Trace()
    s2t.run(sys.argv)