import ConfigParser, sys
from workspace.traces import files, cooker
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

        opt, args = p.parse_args(argv)
        configfile=opt.conf
        tracefile=opt.tracefile
        
        file = open (configfile, "r")
        config = ConfigParser.ConfigParser()
        config.readfp(file)        
        
        e = multirun.EARS(config, tracefile)
        e.multirun()        
                    
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

if __name__ == "__main__":
    #tg = TraceGraph()
    #tg.run(sys.argv)
    
    s2t = SWF2Trace()
    s2t.run(sys.argv)