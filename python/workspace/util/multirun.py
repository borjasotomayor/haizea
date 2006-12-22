import ConfigParser, os, sys
import workspace.ears.server as ears
from workspace.graphing.graph import Graph, PointGraph, StepGraph
import matplotlib
import pylab
from pickle import Pickler, Unpickler

MULTIRUN_SEC="multirun"
PROFILES_OPT="profiles"

GRAPHS_SEC="graphs"
UTILINDIV_OPT="utilization-individual"
UTILCOMB_OPT="utilization-combined"
UTILAVGCOMB_OPT="utilavg-combined"

ADMITINDIV_OPT="admission-individual"
ADMITCOMB_OPT="admission-combined"

BATCHCOMB_OPT="batch-combined"

OUTPUT_OPT="output"


class EARS(object):
    def __init__(self, config, tracefile):
        self.profiles = {}
        self.tracefile=tracefile
        self.config = config
        self.output = "x11"
        profilenames = [s.strip(' ') for s in config.get(MULTIRUN_SEC, PROFILES_OPT).split(",")]
    
        for profile in profilenames:
            profileconfig = ConfigParser.ConfigParser()
            commonsections = [s for s in config.sections() if s.startswith("common:")]
            profilesections = [s for s in config.sections() if s.startswith(profile +":")]
            sections = commonsections + profilesections
            for s in sections:
                s_noprefix = s.split(":")[1]
                items = config.items(s)
                if not profileconfig.has_section(s_noprefix):
                    profileconfig.add_section(s_noprefix)
                for item in items:
                    profileconfig.set(s_noprefix, item[0], item[1])
                    
            self.profiles[profile] = profileconfig
            
    def showGraph(self, graph, filename=None):
        if self.output=="x11":
            graph.show()
        elif self.output == "png":
            filename += ".png"
            print "Saving graph to file %s" % filename
            pylab.savefig(filename)
            pylab.gcf().clear()
    
    def multirun(self):
        utilindiv = self.config.getboolean(GRAPHS_SEC, UTILINDIV_OPT)
        utilcombined = self.config.getboolean(GRAPHS_SEC, UTILCOMB_OPT)
        utilavgcombined = self.config.getboolean(GRAPHS_SEC, UTILAVGCOMB_OPT)
        admittedindiv = self.config.getboolean(GRAPHS_SEC, ADMITINDIV_OPT)
        admittedcombined = self.config.getboolean(GRAPHS_SEC, ADMITCOMB_OPT)
        batchcombined = self.config.getboolean(GRAPHS_SEC, BATCHCOMB_OPT)
        
        if self.config.has_option(GRAPHS_SEC, OUTPUT_OPT):
            self.output = self.config.get(GRAPHS_SEC, OUTPUT_OPT)
        
        needutilstats = utilindiv or utilcombined or utilavgcombined
        needadmissionstats = admittedindiv or admittedcombined
        needbatchstats = batchcombined
        utilstats = {}
        acceptedstats = {}
        rejectedstats = {}
        batchstats = {}
        for profile in self.profiles.items():
            profilename = profile[0]
            profileconfig = profile[1]
            utilstatsfilename = profilename + "-utilization.dat"
            acceptedfilename = profilename + "-accepted.dat"
            rejectedfilename = profilename + "-rejected.dat"
            batchcompletedfilename = profilename + "-batchcompleted.dat"
            queuesizefilename = profilename + "-queuesize.dat"
            print "Running profile '%s'" % profilename
            
            forceRun = False
            mustRun = True
            stats = None
            accepted = None
            rejected = None
            batchcompleted = None
            queuesize = None
            if not forceRun:
                # Check if the data already exists. If so, we don't need to rerun
                if os.path.exists(utilstatsfilename):
                    file = open (utilstatsfilename, "r")
                    u = Unpickler(file)
                    stats = u.load()
                    file = open (acceptedfilename, "r")
                    u = Unpickler(file)
                    accepted = u.load()
                    file = open (rejectedfilename, "r")
                    u = Unpickler(file)
                    rejected = u.load()
                    file = open (batchcompletedfilename, "r")
                    u = Unpickler(file)
                    batchcompleted = u.load()
                    file = open (queuesizefilename, "r")
                    u = Unpickler(file)
                    queuesize = u.load()
                    mustRun = False
                    print "No need to run (data already saved from previous run)"
            if mustRun:
                s = ears.createEARS(profileconfig, self.tracefile)
                s.start()

                stats = s.generateUtilizationStats(s.startTime,s.time)
                accepted = [((v[0] - s.startTime).seconds,v[1]) for v in s.accepted]
                rejected = [((v[0] - s.startTime).seconds,v[1]) for v in s.rejected]
                batchcompleted = [((v[0] - s.startTime).seconds,v[1]) for v in s.batchvmcompleted]
                queuesize = [((v[0] - s.startTime).seconds,v[1]) for v in s.queuesize]
                file = open (utilstatsfilename, "w")
                p = Pickler(file)
                p.dump(stats)
                file = open (acceptedfilename, "w")
                p = Pickler(file)
                p.dump(accepted)
                file = open (rejectedfilename, "w")
                p = Pickler(file)
                p.dump(rejected)
                file = open (batchcompletedfilename, "w")
                p = Pickler(file)
                p.dump(batchcompleted)
                file = open (queuesizefilename, "w")
                p = Pickler(file)
                p.dump(queuesize)

            if needutilstats:
                if utilcombined or utilavgcombined:
                    utilstats[profilename] = stats
                if utilindiv:
                    utilization = [(s[0],s[1]) for s in stats]
                    utilavg = [(s[0],s[2]) for s in stats]
                    g1 = StepGraph([utilization], "Time (s)", "Utilization")
                    g2 = PointGraph([utilavg], "Time (s)", "Utilization")
                    g1.plot()
                    g2.plot()
                    pylab.legend(["Utilization","Average"])
                    g1.show()
            if needadmissionstats:
                if admittedcombined:
                    acceptedstats[profilename] = accepted
                    rejectedstats[profilename] = rejected
                if admittedindiv:
                    g1 = StepGraph([accepted,rejected], "Time (s)", "Requests")
                    g1.plot()
                    pylab.ylim(0,s.acceptednum+1)
                    pylab.legend(["Accepted","Rejected"])
                    g1.show()
            if needbatchstats:
                batchstats[profilename] = batchcompleted
                
        runtime = {}
        utilization = {}
        
        for profile in self.profiles.items():        
            profilename = profile[0]
            utilization[profilename] = utilstats[profilename][-1][2]
            runtime[profilename] = batchstats[profilename][-1][0]

        profilenames = self.profiles.keys()
        profilenames.sort()
        csv = csvheader = ""
        print "UTILIZATION"
        print "-----------"
        for profilename in profilenames:
            print "%s: %.2f" % (profilename, utilization[profilename])
            csv += "%f," % utilization[profilename]
            csvheader+="%s," % profilename
            
        print ""
        print "EXECUTION TIME"
        print "--------------"
        for profilename in profilenames:
            print "%s: %.2f" % (profilename, runtime[profilename])
            csv += ",%f" % runtime[profilename]
            csvheader+=",%s" % profilename
            
        print "CSVHEADER:%s" % csvheader
        print "CSV:%s" % csv
        print ""
                
        if utilcombined:
            utilization = [[(w[0],w[1]) for w in v] for v in utilstats.values()]
            g1 = PointGraph(utilization, "Time (s)", "Utilization")
            g1.plot()
            pylab.ylim(0, 1.05)
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            pylab.legend(utilstats.keys(), loc='lower center')
            self.showGraph(g1, "graph-utilization")

        if utilavgcombined:
            average = [[(w[0],w[2]) for w in v] for v in utilstats.values()]
            g1 = PointGraph(average, "Time (s)", "Utilization (Avg)")
            g1.plot()
            pylab.ylim(0, 1.05)
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            pylab.legend(utilstats.keys(), loc='lower right')
            self.showGraph(g1, "graph-utilizationavg")
                
        if admittedcombined:
            g1 = StepGraph(acceptedstats.values() + rejectedstats.values(), "Time (s)", "Requests")
            g1.plot()
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            lengths = [len(l) for l in acceptedstats.values() + rejectedstats.values()]
            pylab.ylim(0, max(lengths)+1)
            legends  = ["Accepted " + v for v in acceptedstats.keys()]
            legends += ["Rejected " + v for v in rejectedstats.keys()]
            pylab.legend(legends)
            self.showGraph(g1, "graph-admitted")
            
        if batchcombined:
            g1 = PointGraph(batchstats.values(), "Time (s)", "Batch VWs completed")
            g1.plot()
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            lengths = [len(l) for l in batchstats.values()]
            pylab.ylim(0, max(lengths)+1)
            pylab.legend(batchstats.keys(), loc='lower right')
            self.showGraph(g1, "graph-batchcompleted")

    
if __name__ == "__main__":
    configfile="ears-multirun-utilization.conf"
    tracefile="../ears/test_mixed.trace"
    file = open (configfile, "r")
    config = ConfigParser.ConfigParser()
    config.readfp(file)        
    e = EARS(config, tracefile)
    e.multirun()