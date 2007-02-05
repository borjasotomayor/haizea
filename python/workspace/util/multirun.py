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

DISKUSAGEINDIV_OPT="diskusage-individual"
DISKUSAGECOMB_OPT="diskusage-combined"

BATCHCOMB_OPT="batch-combined"

OUTPUT_OPT="output"


class EARS(object):
    def __init__(self, config, tracefile):
        self.profiles = {}
        self.tracefile=tracefile
        self.config = config
        self.output = "x11"
        self.profilenames = [s.strip(' ') for s in config.get(MULTIRUN_SEC, PROFILES_OPT).split(",")]
    
        for profile in self.profilenames:
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
        diskusageindiv = self.config.getboolean(GRAPHS_SEC, DISKUSAGEINDIV_OPT)
        diskusagecombined = self.config.getboolean(GRAPHS_SEC, DISKUSAGECOMB_OPT)
        batchcombined = self.config.getboolean(GRAPHS_SEC, BATCHCOMB_OPT)
        
        if self.config.has_option(GRAPHS_SEC, OUTPUT_OPT):
            self.output = self.config.get(GRAPHS_SEC, OUTPUT_OPT)
        
        utilstats = {}
        acceptedstats = {}
        rejectedstats = {}
        acceptedidsstats = {}
        rejectedidsstats = {}
        batchstats = {}
        diskusagestats = {}
        for profile in self.profilenames:
            profilename = profile
            profileconfig = self.profiles[profile]
            utilstatsfilename = profilename + "-utilization.dat"
            acceptedfilename = profilename + "-accepted.dat"
            rejectedfilename = profilename + "-rejected.dat"
            diskusagefilename = profilename + "-diskusage.dat"
            batchcompletedfilename = profilename + "-batchcompleted.dat"
            queuesizefilename = profilename + "-queuesize.dat"
            print "Running profile '%s'" % profilename
            
            forceRun = False
            mustRun = True
            stats = None
            accepted = None
            rejected = None
            diskusage = None
            batchcompleted = None
            queuesize = None
            if not forceRun:
                # Check if the data already exists. If so, we don't need to rerun
                if os.path.exists(utilstatsfilename):
                    file = open (utilstatsfilename, "r")
                    u = Unpickler(file)
                    stats = u.load()
                    file.close()
                    file = open (acceptedfilename, "r")
                    u = Unpickler(file)
                    accepted = u.load()
                    file.close()
                    file = open (rejectedfilename, "r")
                    u = Unpickler(file)
                    rejected = u.load()
                    file.close()
                    file = open (diskusagefilename, "r")
                    u = Unpickler(file)
                    diskusage = u.load()
                    file.close()
                    file = open (batchcompletedfilename, "r")
                    u = Unpickler(file)
                    batchcompleted = u.load()
                    file.close()
                    file = open (queuesizefilename, "r")
                    u = Unpickler(file)
                    queuesize = u.load()
                    file.close()
                    mustRun = False
                    print "No need to run (data already saved from previous run)"
            if mustRun:
                s = ears.createEARS(profileconfig, self.tracefile)
                s.start()

                stats = s.generateUtilizationStats(s.startTime,s.time)
                accepted = [((v[0] - s.startTime).seconds,v[1]) for v in s.accepted]
                rejected = [((v[0] - s.startTime).seconds,v[1]) for v in s.rejected]
                batchcompleted = [((v[0] - s.startTime).seconds,v[1]) for v in s.batchvmcompleted]
                diskusage = [((v[0] - s.startTime).seconds,v[1],v[2]) for v in s.diskusage]
                queuesize = [((v[0] - s.startTime).seconds,v[1]) for v in s.queuesize]
                file = open (utilstatsfilename, "w")
                p = Pickler(file)
                p.dump(stats)
                file.close()
                file = open (acceptedfilename, "w")
                p = Pickler(file)
                p.dump(accepted)
                file.close()
                file = open (rejectedfilename, "w")
                p = Pickler(file)
                p.dump(rejected)
                file.close()
                file = open (diskusagefilename, "w")
                p = Pickler(file)
                p.dump(diskusage)
                file.close()
                file = open (batchcompletedfilename, "w")
                p = Pickler(file)
                p.dump(batchcompleted)
                file.close()
                file = open (queuesizefilename, "w")
                p = Pickler(file)
                p.dump(queuesize)
                file.close()

            # UTILIZATION
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
                
            # ARs ADMITTED/REJECTED
            acceptedstats[profilename] = accepted
            rejectedstats[profilename] = rejected
            if admittedindiv:
                g1 = StepGraph([accepted,rejected], "Time (s)", "Requests")
                g1.plot()
                pylab.ylim(0,s.acceptednum+1)
                pylab.legend(["Accepted","Rejected"])
                g1.show()
                
            # DISK USAGE
            # Compute maximum
            # TODO: Compute average, others?
            maxusage = []
            nodes = []
            for v in diskusage:
                if not v[1] in nodes:
                    nodes.append(v[1])
            usage = dict([(v,0) for v in nodes])
            for u in diskusage:
                time=u[0]
                nod_id=u[1]
                mbytes=u[2]
                
                usage[nod_id]=mbytes
                
                m = max(usage.values())
                if len(maxusage)>0 and maxusage[-1][0] == time:
                    maxusage[-1] = (time,m)
                else:
                    maxusage.append((time,m))
                
            diskusagestats[profilename] = maxusage
            if diskusageindiv:
                g1 = StepGraph([maxusage], "Time (s)", "Usage")
                g1.plot()
                #pylab.ylim(0,s.acceptednum+1)
                pylab.legend(["Max"])
                g1.show()

            # BATCH COMPLETED
            batchstats[profilename] = batchcompleted
            
            acceptedidsstats[profilename] = s.accepted_ids
            rejectedidsstats[profilename] = s.rejected_ids
                
        runtime = {}
        utilization = {}
        peakdiskusage = {}
        accepted = {}
        rejected = {}
        
        for profile in self.profilenames:        
            utilization[profile] = utilstats[profile][-1][2]
            if batchstats[profile]==[]:
                runtime[profile]=0
            else:
                runtime[profile] = batchstats[profile][-1][0]
            peakdiskusage[profile] = max([v[1] for v in diskusagestats[profile]])
            accepted[profile] = acceptedstats[profile][-1][1]
            rejected[profile] = rejectedstats[profile][-1][1]

        profilenames = self.profiles.keys()
        profilenames.sort()
        csvutil = csvruntime = csvaccept = csvreject = cvsdiskusage = ""
        
        # Print info to stdout
        
        print "ADVANCE RESERVATIONS"
        print "-----------"
        for profilename in profilenames:
            print profilename
            print "\tAccepted: %s" % ",".join([`id` for id in acceptedidsstats[profilename]])
            print "\tRejected: %s" % ",".join([`id` for id in rejectedidsstats[profilename]])
        
        print "UTILIZATION"
        print "-----------"
        for profilename in profilenames:
            print "%s: %.2f" % (profilename, utilization[profilename])
            
        print ""
        print "EXECUTION TIME"
        print "--------------"
        for profilename in profilenames:
            print "%s: %.2f" % (profilename, runtime[profilename])
            

        # Generate CSV files
        # TODO: Use csv module
        csvheader=",".join(profilenames)        
        for stats in [(utilization,"utilization"),(runtime,"runtime"),(accepted,"accepted"),(rejected,"rejected"),(peakdiskusage,"peakdiskusage")]:
            csvline=",".join(`stats[0][profilename]` for profilename in profilenames )
            filename="%s.csv" % stats[1]
            print "Saving CSV file %s" % filename
            f=open(filename, 'w')
            f.write("%s\n" % csvheader)
            f.write("%s\n" % csvline)
            f.close()
                
        if utilcombined:
            utilization = [[(w[0],w[1]) for w in v] for v in utilstats.values()]
            g1 = PointGraph(utilization, "Time (s)", "Utilization")
            g1.plot()
            pylab.ylim(0, 1.05)
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            pylab.legend(utilstats.keys(), loc='lower center')
            self.showGraph(g1, "graph-utilization")

        if utilavgcombined:
            values = [utilstats[profile] for profile in self.profilenames]
            average = [[(w[0],w[2]) for w in v] for v in values]
            g1 = PointGraph(average, "Time (s)", "Utilization (Avg)")
            g1.plot()
            pylab.ylim(0, 1.05)
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            pylab.legend(self.profilenames, loc='lower right')
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

        if diskusagecombined:
            values = [diskusagestats[profile] for profile in self.profilenames]
            g1 = StepGraph(values, "Time (s)", "Max disk used")
            g1.plot()
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            lengths = [len(l) for l in diskusagestats.values()]
            #pylab.ylim(0, max(lengths)+1)
            pylab.legend(self.profilenames, loc='upper right')
            self.showGraph(g1, "graph-diskusage")
            
        if batchcombined:
            values = [batchstats[profile] for profile in self.profilenames]
            g1 = PointGraph(values, "Time (s)", "Batch VWs completed")
            g1.plot()
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            lengths = [len(l) for l in batchstats.values()]
            pylab.ylim(0, max(lengths)+1)
            pylab.legend(self.profilenames, loc='lower right')
            self.showGraph(g1, "graph-batchcompleted")

    
if __name__ == "__main__":
    configfile="../ears/examples/ears-multirun.conf"
    tracefile="../ears/examples/test_predeploy1.trace"
    file = open (configfile, "r")
    config = ConfigParser.ConfigParser()
    config.readfp(file)        
    e = EARS(config, tracefile)
    e.multirun()