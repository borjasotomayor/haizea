import ConfigParser, os, sys
import workspace.ears.server as ears
from workspace.graphing.graph import Graph, PointGraph, StepGraph
import matplotlib
import pylab

MULTIRUN_SEC="multirun"
PROFILES_OPT="profiles"

GRAPHS_SEC="graphs"
UTILINDIV_OPT="utilization-individual"
UTILCOMB_OPT="utilization-combined"
UTILAVGCOMB_OPT="utilavg-combined"

ADMITINDIV_OPT="admission-individual"
ADMITCOMB_OPT="admission-combined"

class EARS(object):
    def __init__(self, config, tracefile):
        self.profiles = {}
        self.tracefile=tracefile
        self.config = config
        
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
    
    def multirun(self):
        utilindiv = self.config.getboolean(GRAPHS_SEC, UTILINDIV_OPT)
        utilcombined = self.config.getboolean(GRAPHS_SEC, UTILCOMB_OPT)
        utilavgcombined = self.config.getboolean(GRAPHS_SEC, UTILAVGCOMB_OPT)
        admittedindiv = self.config.getboolean(GRAPHS_SEC, ADMITINDIV_OPT)
        admittedcombined = self.config.getboolean(GRAPHS_SEC, ADMITCOMB_OPT)
        
        needutilstats = utilindiv or utilcombined or utilavgcombined
        needadmissionstats = admittedindiv or admittedcombined
        utilstats = {}
        acceptedstats = {}
        rejectedstats = {}
        for profile in self.profiles.items():
            profilename = profile[0]
            profileconfig = profile[1]
            s = ears.createEARS(profileconfig, tracefile)
            s.start()
            if needutilstats:
                stats = s.generateUtilizationStats(s.startTime,s.time)
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
                accepted = [((v[0] - s.startTime).seconds,v[1]) for v in s.accepted]
                rejected = [((v[0] - s.startTime).seconds,v[1]) for v in s.rejected]
                if admittedcombined:
                    acceptedstats[profilename] = accepted
                    rejectedstats[profilename] = rejected
                if admittedindiv:
                    g1 = StepGraph([accepted,rejected], "Time (s)", "Requests")
                    g1.plot()
                    pylab.ylim(0,s.acceptednum+1)
                    pylab.legend(["Accepted","Rejected"])
                    g1.show()
                
        if utilcombined:
            utilization = [[(w[0],w[1]) for w in v] for v in utilstats.values()]
            g1 = StepGraph(utilization, "Time (s)", "Utilization")
            g1.plot()
            pylab.gca().xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
            pylab.legend(utilstats.keys(), loc='lower center')
            g1.show()

        if utilavgcombined:
            average = [[(w[0],w[2]) for w in v] for v in utilstats.values()]
            g1 = PointGraph(average, "Time (s)", "Utilization (Avg)")
            g1.plot()
            pylab.legend(utilstats.keys(), loc='upper left')
            g1.show()
                
        if admittedcombined:
            g1 = StepGraph(acceptedstats.values() + rejectedstats.values(), "Time (s)", "Requests")
            g1.plot()
            lengths = [len(l) for l in acceptedstats.values() + rejectedstats.values()]
            pylab.ylim(0, max(lengths)+1)
            legends  = ["Accepted " + v for v in acceptedstats.keys()]
            legends += ["Rejected " + v for v in rejectedstats.keys()]
            pylab.legend(legends)
            g1.show()
            
            
            
            
    
if __name__ == "__main__":
    configfile="ears-multirun.conf"
    tracefile="../ears/test_mixed.trace"
    file = open (configfile, "r")
    config = ConfigParser.ConfigParser()
    config.readfp(file)        
    e = EARS(config, tracefile)
    e.multirun()