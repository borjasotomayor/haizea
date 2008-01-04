import optparse
import os.path
from mx import DateTime
from math import ceil

class Option (optparse.Option):
    ATTRS = optparse.Option.ATTRS + ['required']

    def _check_required (self):
        if self.required and not self.takes_value():
            raise OptionError(
                "required flag set for option that doesn't take a value",
                 self)

    # Make sure _check_required() is called from the constructor!
    CHECK_METHODS = optparse.Option.CHECK_METHODS + [_check_required]

    def process (self, opt, value, values, parser):
        optparse.Option.process(self, opt, value, values, parser)
        parser.option_seen[self] = 1


class OptionParser (optparse.OptionParser):

    def _init_parsing_state (self):
        optparse.OptionParser._init_parsing_state(self)
        self.option_seen = {}

    def check_values (self, values, args):
        for option in self.option_list:
            if (isinstance(option, Option) and
                option.required and
                not self.option_seen.has_key(option)):
                self.error("%s not supplied" % option)
        return (values, args)

def genTraceInjName(tracefile, injectedfile):
    tracename=tracefile.split("/")[-1].split(".")[0]
    
    if injectedfile != None:
        injectname=injectedfile.split("/")[-1].split(".")[0]
        name = tracename + "+" + injectname
    else:
        name = tracename
    
    return name

def genDataDirName(profile, tracefile, injectedfile):
    name = genTraceInjName(tracefile, injectedfile)
    return profile + "/" + name + "/"

def generateScripts(multiconfigfilename, multiconfig, dir):
    configs = multiconfig.getConfigsToRun()
    dir = os.path.abspath(dir)    
    condor = open(dir + "/condor_submit", "w")
    sh = open(dir + "/run.sh", "w")
    reportsh = open(dir + "/report.sh", "w")
    
    exclude = ["sox", "nefarious", "cuckoo", "admiral", "fledermaus-2", "microbe", "merry", "berkshire"]

    condor.write("Universe   = vanilla\n")
    condor.write("Executable = /home/borja/bin/python2.5\n")
    condor.write("transfer_executable = false\n")
    condor.write("getenv = true\n")
    req = "requirements = Mips >= 2000"
    for h in exclude:
        req += " && Machine != \"%s.cs.uchicago.edu\"" % h
    condor.write("%s\n" % req)
    condor.write("Log        = experiment-indiv.log\n")
    condor.write("Output     = experiment-indiv.$(Process).out\n")
    condor.write("Error      = experiment-indiv.$(Process).error\n\n")
    
    sh.write("#!/bin/bash\n\n")
    
    for c in configs:
        profile = c.getProfile()
        tracefile = c.getTracefile()
        injfile = c.getInjectfile()
        name = genTraceInjName(tracefile, injfile)
        configfile = dir + "/%s_%s.conf" % (profile, name)
        fc = open(configfile, "w")
        c.config.write(fc)
        fc.close()
        
        command = "/home/borja/bin/vw/haizea-simulate -c %s -s /home/borja/docs/uchicago/research/experiments/haizea/data" % configfile
        
        condor.write("remote_initialdir=%s\n" % dir)
        condor.write("Arguments  = %s\n" % command)
        condor.write("Queue\n\n")
        
        sh.write("python2.5 %s\n" % command)
    
    reportsh.write("python2.5 /home/borja/bin/vw/haizea-report -c %s -s /home/borja/docs/uchicago/research/experiments/haizea/data\n" % multiconfigfilename)
    
    condor.close()
    sh.close()
    reportsh.close()
    
def roundDateTimeDelta(d):
    return DateTime.DateTimeDelta(d.day, d.hour, d.minute, int(ceil(d.second)))

def vnodemapstr(vnodes):
    if len(vnodes) == 0:
        return "UNUSED"
    else:
        return ",".join(["L"+`l`+"V"+`v` for (l,v) in vnodes])