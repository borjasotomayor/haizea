import optparse
import os.path
from mx import DateTime
from datetime import datetime
from math import ceil, floor
from cPickle import load, dump, HIGHEST_PROTOCOL
from errno import EEXIST

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

def generateScripts(multiconfigfilename, multiconfig, dir, onlymissing = False):
    configs = multiconfig.getConfigsToRun()
    dir = os.path.abspath(dir)    
    basedatadir = "/home/borja/docs/uchicago/research/experiments/haizea/data"
    condor = open(dir + "/condor_submit", "w")
    sh = open(dir + "/run.sh", "w")
    reportsh = open(dir + "/report.sh", "w")

    condor.write(generateCondorHeader(fastonly=True))
    
    sh.write("#!/bin/bash\n\n")
    
    for c in configs:
        profile = c.getProfile()
        tracefile = c.getTracefile()
        injfile = c.getInjectfile()
        name = genTraceInjName(tracefile, injfile)
        datadir = "%s/%s/%s" % (basedatadir, profile, name)
        if not onlymissing or not os.path.exists(datadir):
            configfile = dir + "/%s_%s.conf" % (profile, name)
            fc = open(configfile, "w")
            c.config.write(fc)
            fc.close()
            
            command = "/home/borja/bin/vw/haizea-simulate -c %s -s %s" % (configfile, basedatadir)
            
            condor.write(generateCondorQueueEntry(command=command, dir = dir))
            
            sh.write("echo Running profile=%s trace=%s\n" % (profile, name))
            sh.write("python2.5 %s\n" % command)
    
    reportsh.write("python2.5 /home/borja/bin/vw/haizea-report -c %s -s /home/borja/docs/uchicago/research/experiments/haizea/data\n" % multiconfigfilename)
    
    condor.close()
    sh.close()
    reportsh.close()
    

def generateCondorHeader(fastonly=False, logname="experiment"):
    exclude = ["sox", "nefarious", "cuckoo", "admiral", "fledermaus-2", "microbe", "merry", "berkshire"]
    condor = ""
    condor += "Universe   = vanilla\n"
    condor += "Executable = /home/borja/bin/python2.5\n"
    condor += "transfer_executable = false\n"
    condor += "getenv = true\n"
    req = "requirements = " + " && ".join(["Machine != \"%s.cs.uchicago.edu\"" % h for h in exclude])
    if fastonly:
        req += " && Mips >= 2000"
    condor += "%s\n" % req
    condor += "Log        = %s.log\n" % logname
    condor += "Output     = %s.$(Process).out\n" % logname
    condor += "Error      = %s.$(Process).error\n\n" % logname
    
    return condor

def generateCondorQueueEntry(command, dir):    
    condor  = ""
    condor += "remote_initialdir=%s\n" % dir
    condor += "Arguments  = %s\n" % command
    condor += "Queue\n\n"
    return condor
    
    
def roundDateTimeDelta(d):
    return DateTime.DateTimeDelta(d.day, d.hour, d.minute, int(ceil(d.second)))

def roundDateTime(d):
    return DateTime.DateTime(d.year, d.month, d.day, d.hour, d.minute, int(floor(d.second+0.5)))

def UNIX2DateTime(t):
    d = datetime.fromtimestamp(t)
    return DateTime.DateTime(d.year,d.month,d.day,d.hour,d.minute,d.second)

    
def vnodemapstr(vnodes):
    if len(vnodes) == 0:
        return "UNUSED"
    else:
        return ",".join(["L"+`l`+"V"+`v` for (l,v) in vnodes])
    
# Based on http://norvig.com/python-iaq.html
def abstract():
    import inspect
    caller = inspect.stack()[1][3]
    raise NotImplementedError(caller + ' must be implemented in subclass')

def pickle(data, dir, file):
    f = open (dir + "/" + file, "w")
    dump(data, f, protocol = HIGHEST_PROTOCOL)
    f.close()
