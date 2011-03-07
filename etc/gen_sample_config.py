#!/usr/bin/python

from ConfigParser import RawConfigParser
from haizea.core.configfile import HaizeaConfig
from haizea.common.config import OPTTYPE_INT, OPTTYPE_FLOAT, OPTTYPE_STRING, OPTTYPE_BOOLEAN, OPTTYPE_DATETIME, OPTTYPE_TIMEDELTA 
import textwrap

HEADER_WIDTH=70

def gen_config_file(config, filename, verbose):
    f = open(filename, "w")
    
    for s in HaizeaConfig.sections:
        if verbose:
            f.write("# " + ("=" * HEADER_WIDTH) + " #\n")
            f.write("# " + (" " * HEADER_WIDTH) + " #\n")
            f.write("# " + ("[%s]" % s.name).center(HEADER_WIDTH) + " #\n")
            for line in textwrap.wrap(s.get_doc(), HEADER_WIDTH): 
                f.write("# " + line.ljust(HEADER_WIDTH) + " #\n")
            f.write("# " + (" " * HEADER_WIDTH) + " #\n")
            f.write("# " + ("=" * HEADER_WIDTH) + " #\n")
        
        if verbose:
            newline = "\n"
        else:
            newline = ""
        
        if config.has_section(s.name):
            f.write("[%s]\n%s" % (s.name, newline))
        else:
            if verbose:
                f.write("#[%s]\n%s" % (s.name, newline))
        
        for opt in s.options:
            if verbose:
                f.write("# Option: %s\n" % opt.name)

                if opt.required:
                    required_str = "Yes"
                else:
                    if opt.required_if:
                        required_str = "Only if "
                        conds = []
                        for r in opt.required_if:
                            sec,o = r[0]
                            val = r[1]
                            conds.append("[%s].%s == %s" % (sec,o, val))
                        required_str += ", ".join(conds)
                    else:
                        required_str = "No"
                        if opt.default:
                            required_str += " (default is %s)" % opt.default                
                
                f.write("# Required: %s\n" % required_str)
                                
                if opt.valid:
                    valid_str = ", ".join(opt.valid)
                else:
                    if opt.type == OPTTYPE_INT:
                        valid_str = "An integer number"
                    elif opt.type == OPTTYPE_FLOAT:
                        valid_str = "A real number"
                    elif opt.type == OPTTYPE_STRING:
                        valid_str = "Any string"
                    elif opt.type == OPTTYPE_BOOLEAN:
                        valid_str = "True or False"
                    elif opt.type == OPTTYPE_DATETIME:
                        valid_str = "An ISO timestamp: i.e., YYYY-MM-DD HH:MM:SS"
                    elif opt.type == OPTTYPE_TIMEDELTA:
                        valid_str = "A duration in the format HH:MM:SS"                
                
                f.write("# Valid values: %s\n" % valid_str)

                f.write("# \n")

                optdoc = opt.get_doc()
                optdoc = "# " + optdoc.replace('\n','\n# ')
                f.write("%s\n" % optdoc)
            
            if config.has_option(s.name, opt.name):
                f.write("%s: %s\n%s" % (opt.name, config.get(s.name, opt.name), newline))
            else:
                if verbose:
                    if opt.default:
                        default = opt.default
                    else:
                        default = ""
                    f.write("#%s: %s\n%s" % (opt.name, default, newline))
            
        f.write("\n")
            
    f.close()
        
        
# Sample configuration file for simulation w/ tracefile

sample_trace = RawConfigParser()
sample_trace.add_section("general")
sample_trace.set("general", "loglevel", "INFO")
sample_trace.set("general", "mode", "simulated")
sample_trace.set("general", "lease-preparation", "unmanaged")
sample_trace.set("general", "persistence-file", "none")

sample_trace.add_section("simulation")
sample_trace.set("simulation", "clock", "simulated")
sample_trace.set("simulation", "starttime", "2006-11-25 13:00:00")
sample_trace.set("simulation", "resources", "4  CPU:100 Memory:1024")
sample_trace.set("simulation", "imagetransfer-bandwidth", "100")

sample_trace.add_section("accounting")
sample_trace.set("accounting", "datafile", "/var/tmp/haizea/results.dat")
sample_trace.set("accounting", "probes", "ar best-effort immediate cpu-utilization")

sample_trace.add_section("scheduling")
sample_trace.set("scheduling", "backfilling", "aggressive")
sample_trace.set("scheduling", "policy-preemption", "ar-preempts-everything")
sample_trace.set("scheduling", "suspension", "all")
sample_trace.set("scheduling", "suspend-rate", "32")
sample_trace.set("scheduling", "resume-rate", "32")
sample_trace.set("scheduling", "migration", "yes")
sample_trace.set("scheduling", "wakeup-interval", "10")

sample_trace.add_section("deploy-imagetransfer")
sample_trace.set("deploy-imagetransfer", "transfer-mechanism", "multicast")
sample_trace.set("deploy-imagetransfer", "avoid-redundant-transfers", "True")
sample_trace.set("deploy-imagetransfer", "diskimage-reuse", "none")
sample_trace.set("deploy-imagetransfer", "diskimage-cache-size", "20480")

sample_trace.add_section("tracefile")
sample_trace.set("tracefile", "tracefile", "/usr/share/haizea/traces/sample.lwf")

sample_trace = HaizeaConfig(sample_trace)

gen_config_file(sample_trace.config, "sample_trace.conf", verbose = True)
gen_config_file(sample_trace.config, "sample_trace_barebones.conf", verbose = False)


# Sample configuration file for OpenNebula

sample_opennebula = RawConfigParser()
sample_opennebula.add_section("general")
sample_opennebula.set("general", "loglevel", "INFO")
sample_opennebula.set("general", "mode", "opennebula")
sample_opennebula.set("general", "lease-preparation", "unmanaged")

sample_opennebula.add_section("opennebula")
sample_opennebula.set("opennebula", "host", "localhost")

sample_opennebula.add_section("accounting")
sample_opennebula.set("accounting", "datafile", "/var/tmp/haizea/results.dat")
sample_opennebula.set("accounting", "probes", "ar best-effort immediate cpu-utilization")

sample_opennebula.add_section("scheduling")
sample_opennebula.set("scheduling", "backfilling", "aggressive")
sample_opennebula.set("scheduling", "policy-preemption", "ar-preempts-everything")
sample_opennebula.set("scheduling", "suspension", "all")
sample_opennebula.set("scheduling", "migration", "no")
sample_opennebula.set("scheduling", "suspendresume-exclusion", "global")
sample_opennebula.set("scheduling", "suspend-rate", "32")
sample_opennebula.set("scheduling", "resume-rate", "32")
sample_opennebula.set("scheduling", "migration", "yes")

sample_opennebula = HaizeaConfig(sample_opennebula)

gen_config_file(sample_opennebula.config, "sample_opennebula.conf", verbose = True)
gen_config_file(sample_opennebula.config, "sample_opennebula_barebones.conf", verbose = False)
