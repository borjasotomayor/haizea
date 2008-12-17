# -------------------------------------------------------------------------- #
# Copyright 2006-2008, University of Chicago                                 #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

from haizea.resourcemanager.rm import ResourceManager
from haizea.common.utils import generate_config_name, unpickle
from haizea.resourcemanager.configfile import HaizeaConfig, HaizeaMultiConfig
from haizea.common.config import ConfigException
from haizea.cli.optionparser import OptionParser, Option
from haizea.cli import Command
import haizea.common.defaults as defaults
import sys
import os
import errno
import signal
import time


class haizea(Command):
    """
    This is the main Haizea command. By default, it will start Haizea as a daemon, which
    can receive requests via RPC or interact with other components such as OpenNebula. It can
    also start as a foreground process, and write all log messages to the console. All
    Haizea options are specified through the configuration file."""
    
    name = "haizea"
    
    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-c", "--conf", action="store", type="string", dest="conf",
                                         help = """
                                         The location of the Haizea configuration file. If not
                                         specified, Haizea will first look for it in
                                         /etc/haizea/haizea.conf and then in ~/.haizea/haizea.conf.
                                         """))
        self.optparser.add_option(Option("-f", "--fg", action="store_true",  dest="foreground",
                                         help = """
                                         Runs Haizea in the foreground.
                                         """))
        self.optparser.add_option(Option("--stop", action="store_true",  dest="stop",
                                         help = """
                                         Stops the Haizea daemon.
                                         """))
                
    def run(self):
        self.parse_options()

        pidfile = defaults.DAEMON_PIDFILE # TODO: Make configurable

        if self.opt.stop == None:
            # Start Haizea
             
            # Check if a daemon is already running
            if os.path.exists(pidfile):
                pf  = file(pidfile,'r')
                pid = int(pf.read().strip())
                pf.close()
     
                try:
                    os.kill(pid, signal.SIG_DFL)
                except OSError, (err, msg):
                    if err == errno.ESRCH:
                        # Pidfile is stale. Remove it.
                        os.remove(pidfile)
                    else:
                        msg = "Unexpected error when checking pid file '%s'.\n%s\n" %(pidfile, msg)
                        sys.stderr.write(msg)
                        sys.exit(1)
                else:
                    msg = "Haizea seems to be already running (pid %i)\n" % pid
                    sys.stderr.write(msg)
                    sys.exit(1)
     
            try:
                configfile=self.opt.conf
                if configfile == None:
                    # Look for config file in default locations
                    for loc in defaults.CONFIG_LOCATIONS:
                        if os.path.exists(loc):
                            config = HaizeaConfig.from_file(loc)
                            break
                    else:
                        print >> sys.stdout, "No configuration file specified, and none found at default locations."
                        print >> sys.stdout, "Make sure a config file exists at:\n  -> %s" % "\n  -> ".join(defaults.CONFIG_LOCATIONS)
                        print >> sys.stdout, "Or specify a configuration file with the --conf option."
                        exit(1)
                else:
                    config = HaizeaConfig.from_file(configfile)
            except ConfigException, msg:
                print >> sys.stderr, "Error in configuration file:"
                print >> sys.stderr, msg
                exit(1)
                
            daemon = not self.opt.foreground
        
            rm = ResourceManager(config, daemon, pidfile)
        
            rm.start()
        elif self.opt.stop: # Stop Haizea
            # Based on code in:  http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/66012
            try:
                pf  = file(pidfile,'r')
                pid = int(pf.read().strip())
                pf.close()
            except IOError:
                msg = "Could not stop, pid file '%s' missing.\n"
                sys.stderr.write(msg % pidfile)
                sys.exit(1)
            try:
               while 1:
                   os.kill(pid, signal.SIGTERM)
                   time.sleep(1)
            except OSError, err:
               err = str(err)
               if err.find("No such process") > 0:
                   os.remove(pidfile)
               else:
                   print str(err)
                   sys.exit(1)

class haizea_generate_configs(Command):
    """
    Takes an Haizea multiconfiguration file and generates the individual
    configuration files. See the Haizea manual for more details on multiconfiguration
    files."""
    
    name = "haizea-generate-configs"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True,
                                         help = """
                                         Multiconfiguration file.
                                         """))
        self.optparser.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True,
                                         help = """
                                         Directory where the individual configuration files
                                         must be created.
                                         """))
                
    def run(self):    
        self.parse_options()
        
        configfile=self.opt.conf
        multiconfig = HaizeaMultiConfig.from_file(configfile)
        
        dir = self.opt.dir
        
        configs = multiconfig.get_configs()
        
        etcdir = os.path.abspath(dir)    
        if not os.path.exists(etcdir):
            os.makedirs(etcdir)
            
        for c in configs:
            profile = c.get_attr("profile")
            tracefile = c.get("tracefile")
            injfile = c.get("injectionfile")
            configname = generate_config_name(profile, tracefile, injfile)
            configfile = etcdir + "/%s.conf" % configname
            fc = open(configfile, "w")
            c.config.write(fc)
            fc.close()

class haizea_generate_scripts(Command):
    """
    Generates a script, based on a script template, to run all the individual 
    configuration files generated by haizea-generate-configs. This command 
    requires Mako Templates for Python (http://www.makotemplates.org/)."""
    
    name = "haizea-generate-scripts"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True,
                                         help = """
                                         Multiconfiguration file used in haizea-generate-configs.
                                         """))
        self.optparser.add_option(Option("-d", "--confdir", action="store", type="string", dest="confdir", required=True,
                                         help = """
                                         Directory containing the individual configuration files.
                                         """))
        self.optparser.add_option(Option("-t", "--template", action="store", type="string", dest="template", required=True,
                                         help = """
                                         Script template (sample templates are included in /usr/share/haizea/etc)
                                         """))
        self.optparser.add_option(Option("-m", "--only-missing", action="store_true",  dest="onlymissing",
                                         help = """
                                         If specified, the generated script will only run the configurations
                                         that have not already produced a datafile. This is useful when some simulations
                                         fail, and you don't want to have to rerun them all.
                                         """))
                
    def run(self):        
        self.parse_options()
        
        configfile=self.opt.conf
        multiconfig = HaizeaMultiConfig.from_file(configfile)
                
        try:
            from mako.template import Template
        except:
            print "You need Mako Templates for Python to run this command."
            print "You can download them at http://www.makotemplates.org/"
            exit(1)
    
        configs = multiconfig.get_configs()
        
        etcdir = os.path.abspath(self.opt.confdir)    
        if not os.path.exists(etcdir):
            os.makedirs(etcdir)
            
        templatedata = []    
        for c in configs:
            profile = c.get_attr("profile")
            tracefile = c.get("tracefile")
            injfile = c.get("injectionfile")
            datafile = c.get("datafile")
            configname = generate_config_name(profile, tracefile, injfile)
            if not self.opt.onlymissing or not os.path.exists(datafile):
                configfile = etcdir + "/%s.conf" % configname
                templatedata.append((configname, configfile))
    
        template = Template(filename=self.opt.template)
        print template.render(configs=templatedata, etcdir=etcdir)


class haizea_convert_data(Command):
    """
    Converts Haizea datafiles into another (easier to process) format.
    
    This command is still not fully implemented."""
    
    name = "haizea-convert-data"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-t", "--type", action="store",  dest="type",
                                         choices = ["per-experiment", "per-lease"],
                                         help = """
                                         Type of data to produce
                                         """))
        self.optparser.add_option(Option("-f", "--format", action="store", type="string", dest="format",
                                         help = """
                                         Output format. Currently supported: csv
                                         """))
                
    def run(self):            
        self.parse_options()

        datafiles=self.args[1:]
        if len(datafiles) == 0:
            print "Please specify at least one datafile to convert"
            exit(1)
        
        attr_names = unpickle(datafiles[0]).attrs.keys()

        if len(attr_names) == 0:
            header = ""
        else:
            header = ",".join(attr_names) + ","
        
        if self.opt.type == "per-experiment":
            header += "all-best-effort"
        elif self.opt.type == "per-lease":
            header += "lease_id,waiting_time,slowdown"
            
        print header
        
        for datafile in datafiles:
            stats = unpickle(datafile)
        
            attrs = ",".join([stats.attrs[attr_name] for attr_name in attr_names])
            
            if self.opt.type == "per-experiment":
                print attrs + "," + `(stats.get_besteffort_end() - stats.starttime).seconds`
            elif self.opt.type == "per-lease":
                waitingtimes = stats.get_waiting_times()
                slowdowns = stats.get_slowdowns()
                for lease_id in waitingtimes:
                    print ",".join(attrs + [`lease_id`, `waitingtimes[lease_id].seconds`, `slowdowns[lease_id]`])


