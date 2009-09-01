# -------------------------------------------------------------------------- #
# Copyright 2006-2009, University of Chicago                                 #
# Copyright 2008-2009, Distributed Systems Architecture Group, Universidad   #
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

from haizea.core.manager import Manager
from haizea.common.utils import generate_config_name, unpickle
from haizea.core.configfile import HaizeaConfig, HaizeaMultiConfig
from haizea.core.accounting import AccountingDataCollection
from haizea.common.config import ConfigException
from haizea.cli.optionparser import OptionParser, Option
from haizea.cli import Command
from mx.DateTime import TimeDelta
import xml.etree.ElementTree as ET
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
        
            manager = Manager(config, daemon, pidfile)
        
            manager.start()
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
    """
    
    name = "haizea-convert-data"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-t", "--type", action="store",  dest="type",
                                         choices = ["per-run", "per-lease", "counter"],
                                         help = """
                                         Type of data to produce.
                                         """))
        self.optparser.add_option(Option("-c", "--counter", action="store",  dest="counter",
                                         help = """
                                         Counter to print out when using '--type counter'.
                                         """))
        self.optparser.add_option(Option("-f", "--format", action="store", type="string", dest="format",
                                         help = """
                                         Output format. Currently supported: csv
                                         """))
        self.optparser.add_option(Option("-l", "--list-counters", action="store_true",  dest="list_counters",
                                         help = """
                                         If specified, the command will just print out the names of counters
                                         stored in the data file and then exit, regardless of other parameters.
                                         """))
                
    def run(self):            
        self.parse_options()

        datafiles=self.args[1:]
        if len(datafiles) == 0:
            print "Please specify at least one datafile to convert"
            exit(1)
        
        datafile1 = unpickle(datafiles[0])
        
        counter_names = datafile1.counters.keys()
        attr_names = datafile1.attrs.keys()
        lease_stats_names = datafile1.lease_stats_names
        stats_names = datafile1.stats_names

        if self.opt.list_counters:
            for counter in counter_names:
                print counter
            exit(0)
        
        if self.opt.type == "per-run":
            header_fields = attr_names + stats_names
        elif self.opt.type == "per-lease":
            header_fields = attr_names + ["lease_id"] + lease_stats_names
        elif self.opt.type == "counter":
            counter = self.opt.counter
            if not datafile1.counters.has_key(counter):
                print "The specified datafile does not have a counter called '%s'" % counter
                exit(1)
            header_fields = attr_names + ["time", "value"]
            if datafile1.counter_avg_type[counter] != AccountingDataCollection.AVERAGE_NONE:
                header_fields.append("average")                

        header = ",".join(header_fields)
            
        print header
        
        for datafile in datafiles:
            data = unpickle(datafile)
        
            attrs = [data.attrs[attr_name] for attr_name in attr_names]
                        
            if self.opt.type == "per-run":
                fields = attrs + [`data.stats[stats_name]` for stats_name in stats_names]
                print ",".join(fields)
            elif self.opt.type == "per-lease":
                leases = data.lease_stats
                for lease_id, lease_stat in leases.items():
                    fields = attrs + [`lease_id`] + [`lease_stat.get(lease_stat_name,"")` for lease_stat_name in lease_stats_names]
                    print ",".join(fields)
            elif self.opt.type == "counter":
                for (time, lease_id, value, avg) in data.counters[counter]:
                    fields = attrs + [`time`, `value`]
                    if data.counter_avg_type[counter] != AccountingDataCollection.AVERAGE_NONE:
                        fields.append(`avg`)
                    print ",".join(fields)
                    


class haizea_lwf2xml(Command):
    """
    Converts old Haizea LWF file into new XML-based LWF format
    """
    
    name = "haizea-lwf2xml"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-i", "--in", action="store",  type="string", dest="inf",
                                         help = """
                                         Input file
                                         """))
        self.optparser.add_option(Option("-o", "--out", action="store", type="string", dest="outf",
                                         help = """
                                         Output file
                                         """))
                
    def run(self):            
        self.parse_options()

        infile = self.opt.inf
        outfile = self.opt.outf
        
        root = ET.Element("lease-workload")
        root.set("name", infile)
        description = ET.SubElement(root, "description")
        time = TimeDelta(seconds=0)
        id = 1
        requests = ET.SubElement(root, "lease-requests")
        
        
        infile = open(infile, "r")
        for line in infile:
            if line[0]!='#' and len(line.strip()) != 0:
                fields = line.split()
                submit_time = int(fields[0])
                start_time = int(fields[1])
                duration = int(fields[2])
                real_duration = int(fields[3])
                num_nodes = int(fields[4])
                cpu = int(fields[5])
                mem = int(fields[6])
                disk = int(fields[7])
                vm_image = fields[8]
                vm_imagesize = int(fields[9])
                
                
        
                lease_request = ET.SubElement(requests, "lease-request")
                lease_request.set("arrival", str(TimeDelta(seconds=submit_time)))
                if real_duration != duration:
                    realduration = ET.SubElement(lease_request, "realduration")
                    realduration.set("time", str(TimeDelta(seconds=real_duration)))
                
                lease = ET.SubElement(lease_request, "lease")
                lease.set("id", `id`)

                
                nodes = ET.SubElement(lease, "nodes")
                node_set = ET.SubElement(nodes, "node-set")
                node_set.set("numnodes", `num_nodes`)
                res = ET.SubElement(node_set, "res")
                res.set("type", "CPU")
                if cpu == 1:
                    res.set("amount", "100")
                else:
                    pass
                res = ET.SubElement(node_set, "res")
                res.set("type", "Memory")
                res.set("amount", `mem`)
                
                start = ET.SubElement(lease, "start")
                if start_time == -1:
                    lease.set("preemptible", "true")
                else:
                    lease.set("preemptible", "false")
                    exact = ET.SubElement(start, "exact")
                    exact.set("time", str(TimeDelta(seconds=start_time)))

                duration_elem = ET.SubElement(lease, "duration")
                duration_elem.set("time", str(TimeDelta(seconds=duration)))

                software = ET.SubElement(lease, "software")
                diskimage = ET.SubElement(software, "disk-image")
                diskimage.set("id", vm_image)
                diskimage.set("size", `vm_imagesize`)
                
                    
                id += 1
        tree = ET.ElementTree(root)
        print ET.tostring(root)
        #tree.write("page.xhtml")
#head = ET.SubElement(root, "head")

#title = ET.SubElement(head, "title")
#title.text = "Page Title"

#body = ET.SubElement(root, "body")
#body.set("bgcolor", "#ffffff")

#body.text = "Hello, World!"

# wrap it in an ElementTree instance, and save as XML
#tree = ET.ElementTree(root)

        


