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
from haizea.common.stats import percentile
from haizea.cli.optionparser import Option
from haizea.cli import Command
from mx.DateTime import TimeDelta, Parser
import haizea.common.defaults as defaults
import sys
import os
import errno
import signal
from time import sleep

try:
    import xml.etree.ElementTree as ET
except ImportError:
    # Compatibility with Python <=2.4
    import elementtree.ElementTree as ET 


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
                    sleep(1)
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
        
        etcdir = self.opt.dir
        
        configs = multiconfig.get_configs()
        
        etcdir = os.path.abspath(etcdir)    
        if not os.path.exists(etcdir):
            os.makedirs(etcdir)
            
        for c in configs:
            profile = c.get_attr("profile")
            tracefile = c.get("tracefile")
            injfile = c.get("injectionfile")
            annotationfile = c.get("annotationfile")
            configname = generate_config_name(profile, tracefile, annotationfile, injfile)
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
        except Exception, e:
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
            annotationfile = c.get("annotationfile")            
            configname = generate_config_name(profile, tracefile, annotationfile, injfile)
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
                fields = attrs + [str(data.stats[stats_name]) for stats_name in stats_names]
                print ",".join(fields)
            elif self.opt.type == "per-lease":
                leases = data.lease_stats
                for lease_id, lease_stat in leases.items():
                    fields = attrs + [`lease_id`] + [str(lease_stat.get(lease_stat_name,"")) for lease_stat_name in lease_stats_names]
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
        lease_id = 1
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
                lease.set("id", `lease_id`)

                
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
                
                    
                lease_id += 1
        tree = ET.ElementTree(root)
        print ET.tostring(root)


        
class haizea_swf2lwf(Command):
    """
    Converts Standard Workload Format (SWF, used in the Parallel Workloads Archive at
    http://www.cs.huji.ac.il/labs/parallel/workload/) to Lease Workload Format
    """
    
    name = "haizea-swf2lwf"

    def __init__(self, argv):
        Command.__init__(self, argv)
        
        self.optparser.add_option(Option("-i", "--in", action="store",  type="string", dest="inf", required=True,
                                         help = """
                                         Input file
                                         """))
        self.optparser.add_option(Option("-o", "--out", action="store", type="string", dest="outf", required=True,
                                         help = """
                                         Output file
                                         """))
        self.optparser.add_option(Option("-p", "--preemptible", action="store", type="string", dest="preemptible", required=True,
                                         help = """
                                         Should the leases be preemptable or not?
                                         """))
        self.optparser.add_option(Option("-f", "--from", action="store", type="string", dest="from_time", default="00:00:00:00",
                                         help = """
                                         This parameter, together with the --amount parameter, allows converting just
                                         an interval of the SWF file.
                                         
                                         This parameter must be a timestamp of the format DD:HH:MM:SS. Only jobs sumitted
                                         at or after that time will be converted.
                                         
                                         Default is 00:00:00:00
                                         """))        
        self.optparser.add_option(Option("-l", "--interval-length", action="store", type="string", dest="interval_length",
                                         help = """
                                         Length of the interval in format DD:HH:MM:SS. Default is to convert jobs until the
                                         end of the SWF file.
                                         """))        
        self.optparser.add_option(Option("-q", "--queues", action="store", type="string", dest="queues",
                                         help = """
                                         Only convert jobs from the specified queues
                                         """))
        self.optparser.add_option(Option("-m", "--memory", action="store", type="string", dest="mem",
                                         help = """
                                         Memory requested by jobs.
                                         """))
        self.optparser.add_option(Option("-s", "--scale", action="store", type="string", dest="scale",
                                         help = """
                                         Scale number of processors by 1/SCALE.
                                         """))        
        self.optparser.add_option(Option("-n", "--site", action="store", type="string", dest="site",
                                         help = """
                                         File containing site description
                                         """))        
                
    def run(self):            
        self.parse_options()

        infile = self.opt.inf
        outfile = self.opt.outf
        
        from_time = Parser.DateTimeDeltaFromString(self.opt.from_time)
        if self.opt.interval_length == None:
            to_time = None
        else:
            to_time = from_time + Parser.DateTimeDeltaFromString(self.opt.interval_length)

        root = ET.Element("lease-workload")
        root.set("name", infile)
        description = ET.SubElement(root, "description")
        description.text = "Created with haizea-swf2lwf %s" % " ".join(self.argv[1:])

        if self.opt.site != None:
            site_elem = ET.parse(self.opt.site).getroot()
            site_num_nodes = int(site_elem.find("nodes").find("node-set").get("numnodes"))
            root.append(site_elem)
        
        time = TimeDelta(seconds=0)
        requests = ET.SubElement(root, "lease-requests")
        
        slowdowns = []
        users = set()
        utilization = 0
        utilization_no_ramp = 0
        no_ramp_cutoff = from_time + ((to_time - from_time) * 0.05)
        
        infile = open(infile, "r")
        for line in infile:
            if line[0]!=';' and len(line.strip()) != 0:
                fields = line.split()
                
                # Unpack the job's attributes. The description of each field is
                # taken from the SWF documentation at
                # http://www.cs.huji.ac.il/labs/parallel/workload/swf.html
                
                # Job Number -- a counter field, starting from 1. 
                job_number = int(fields[0])
                
                # Submit Time -- in seconds. The earliest time the log refers to is zero, 
                # and is the submittal time the of the first job. The lines in the log are 
                # sorted by ascending submittal times. It makes sense for jobs to also be 
                # numbered in this order.
                submit_time = int(fields[1])

                # Wait Time -- in seconds. The difference between the job's submit time 
                # and the time at which it actually began to run. Naturally, this is only 
                # relevant to real logs, not to models.
                wait_time = int(fields[2])

                # Run Time -- in seconds. The wall clock time the job was running (end 
                # time minus start time).
                # We decided to use ``wait time'' and ``run time'' instead of the equivalent 
                # ``start time'' and ``end time'' because they are directly attributable to 
                # the scheduler and application, and are more suitable for models where only 
                # the run time is relevant.
                # Note that when values are rounded to an integral number of seconds (as 
                # often happens in logs) a run time of 0 is possible and means the job ran 
                # for less than 0.5 seconds. On the other hand it is permissable to use 
                # floating point values for time fields.
                run_time = int(fields[3])
                
                # Number of Allocated Processors -- an integer. In most cases this is also 
                # the number of processors the job uses; if the job does not use all of them, 
                # we typically don't know about it.
                num_processors_allocated = int(fields[4])
                
                # Average CPU Time Used -- both user and system, in seconds. This is the 
                # average over all processors of the CPU time used, and may therefore be 
                # smaller than the wall clock runtime. If a log contains the total CPU time 
                # used by all the processors, it is divided by the number of allocated 
                # processors to derive the average.
                avg_cpu_time = float(fields[5])
                
                # Used Memory -- in kilobytes. This is again the average per processor.
                used_memory = int(fields[6])
                
                # Requested Number of Processors.
                num_processors_requested = int(fields[7])
                
                # Requested Time. This can be either runtime (measured in wallclock seconds), 
                # or average CPU time per processor (also in seconds) -- the exact meaning 
                # is determined by a header comment. In many logs this field is used for 
                # the user runtime estimate (or upper bound) used in backfilling. If a log 
                # contains a request for total CPU time, it is divided by the number of 
                # requested processors.
                time_requested = int(fields[8])
                
                # Requested Memory (again kilobytes per processor).
                mem_requested = int(fields[9])
                
                # Status 1 if the job was completed, 0 if it failed, and 5 if cancelled. 
                # If information about chekcpointing or swapping is included, other values 
                # are also possible. See usage note below. This field is meaningless for 
                # models, so would be -1.
                status = int(fields[10])
                
                # User ID -- a natural number, between one and the number of different users.
                user_id = int(fields[11])
                
                # Group ID -- a natural number, between one and the number of different groups. 
                # Some systems control resource usage by groups rather than by individual users.
                group_id = int(fields[12])
                
                # Executable (Application) Number -- a natural number, between one and the number 
                # of different applications appearing in the workload. in some logs, this might 
                # represent a script file used to run jobs rather than the executable directly; 
                # this should be noted in a header comment.
                exec_number = int(fields[13])
                
                # Queue Number -- a natural number, between one and the number of different 
                # queues in the system. The nature of the system's queues should be explained 
                # in a header comment. This field is where batch and interactive jobs should 
                # be differentiated: we suggest the convention of denoting interactive jobs by 0.
                queue = int(fields[14])
                
                # Partition Number -- a natural number, between one and the number of different 
                # partitions in the systems. The nature of the system's partitions should be 
                # explained in a header comment. For example, it is possible to use partition 
                # numbers to identify which machine in a cluster was used.
                partition = int(fields[15])
                
                # Preceding Job Number -- this is the number of a previous job in the workload, 
                # such that the current job can only start after the termination of this preceding 
                # job. Together with the next field, this allows the workload to include feedback 
                # as described below.
                prec_job = int(fields[16])

                # Think Time from Preceding Job -- this is the number of seconds that should elapse 
                # between the termination of the preceding job and the submittal of this one. 
                prec_job_thinktime = int(fields[17])

                                
                # Check if we have to skip this job
                
                submit_time = TimeDelta(seconds=submit_time)
                
                if submit_time < from_time:
                    continue
                
                if to_time != None and submit_time > to_time:
                    break
                
                if run_time < 0 and status==5:
                    # This is a job that got cancelled while waiting in the queue
                    continue
                
                if self.opt.queues != None:
                    queues = [int(q) for q in self.opt.queues.split(",")]
                    if queue not in queues:
                        # Job was submitted to a queue we're filtering out
                        continue              
        
                if self.opt.scale != None:
                    num_processors_requested = int(num_processors_requested/int(self.opt.scale))
                    
                lease_request = ET.SubElement(requests, "lease-request")
                # Make submission time relative to starting time of trace
                lease_request.set("arrival", str(submit_time - from_time))

                if run_time == 0:
                    # As specified in the SWF documentation, a runtime of 0 means
                    # the job ran for less than a second, so we round up to 1.
                    run_time = 1 
                realduration = ET.SubElement(lease_request, "realduration")
                realduration.set("time", str(TimeDelta(seconds=run_time)))
                
                lease = ET.SubElement(lease_request, "lease")
                lease.set("id", `job_number`)

                
                nodes = ET.SubElement(lease, "nodes")
                node_set = ET.SubElement(nodes, "node-set")
                node_set.set("numnodes", `num_processors_requested`)
                res = ET.SubElement(node_set, "res")
                res.set("type", "CPU")
                res.set("amount", "100")

                res = ET.SubElement(node_set, "res")
                res.set("type", "Memory")
                if self.opt.mem != None:
                    res.set("amount", self.opt.mem)
                elif mem_requested != -1:
                    res.set("amount", `mem_requested / 1024`)
                else:
                    print "Cannot convert this file. Job #%i does not specify requested memory, and --memory parameter not specified" % job_number
                    exit(-1)
                    
                if run_time < 10:
                    run_time2 = 10.0
                else:
                    run_time2 = float(run_time)
                slowdown = (wait_time + run_time2) / run_time2
                slowdowns.append(slowdown)
                
                if not user_id in users:
                    users.add(user_id)
                    
                if submit_time + wait_time < to_time:
                    time_to_end = to_time - (submit_time + wait_time)
                    time_in_interval = min(run_time, time_to_end.seconds)
                    utilization += time_in_interval * num_processors_requested
                    if submit_time + wait_time > no_ramp_cutoff:
                        utilization_no_ramp += time_in_interval * num_processors_requested
                
                start = ET.SubElement(lease, "start")
                #lease.set("preemptible", self.opt.preemptible)
                lease.set("user", `user_id`)

                duration_elem = ET.SubElement(lease, "duration")
                duration_elem.set("time", str(TimeDelta(seconds=time_requested)))

                # No software environment specified. The annotator would have to be used to
                # add one (or an image file when running a simulation).
                software = ET.SubElement(lease, "software")
                diskimage = ET.SubElement(software, "none")
                
                # Add unused SWF attributes to the extra section, for future reference.
                extra = ET.SubElement(lease, "extra")
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_waittime")
                attr.set("value", `wait_time`)
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_runtime")
                attr.set("value", `run_time`)
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_avgcputime")
                attr.set("value", `avg_cpu_time`)
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_queue")
                attr.set("value", `queue`)
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_group")
                attr.set("value", `group_id`)
                attr = ET.SubElement(extra, "attr")
                attr.set("name", "SWF_execnumber")
                attr.set("value", `exec_number`)
                    
        tree = ET.ElementTree(root)
        
        outfile = open(outfile, "w")
        tree.write(outfile)
        
        infile.close()
        outfile.close()
        
        slowdowns.sort()

        total_capacity = site_num_nodes * (to_time - from_time).seconds
        utilization = float(utilization) / float(total_capacity)
        utilization_no_ramp = float(utilization_no_ramp) / float(total_capacity)

        print "SLOWDOWNS"
        print "---------"
        print "min: %.2f" % slowdowns[0]
        print "10p: %.2f" % percentile(slowdowns, 0.1)
        print "25p: %.2f" % percentile(slowdowns, 0.25)
        print "med: %.2f" % percentile(slowdowns, 0.5)
        print "75p: %.2f" % percentile(slowdowns, 0.75)
        print "90p: %.2f" % percentile(slowdowns, 0.9)
        print "max: %.2f" % slowdowns[-1]
        print 
        print "USERS"
        print "-----"
        print "Number of users: %i" % len(users)
        print 
        print "UTILIZATION"
        print "-----------"
        print "Utilization: %.2f%%" % (utilization * 100)
        print "Utilization (no ramp-up): %.2f%%" % (utilization_no_ramp * 100)

