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
import sys
import os
import os.path
import errno
import signal
import time

DEFAULT_CONFIG_LOCATIONS = ["/etc/haizea.conf",
                            os.path.expanduser("~/.haizea/haizea.conf")]
DEFAULT_PIDFILE = "/var/tmp/haizea.pid"


def haizea(argv):
    p = OptionParser()
    p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf"))
    p.add_option(Option("-f", "--fg", action="store_true",  dest="foreground"))
    p.add_option(Option("--stop", action="store_true",  dest="stop"))

    opt, args = p.parse_args(argv)
        
    pidfile = DEFAULT_PIDFILE # TODO: Make configurable

    if opt.stop == None:
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
            configfile=opt.conf
            if configfile == None:
                # Look for config file in default locations
                for loc in DEFAULT_CONFIG_LOCATIONS:
                    if os.path.exists(loc):
                        config = HaizeaConfig.from_file(loc)
                        break
                else:
                    print >> sys.stdout, "No configuration file specified, and none found at default locations."
                    print >> sys.stdout, "Make sure a config file exists at:\n  -> %s" % "\n  -> ".join(DEFAULT_CONFIG_LOCATIONS)
                    print >> sys.stdout, "Or specify a configuration file with the --conf option."
                    exit(1)
            else:
                config = HaizeaConfig.from_file(configfile)
        except ConfigException, msg:
            print >> sys.stderr, "Error in configuration file:"
            print >> sys.stderr, msg
            exit(1)
            
        daemon = not opt.foreground
    
        rm = ResourceManager(config, daemon, pidfile)
    
        rm.start()
    elif opt.stop: # Stop Haizea
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


def haizea_generate_configs(argv):
    p = OptionParser()
    p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
    p.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True))

    opt, args = p.parse_args(argv)
    
    configfile=opt.conf
    multiconfig = HaizeaMultiConfig.from_file(configfile)
    
    dir = opt.dir
    
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
                        
def haizea_generate_scripts(argv):
    p = OptionParser()
    p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
    p.add_option(Option("-t", "--template", action="store", type="string", dest="template", required=True))
    p.add_option(Option("-d", "--confdir", action="store", type="string", dest="confdir", required=True))
    p.add_option(Option("-m", "--only-missing", action="store_true",  dest="onlymissing"))

    opt, args = p.parse_args(argv)
    
    configfile=opt.conf
    multiconfig = HaizeaMultiConfig.from_file(configfile)
            
    try:
        from mako.template import Template
    except:
        print "You need Mako Templates for Python to run this command."
        print "You can download them at http://www.makotemplates.org/"
        exit(1)

    configs = multiconfig.get_configs()
    
    etcdir = os.path.abspath(opt.confdir)    
    if not os.path.exists(etcdir):
        os.makedirs(etcdir)
        
    templatedata = []    
    for c in configs:
        profile = c.get_attr("profile")
        tracefile = c.get("tracefile")
        injfile = c.get("injectionfile")
        datafile = c.get("datafile")
        configname = generate_config_name(profile, tracefile, injfile)
        if not opt.onlymissing or not os.path.exists(datafile):
            configfile = etcdir + "/%s.conf" % configname
            templatedata.append((configname, configfile))

    template = Template(filename=opt.template)
    print template.render(configs=templatedata, etcdir=etcdir)


def haizea_convert_data(argv):
    p = OptionParser()
    p.add_option(Option("-d", "--datafiles", action="store", type="string", dest="datafiles", required=True))
    p.add_option(Option("-s", "--summary", action="store_true",  dest="summary"))
    p.add_option(Option("-l", "--lease-stats", action="store", type="string", dest="lease"))
    p.add_option(Option("-t", "--include-attributes", action="store_true", dest="attributes"))
    p.add_option(Option("-f", "--format", action="store", type="string", dest="format"))

    opt, args = p.parse_args(argv)
    
    datafile=opt.datafiles
    
    stats = unpickle(datafile)
    
    # Barebones for now. Just prints out lease id, waiting time, and
    # slowdown (only best-effort leases)
    waitingtimes = stats.get_waiting_times()
    slowdowns = stats.get_slowdowns()
    print "lease_id waiting_time slowdown"
    for lease_id in waitingtimes:
        print lease_id, waitingtimes[lease_id].seconds, slowdowns[lease_id]


