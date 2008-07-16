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
from haizea.traces.generators import generateTrace, generateImages
from haizea.common.utils import gen_traceinj_name, unpickle
from haizea.common.config import RMConfig, RMMultiConfig, TraceConfig, ImageConfig
import os.path
import optparse

def haizea(argv):
    p = OptionParser()
    p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))

    opt, args = p.parse_args(argv)
    
    configfile=opt.conf
    config = RMConfig.fromFile(configfile)
    
    rm = ResourceManager(config)

    rm.start()

def haizea_generate_configs(argv):
    p = OptionParser()
    p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
    p.add_option(Option("-d", "--dir", action="store", type="string", dest="dir", required=True))

    opt, args = p.parse_args(argv)
    
    configfile=opt.conf
    multiconfig = RMMultiConfig.fromFile(configfile)
    
    dir = opt.dir
    
    configs = multiconfig.getConfigs()
    
    etcdir = os.path.abspath(dir)    
    if not os.path.exists(etcdir):
        os.makedirs(etcdir)
        
    for c in configs:
        profile = c.getProfile()
        tracefile = c.getTracefile()
        injfile = c.getInjectfile()
        name = gen_traceinj_name(tracefile, injfile)
        configfile = etcdir + "/%s_%s.conf" % (profile, name)
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
    multiconfig = RMMultiConfig.fromFile(configfile)
            
    try:
        from mako.template import Template
    except:
        print "You need Mako Templates for Python to run this command."
        print "You can download them at http://www.makotemplates.org/"
        exit(1)

    configs = multiconfig.getConfigsToRun()
    
    etcdir = os.path.abspath(opt.confdir)    
    if not os.path.exists(etcdir):
        os.makedirs(etcdir)
        
    templatedata = []    
    for c in configs:
        profile = c.getProfile()
        tracefile = c.getTracefile()
        injfile = c.getInjectfile()
        datafile = c.getDataFile()
        name = gen_traceinj_name(tracefile, injfile)
        if not opt.onlymissing or not os.path.exists(datafile):
            configfile = etcdir + "/%s_%s.conf" % (profile, name)
            templatedata.append((profile, name, configfile))

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
    
class Option (optparse.Option):
    ATTRS = optparse.Option.ATTRS + ['required']

    def _check_required (self):
        if self.required and not self.takes_value():
            raise optparse.OptionError(
                "required flag set for option that doesn't take a value",
                 self)

    # Make sure _check_required() is called from the constructor!
    CHECK_METHODS = optparse.Option.CHECK_METHODS + [_check_required]

    def process (self, opt, value, values, parser):
        optparse.Option.process(self, opt, value, values, parser)
        parser.option_seen[self] = 1