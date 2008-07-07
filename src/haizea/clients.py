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
from haizea.common.utils import Option, OptionParser, genTraceInjName
from haizea.common.config import RMConfig, RMMultiConfig, TraceConfig, ImageConfig
import os.path

class Haizea(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = RMConfig.fromFile(configfile)
        
        rm = ResourceManager(config)
    
        rm.start()

class GenConfigs(object):
    def __init__(self):
        pass
    
    def run(self, argv):
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
            datadir = c.getDataDir()
            name = genTraceInjName(tracefile, injfile)
            configfile = etcdir + "/%s_%s.conf" % (profile, name)
            fc = open(configfile, "w")
            c.config.write(fc)
            fc.close()
                        
class genScript(object):
    def __init__(self):
        pass
    
    def run(self, argv):
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
            datadir = c.getDataDir()
            name = genTraceInjName(tracefile, injfile)
            if not opt.onlymissing or not os.path.exists(datadir):
                configfile = etcdir + "/%s_%s.conf" % (profile, name)
                templatedata.append((profile, name, configfile))

        template = Template(filename=opt.template)
        print template.render(configs=templatedata, etcdir=etcdir)

    
class TraceGenerator(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-g", "--guaranteeavg", action="store_true", dest="guaranteeavg"))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = TraceConfig.fromFile(configfile)
        
        tracefile = opt.tracefile

        generateTrace(config, tracefile, opt.guaranteeavg)     


class ImageGenerator(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-c", "--conf", action="store", type="string", dest="conf", required=True))
        p.add_option(Option("-f", "--imagefile", action="store", type="string", dest="imagefile", required=True))

        opt, args = p.parse_args(argv)
        
        configfile=opt.conf
        config = ImageConfig.fromFile(configfile)
        
        imagefile = opt.imagefile

        generateImages(config, imagefile)           

        
class InjectionAnalyzer(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-f", "--injectionfile", action="store", type="string", dest="injectionfile", required=True))

        opt, args = p.parse_args(argv)
        
        injectionfile = opt.injectionfile

        #analyzeARLeaseInjection(injectionfile)  