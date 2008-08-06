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

import ConfigParser
from mx.DateTime import ISO
from mx.DateTime import TimeDelta
import textwrap
        
OPTTYPE_INT = 0
OPTTYPE_FLOAT = 1
OPTTYPE_STRING = 2
OPTTYPE_BOOLEAN = 3
OPTTYPE_DATETIME = 4
OPTTYPE_TIMEDELTA = 5

class ConfigException(Exception):
    """A simple exception class used for configuration exceptions"""
    pass

class Section(object):
    def __init__(self, name, required, required_if=None, doc=None):
        self.name = name
        self.required = required
        self.required_if = required_if
        self.doc = doc
        self.options = {}
        
    def get_doc(self):
        return textwrap.dedent(self.doc).strip()


class Option(object):
    def __init__(self, name, getter, type, required, required_if=None, default=None, valid=None, doc=None):
        self.name = name
        self.getter = getter
        self.type = type
        self.required = required
        self.required_if = required_if
        self.default = default
        self.valid = valid
        self.doc = doc
        
    def get_doc(self):
        return textwrap.dedent(self.doc).strip()

class Config(object):
    def __init__(self, config, sections):
        self.config = config
        self.sections = sections
        self.__options = {}
        
        self.__load_all()
        
    def __load_all(self):
        required_sections = [s for s in self.sections if s.required]
        conditional_sections = [s for s in self.sections if not s.required and s.required_if != None]
        optional_sections = [s for s in self.sections if not s.required and s.required_if == None]
        
        sections = required_sections + conditional_sections + optional_sections
        
        for sec in sections:
            has_section = self.config.has_section(sec.name)
            
            # If the section is required, check if it exists
            if sec.required and not has_section:
                raise ConfigException, "Required section [%s] not found" % sec.name
            
            # If the section is conditionally required, check that
            # it meets the conditions
            if sec.required_if != None:
                for req in sec.required_if:
                    (condsec,condopt) = req[0]
                    condvalue = req[1]
                    
                    if self.config.has_option(condsec,condopt) and self.config.get(condsec,condopt) == condvalue:
                        if not has_section:
                            raise ConfigException, "Section '%s' is required when %s.%s==%s" % (sec.name, condsec, condopt, condvalue)
                    
            # Load options
            if has_section:
                for opt in sec.options:
                    self.__load_option(sec, opt)

    
    def __load_option(self, sec, opt):
        # Load a single option
        secname = sec.name
        optname = opt.name
        
        has_option = self.config.has_option(secname, optname)
        
        if not has_option:
            if opt.required:
                raise ConfigException, "Required option '%s.%s' not found" % (secname, optname)
            if opt.required_if != None:
                for req in opt.required_if:
                    (condsec,condopt) = req[0]
                    condvalue = req[1]
                    
                    if self.config.has_option(condsec,condopt) and self.config.get(condsec,condopt) == condvalue:
                        raise ConfigException, "Option '%s.%s' is required when %s.%s==%s" % (secname, optname, condsec, condopt, condvalue)
            
            value = opt.default
        else:
            if opt.type == OPTTYPE_INT:
                value = self.config.getint(secname, optname)
            elif opt.type == OPTTYPE_FLOAT:
                value = self.config.getfloat(secname, optname)
            elif opt.type == OPTTYPE_STRING:
                value = self.config.get(secname, optname)
            elif opt.type == OPTTYPE_BOOLEAN:
                value = self.config.getboolean(secname, optname)
            elif opt.type == OPTTYPE_DATETIME:
                value = self.config.get(secname, optname)
                value = ISO.ParseDateTime(value)
            elif opt.type == OPTTYPE_TIMEDELTA:
                value = self.config.getint(secname, optname)
                value = TimeDelta(seconds=value)
                
            if opt.valid != None:
                if not value in opt.valid:
                    raise ConfigException, "Invalid value specified for '%s.%s'. Valid values are %s" % (secname, optname, opt.valid)
                  
        self.__options[opt.getter] = value
        
    def get(self, opt):
        return self.__options[opt]
        
    @classmethod
    def from_file(cls, configfile):
        file = open (configfile, "r")
        c = ConfigParser.ConfigParser()
        c.readfp(file)
        cfg = cls(c)
        return cfg

        
        