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

import logging

class Logger(object):
    def __init__(self, rm, file = None):
        self.rm = rm

        self.logger = logging.getLogger("haizea")
        self.extremedebug = False
        if file == None:
            handler = logging.StreamHandler()
        else:
            handler = logging.FileHandler(file)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.loglevel = {"CRITICAL": 50,
                    "ERROR": 40,
                    "WARNING": 30,
                    "STATUS": 25,
                    "INFO": 20,
                    "DEBUG": 10,
                    "EXTREMEDEBUG": 5,
                    "NOTSET": 0}
        
        level = self.rm.config.get("loglevel")
        self.logger.setLevel(self.loglevel[level])
        self.extremedebug = (level == "EXTREMEDEBUG")

    def info(self, msg, comp):
        self.logger.info("[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))
        
    def debug(self, msg, comp):
        self.logger.debug("[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))
    
    def edebug(self, msg, comp):
        # Since there is such a huge amount of edebug messages, we use the
        # extremedebug variable to decide if we call the log function
        # (this actually saves quite a bit of cycles spent in logging functions
        # that ultimately determine that the message doesn't have to printed)
        if self.extremedebug:
            self.logger.log(self.loglevel["EXTREMEDEBUG"],"[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))
    
    def status(self, msg, comp):
        self.logger.log(self.loglevel["STATUS"],"[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))
        
    def warning(self, msg, comp):
        self.logger.warning("[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))    
        
    def error(self, msg, comp):
        self.logger.error("[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))        
        
    def log(self, level, msg, comp):
        if level != "EXTREMEDEBUG" or self.extremedebug:
            self.logger.log(self.loglevel[level], "[%s] %s %s" % (self.rm.clock.get_time(), comp.ljust(7), msg))        