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

import logging
import sys
from haizea.common.utils import get_clock
from haizea.common.constants import LOGLEVEL_VDEBUG, LOGLEVEL_STATUS

logging.addLevelName(LOGLEVEL_VDEBUG, "VDEBUG")
logging.addLevelName(LOGLEVEL_STATUS, "STATUS")

# Custom logger that uses our log record
class HaizeaLogger(logging.Logger):
    
    def makeRecord(self, name, lvl, fn, lno, msg, args, exc_info, func=None, extra=None):
        # Modify "extra" parameter keyword
        try:
            haizeatime = get_clock().get_time()
        except:
            # This is a kludge. Basically, calling get_clock will
            # fail if Manager is not yet fully constructed (since it's
            # a singleton). The more correct solution is to more cleanly
            # separate the initialization code in the Manager from the
            # initialization that actually involves interacting with
            # other components (which may want to use the logger)
            haizeatime = "                      "
        extra = { "haizeatime" : haizeatime}
        if sys.version_info[1] <= 4:
            name = "[%s] %s" % (haizeatime, name)
            return logging.Logger.makeRecord(self, name, lvl, fn, lno, msg, args, exc_info)
        else:
            return logging.Logger.makeRecord(self, name, lvl, fn, lno, msg, args, exc_info, func, extra)
    
    def status(self, msg):
        self.log(logging.getLevelName("STATUS"), msg)

    def vdebug(self, msg):
        # Since there is such a huge amount of vdebug messages, we check the
        # log level manually to decide if we call the log function or not.
        # (this actually saves quite a bit of cycles spent in logging functions
        # that ultimately determine that the message doesn't have to printed)
        if self.getEffectiveLevel() == LOGLEVEL_VDEBUG:
            self.log(logging.getLevelName("VDEBUG"), msg)