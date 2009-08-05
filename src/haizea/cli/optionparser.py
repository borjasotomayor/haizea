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
import optparse as py_optparse

class OptionParser (py_optparse.OptionParser):
    def _init_parsing_state (self):
        py_optparse.OptionParser._init_parsing_state(self)
        self.option_seen = {}

    def check_values (self, values, args):
        for option in self.option_list:
            if (isinstance(option, Option) and
                option.required and
                not self.option_seen.has_key(option)):
                self.error("%s not supplied" % option)
        return (values, args)
    
class Option (py_optparse.Option):
    ATTRS = py_optparse.Option.ATTRS + ['required']

    def _check_required (self):
        if self.required and not self.takes_value():
            raise py_optparse.OptionError(
                "required flag set for option that doesn't take a value",
                 self)

    # Make sure _check_required() is called from the constructor!
    CHECK_METHODS = py_optparse.Option.CHECK_METHODS + [_check_required]

    def process (self, opt, value, values, parser):
        py_optparse.Option.process(self, opt, value, values, parser)
        parser.option_seen[self] = 1