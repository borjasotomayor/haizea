# -------------------------------------------------------------------------- #
# Copyright 2006-2008, Borja Sotomayor                                       #
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

import haizea.traces.readers as tracereaders

def analyzeARLeaseInjection(tracefile):
    injectedleases = tracereaders.LWF(tracefile)
    accumdur = 0
    for l in injectedleases:
        dur = l.end - l.start
        numnodes = l.numnodes
        
        accumdur += dur * numnodes
    
    seconds = accumdur.seconds
    formatted = accumdur.strftime("%dd-%Hh-%Mm-%Ss")

    print "%i %s" % (seconds, formatted)