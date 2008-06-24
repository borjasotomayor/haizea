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

from haizea.common.config import TraceConfig, ImageConfig
from haizea.traces.formats import LWF, LWFEntry

def generateTrace(config, file, guaranteeAvg = False):
    tracedur = config.getTraceDuration()
    
    print config.intervaldist.getAvg()
    
    avgnumreq = tracedur / config.intervaldist.getAvg()
    idealaccumdur = avgnumreq * config.durationdist.getAvg() * config.numnodesdist.getAvg()

    print avgnumreq
    print config.durationdist.getAvg()
    print config.numnodesdist.getAvg()
    print idealaccumdur

    good = False
    deadlineavg = config.deadlinedist.get()

    while not good:
        entries = []
        time = - deadlineavg
        accumdur = 0
        while time + deadlineavg + config.durationdist.getAvg() < tracedur:
            entry = LWFEntry()
            entry.reqTime = time
            entry.startTime = time + config.deadlinedist.get()
            entry.duration = config.durationdist.get()
            entry.realDuration = entry.duration
            entry.numNodes = config.numnodesdist.get()
            entry.CPU = 1
            entry.mem = 1024
            entry.disk = 0
            entry.vmImage = "NONE.img"
            entry.vmImageSize = 600
            accumdur += entry.duration * entry.numNodes
            entries.append(entry)

            interval = config.intervaldist.get()          
            time += interval
            
        if not guaranteeAvg:
            good = True
        else:
            dev = abs((accumdur / idealaccumdur) - 1)
            if dev < 0.01:
                print "Deviation is satisfactory: %.3f" % dev
                good = True
            else:
                print "Deviation is too big: %.3f. Generating again." % dev

    for e in entries:
        if e.reqTime < 0:
            e.reqTime = 0

    lwf = LWF(entries)
    lwf.toFile(file)
        
        
def generateImages(config, file):
    f = open(file, "w")
    
    # Write image sizes
    for i in config.images:
        print >>f, "%s %i" % (i, config.sizedist.get())
    
    print >>f, "#"
    
    l = config.getFileLength()
    for i in xrange(l):
        print >>f, config.imagedist.get()

    f.close()


if __name__ == "__main__":
    configfile="../configfiles/images.conf"
    imagefile="../traces/examples/generated.images"


    config = ImageConfig.fromFile(configfile)
    
    generateImages(config, imagefile)   