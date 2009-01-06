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

from mx.DateTime import TimeDelta
from haizea.resourcemanager.leases import Lease, ARLease, BestEffortLease
from haizea.resourcemanager.scheduler.slottable import ResourceTuple
import haizea.common.constants as constants
import haizea.traces.formats as formats

def SWF(tracefile, config):
    file = open (tracefile, "r")
    requests = []
    inittime = config.get("starttime")
    for line in file:
        if line[0]!=';':
            req = None
            fields = line.split()
            reqtime = float(fields[8])
            runtime = int(fields[3]) # 3: RunTime
            waittime = int(fields[2])
            status = int(fields[10])
            
            if reqtime > 0:
                tSubmit = int(fields[1]) # 1: Submission time
                tSubmit = inittime + TimeDelta(seconds=tSubmit) 
                vmimage = "NOIMAGE"
                vmimagesize = 600 # Arbitrary
                numnodes = int(fields[7]) # 7: reqNProcs
                resreq = ResourceTuple.create_empty()
                resreq.set_by_type(constants.RES_CPU, 1) # One CPU per VM, should be configurable
                resreq.set_by_type(constants.RES_MEM, 1024) # Should be configurable
                resreq.set_by_type(constants.RES_DISK, vmimagesize + 0) # Should be configurable
                maxdur = TimeDelta(seconds=reqtime)
                if runtime < 0 and status==5:
                    # This is a job that got cancelled while waiting in the queue
                    continue
                else:
                    if runtime == 0:
                        runtime = 1 # Runtime of 0 is <0.5 rounded down.
                    realdur = TimeDelta(seconds=runtime) # 3: RunTime
                if realdur > maxdur:
                    realdur = maxdur
                preemptible = True
                req = BestEffortLease(tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, preemptible, realdur)
                req.state = Lease.STATE_NEW
                requests.append(req)
    return requests

def IMG(imgfile):
    file = open (imgfile, "r")
    imagesizes = {}
    images = []
    state = 0  # 0 -> Reading image sizes  1 -> Reading image sequence
    for line in file:
        if line[0]=='#':
            state = 1
        elif state == 0:
            image, size = line.split()
            imagesizes[image] = int(size)
        elif state == 1:
            images.append(line.strip())
    return imagesizes, images

def LWF(tracefile, inittime):
    file = formats.LWF.fromFile(tracefile)
    requests = []
    for entry in file.entries:
        tSubmit = inittime + TimeDelta(seconds=entry.reqTime) 
        if entry.startTime == -1:
            tStart = None
        else:
            tStart = inittime + TimeDelta(seconds=entry.startTime)
        duration = TimeDelta(seconds=entry.duration)
        realduration = TimeDelta(seconds=entry.realDuration)
        vmimage = entry.vmImage
        vmimagesize = entry.vmImageSize
        numnodes = entry.numNodes
        resreq = ResourceTuple.create_empty()
        resreq.set_by_type(constants.RES_CPU, entry.CPU)
        resreq.set_by_type(constants.RES_MEM, entry.mem)
        resreq.set_by_type(constants.RES_DISK, vmimagesize + entry.disk)
        if tStart == None:
            preemptible = True
            req = BestEffortLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, preemptible, realduration)
        else:
            preemptible = False
            req = ARLease(tSubmit, tStart, duration, vmimage, vmimagesize, numnodes, resreq, preemptible, realduration)
        req.state = Lease.STATE_NEW
        requests.append(req)
    return requests
