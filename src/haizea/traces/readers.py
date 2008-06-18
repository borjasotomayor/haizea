from mx.DateTime import TimeDelta, DateTimeDelta
from mx.DateTime import ISO
from mx.DateTime import now
from haizea.resourcemanager.datastruct import ExactLease, BestEffortLease, ResourceTuple
import haizea.common.constants as constants
import haizea.traces.formats as formats

def SWF(tracefile, config):
    file = open (tracefile, "r")
    requests = []
    inittime = config.getInitialTime()
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
                resreq = ResourceTuple.createEmpty()
                resreq.setByType(constants.RES_CPU, 1) # One CPU per VM, should be configurable
                resreq.setByType(constants.RES_MEM, 1024) # Should be configurable
                resreq.setByType(constants.RES_DISK, vmimagesize + 0) # Should be configurable
                maxdur = TimeDelta(seconds=reqtime)
                if runtime < 0 and status==5:
                    # This is a job that got cancelled while waiting in the queue
                    realdur = maxdur
                    maxqueuetime = tSubmit + TimeDelta(seconds=waittime)
                else:
                    if runtime == 0:
                        runtime = 1 # Runtime of 0 is <0.5 rounded down.
                    realdur = TimeDelta(seconds=runtime) # 3: RunTime
                    maxqueuetime = None
                if realdur > maxdur:
                    realdur = maxdur
                req = BestEffortLease(None, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur, maxqueuetime, timeOnDedicated=realdur)
                req.state = constants.LEASE_STATE_PENDING
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
            image,size = line.split()
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
        resreq = ResourceTuple.createEmpty()
        resreq.setByType(constants.RES_CPU, entry.CPU)
        resreq.setByType(constants.RES_MEM, entry.mem)
        resreq.setByType(constants.RES_DISK, vmimagesize + entry.disk)
        if tStart == None:
            req = BestEffortLease(tSubmit, duration, vmimage, vmimagesize, numnodes, resreq, realduration)
        else:
            req = ExactLease(tSubmit, tStart, duration, vmimage, vmimagesize, numnodes, resreq, realduration)
        req.state = constants.LEASE_STATE_PENDING
        requests.append(req)
    return requests
