from mx.DateTime import TimeDelta, DateTimeDelta
from mx.DateTime import ISO
from mx.DateTime import now
from workspace.haizea.resourcemanager.datastruct import ExactLease, BestEffortLease, ResourceTuple
import workspace.haizea.common.constants as constants
import workspace.haizea.traces.formats as formats

def CSV(tracefile, config):
    file = open (tracefile, "r")
    requests = []
    inittime = config.getInitialTime()
    for line in file:
        if line[0]!='#':
            req = None
            fields = line.split(";")
            
            # We support two legacy CSV formats used in previous prototypes.
            
            if len(fields) == 10:
                # In this format, the fields have the following meaning:
                #     0:"time",
                #     1:"uri",
                #     2:"size",
                #     3:"numNodes",
                #     4:"memory",
                #     5:"cpu",
                #     6:"mode",
                #     7:"deadline",
                #     8:"duration",
                #     9:"tag"
                tSubmit = inittime + TimeDelta(seconds=int(fields[0])) # 0: time
                vmimage = fields[1] # 1: uri
                vmimagesize = int(fields[2]) # 2: size
                numnodes = int(fields[3]) # 3: numNodes
                resreq = {}
                resreq[constants.RES_CPU] = 1 # One CPU per VM
                resreq[constants.RES_MEM] = int(fields[4]) # 4: memory
                resreq[constants.RES_DISK] = vmimagesize + 0 # TODO: Make this a config param
                if fields[7] != "NULL": # 7: deadline
                    start = tSubmit + TimeDelta(seconds=int(fields[7])) # 7: deadline
                    end = start + TimeDelta(seconds=int(fields[8])) # 8: duration
                    req = ExactLease(None, tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, end)
                else:
                    maxdur = TimeDelta(seconds=int(fields[8])) # 8: duration
                    req = BestEffortLease(None, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, maxdur)
            elif len(fields) == 12:
                # In this format, the fields have the following meaning:
                #     0:"time",
                #     1:"realstart",
                #     2:"uri",
                #     3:"size",
                #     4:"numNodes",
                #     5:"memory",
                #     6:"cpu",
                #     7:"mode",
                #     8:"deadline",
                #     9:"duration",
                #     10:"realduration",
                #     11:"tag"
                tSubmit = inittime + TimeDelta(seconds=int(fields[0])) # 0: time
                vmimage = fields[2] # 2: uri
                vmimagesize = int(fields[3]) # 3: size
                numnodes = int(fields[4]) # 4: numNodes
                resreq = [None,None,None,None,None] # TODO: Hardcoding == bad
                resreq[constants.RES_CPU] = 1 # One CPU per VM
                resreq[constants.RES_MEM] = int(fields[5]) # 5: memory
                resreq[constants.RES_NETIN] = 0
                resreq[constants.RES_NETOUT] = 0
                resreq[constants.RES_DISK] = vmimagesize + 0 # TODO: Make this a config param
                resreq = ResourceTuple(resreq)
                if fields[8] != "NULL": # 8: deadline
                    start = tSubmit + TimeDelta(seconds=int(fields[8])) # 8: deadline
                    end = start + TimeDelta(seconds=int(fields[9])) # 9: duration
                    prematureend = start + TimeDelta(seconds=int(fields[10])) # 10: realduration
                    req = ExactLease(None, tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, prematureend)
                else:
                    if fields[9]=="INF":
                        maxdur = DateTimeDelta(30) # 30 days
                    else:
                        maxdur = TimeDelta(seconds=int(fields[9])) # 9: duration
                    realdur = TimeDelta(seconds=int(fields[10])) # 10: realduration
                    req = BestEffortLease(None, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur, timeOnDedicated=realdur)
            req.state = constants.LEASE_STATE_PENDING
            requests.append(req)
    return requests

def GWF(tracefile, config):
    file = open (tracefile, "r")
    requests = []
    inittime = config.getInitialTime()
    traceinittime = None
    for line in file:
        if line[0]!='#':
            req = None
            fields = line.split()
            reqtime = float(fields[8])
            
            if reqtime > 0 and fields[18] == "UNITARY":
                tSubmit = int(fields[1]) # 1: Submission time
                if traceinittime == None:
                    traceinittime = tSubmit
                relTSubmit = tSubmit - traceinittime
                tSubmit = inittime + TimeDelta(seconds=relTSubmit) 
                vmimage = "NOIMAGE"
                vmimagesize = 600 # Arbitrary
                numnodes = int(fields[7]) # 7: reqNProcs
                resreq = [None,None,None,None,None] # TODO: Hardcoding == bad
                resreq[constants.RES_CPU] = 1 # One CPU per VM
                resreq[constants.RES_MEM] = 512 # Arbitrary (2 VMs per node)
                resreq[constants.RES_NETIN] = 0
                resreq[constants.RES_NETOUT] = 0
                resreq[constants.RES_DISK] = vmimagesize + 0 # TODO: Make this a config param
                resreq = ResourceTuple(resreq)
                maxdur = TimeDelta(seconds=reqtime)
                realdur = TimeDelta(seconds=int(fields[3])) # 3: RunTime
                req = BestEffortLease(None, tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur)
                req.state = constants.LEASE_STATE_PENDING
                requests.append(req)
    return requests

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
                resreq = [None,None,None,None,None] # TODO: Hardcoding == bad
                resreq[constants.RES_CPU] = 1 # One CPU per VM
                resreq[constants.RES_MEM] = 1024 
                resreq[constants.RES_NETIN] = 0
                resreq[constants.RES_NETOUT] = 0
                resreq[constants.RES_DISK] = vmimagesize + 0 # TODO: Make this a config param
                resreq = ResourceTuple(resreq)
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

def LWF(tracefile, config = None):
    file = formats.LWF.fromFile(tracefile)
    if config != None:
        inittime = config.getInitialTime()
    else:
        inittime = now()
    requests = []
    for entry in file.entries:
        tSubmit = inittime + TimeDelta(seconds=entry.reqTime) 
        tStart = inittime + TimeDelta(seconds=entry.startTime)
        tEnd = tStart + TimeDelta(seconds=entry.duration)
        tRealEnd = tStart + TimeDelta(seconds=entry.realDuration)
        vmimage = entry.vmImage
        vmimagesize = entry.vmImageSize
        numnodes = entry.numNodes
        resreq = {}
        resreq[constants.RES_CPU] = entry.CPU
        resreq[constants.RES_MEM] = entry.mem
        resreq[constants.RES_DISK] = vmimagesize + entry.disk
        req = ExactLease(None, tSubmit, tStart, tEnd, vmimage, vmimagesize, numnodes, resreq, tRealEnd)
        req.state = constants.LEASE_STATE_PENDING
        requests.append(req)
    return requests
