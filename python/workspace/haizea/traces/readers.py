from mx.DateTime import TimeDelta
from mx.DateTime import ISO
from workspace.haizea.resourcemanager.datastruct import ExactLease, BestEffortLease 
import workspace.haizea.common.constants as constants

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
                    req = ExactLease(tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq)
                else:
                    maxdur = TimeDelta(seconds=int(fields[8])) # 8: duration
                    req = BestEffortLease(tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq)
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
                resreq = {}
                resreq[constants.RES_CPU] = 1 # One CPU per VM
                resreq[constants.RES_MEM] = int(fields[5]) # 5: memory
                resreq[constants.RES_DISK] = vmimagesize + 0 # TODO: Make this a config param
                if fields[8] != "NULL": # 8: deadline
                    start = tSubmit + TimeDelta(seconds=int(fields[8])) # 8: deadline
                    end = start + TimeDelta(seconds=int(fields[9])) # 9: duration
                    prematureend = start + TimeDelta(seconds=int(fields[10])) # 10: realduration
                    req = ExactLease(tSubmit, start, end, vmimage, vmimagesize, numnodes, resreq, prematureend)
                else:
                    maxdur = TimeDelta(seconds=int(fields[9])) # 9: duration
                    realdur = TimeDelta(seconds=int(fields[10])) # 10: realduration
                    req = BestEffortLease(tSubmit, maxdur, vmimage, vmimagesize, numnodes, resreq, realdur)
            req.state = constants.LEASE_STATE_PENDING
            requests.append(req)
    return requests

def GWF(tracefile, config):
    return None