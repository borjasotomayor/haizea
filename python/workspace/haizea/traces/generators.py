from workspace.haizea.common.config import TraceConfig
from workspace.haizea.traces.formats import LWF, LWFEntry

def generateTrace(config, file):
    entries = []
    tracedur = config.getTraceDuration()
    time = 0
    while time < tracedur:
        interval = config.intervaldist.get()          
        time += interval

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
        
        entries.append(entry)
        
    lwf = LWF(entries)
    lwf.toFile(file)
        


if __name__ == "__main__":
    configfile="../configfiles/inject.traceconf"
    tracefile="../traces/examples/generated.lwf"


    config = TraceConfig.fromFile(configfile)
    
    generateTrace(config, tracefile)   