from workspace.haizea.common.config import TraceConfig, ImageConfig
from workspace.haizea.traces.formats import LWF, LWFEntry

def generateTrace(config, file, guaranteeAvg = False):
    tracedur = config.getTraceDuration()
    
    avgnumreq = tracedur / config.intervaldist.getAvg()
    idealaccumdur = avgnumreq * config.durationdist.getAvg() * config.numnodesdist.getAvg()

    print avgnumreq
    print config.durationdist.getAvg()
    print config.numnodesdist.getAvg()
    print idealaccumdur

    good = False

    while not good:
        entries = []
        time = 0
        accumdur = 0
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
            accumdur += entry.duration * entry.numNodes
            entries.append(entry)

        if not guaranteeAvg:
            good = True
        else:
            dev = abs((accumdur / idealaccumdur) - 1)
            if dev < 0.01:
                print "Deviation is satisfactory: %.3f" % dev
                good = True
            else:
                print "Deviation is too big: %.3f. Generating again." % dev

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