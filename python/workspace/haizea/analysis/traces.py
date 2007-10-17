import workspace.haizea.traces.readers as tracereaders

def analyzeExactLeaseInjection(tracefile):
    injectedleases = tracereaders.LWF(tracefile)
    accumdur = 0
    for l in injectedleases:
        dur = l.end - l.start
        numnodes = l.numnodes
        
        accumdur += dur * numnodes
    
    seconds = accumdur.seconds
    formatted = accumdur.strftime("%dd-%Hh-%Mm-%Ss")

    print "%i %s" % (seconds, formatted)