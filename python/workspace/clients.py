import sys
from workspace.traces import files
from workspace.graphing import graph
from workspace.util import stats
from workspace.util.miscutil import *

class TraceGraph(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-t", "--tracefile", action="store", type="string", dest="tracefile", required=True))
        p.add_option(Option("-o", "--output", action="store", type="choice", dest="output", 
                     choices=["png","x11"], default="x11"))
        p.add_option(Option("-f", "--outputfile", action="store", type="string", dest="outputfile"))
        
        opt, args = p.parse_args(argv)
        
        
        #TODO: Lots of error checking
    
        #TODO: Customize what makes it into the figure
        
        trace = files.TraceFile.fromFile(opt.tracefile)
        fig = graph.Figure()
        schedGraph = trace.toScheduleGraph()
        imageGraph = trace.toImageHistogram()
        durGraph = trace.toDurationHistogram()
        fig.addGraph(imageGraph,2,2,1)
        fig.addGraph(durGraph,2,2,2)
        fig.addGraph(schedGraph,2,1,2)

        fig.plot()
    
        if opt.output == "x11":
            fig.show()
        elif opt.output == "png":
            pass

class SWF2Trace(object):
    def __init__(self):
        pass
    
    def run(self, argv):
        p = OptionParser()
        p.add_option(Option("-f", "--swffile", action="store", type="string", dest="swffile", required=True))
        p.add_option(Option("-i", "--imagefile", action="store", type="string", dest="imagefile", required=True))
        p.add_option(Option("-d", "--imagedist", action="store", type="choice", dest="imagedist", 
                     choices=["uniform","explicit"], default="uniform"))
        p.add_option(Option("-n", "--maxnodes", action="store", type="int", dest="maxnodes", default=-1))
        p.add_option(Option("-t", "--maxduration", action="store", type="int", dest="maxduration", default=-1))
        p.add_option(Option("-1", "--first-record", action="store", type="int", dest="firstrecord", default=-1))
        p.add_option(Option("-2", "--last-record", action="store", type="int", dest="lastrecord", default=-1))
        
        opt, args = p.parse_args(argv)

        swf = files.SWFFile.fromFile(opt.swffile)
        
        # Image file and distribution
        imagefile = opt.imagefile
        imagedist = opt.imagedist
        
        images = []
        imagesizes = {}
        probs = []  # Only for explicit
        
        file = open (imagefile, "r")
        for line in file:
            values = line.strip().split(";")
            image = values[0]
            size = values[1]
            
            images.append(image)
            imagesizes[image]=size
            
            if imagedist == "explicit":
                probs.append(values[2])

        if imagedist == "uniform":
            imagedist = stats.DiscreteUniformDistribution(images)
        elif imagedist == "explicit":
            imagedist = stats.DiscreteDistribution(images, probs)
            
        if opt.firstrecord == -1 and opt.lastrecord == -1:
            range = None
        else:
            range = [opt.firstrecord,opt.lastrecord]
            
        trace = swf.toTrace(imageDist=imagedist, 
                            imageSizes=imagesizes, 
                            maxnodes=opt.maxnodes, 
                            maxduration=opt.maxduration, 
                            range=range)

        trace.toFile(sys.stdout)
        g = trace.toScheduleGraph()
        g.plot()
        g.show()

if __name__ == "__main__":
    #tg = TraceGraph()
    #tg.run(sys.argv)
    
    s2t = SWF2Trace()
    s2t.run(sys.argv)