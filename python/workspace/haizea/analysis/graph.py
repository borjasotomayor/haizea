import pylab
from matplotlib.ticker import FormatStrFormatter
import Image

class Graph(object):
    def __init__(self, xlabel="", ylabel=""):
        self.xlabel=xlabel
        self.ylabel=ylabel
    
    def plot(self):
        pylab.xlabel(self.xlabel)
        pylab.ylabel(self.ylabel)
    
    def show(self):
        pass
    
class PointGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        Graph.plot(self)        
        largestY = None
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            largestY = max(largestY,max(y))
            pylab.plot(x,y)
        

        pylab.ylim(0, largestY * 1.05)            
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
            

    
    def show(self):
        pylab.show()
        
class StepGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        Graph.plot(self)
        largestY = None
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            largestY = max(largestY,max(y))
            pylab.plot(x,y, linestyle="steps")
        

        pylab.ylim(0, largestY * 1.05)
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
    
    def show(self):
        pylab.show()