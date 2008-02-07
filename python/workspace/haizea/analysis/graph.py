import pylab
from matplotlib.ticker import FormatStrFormatter
import matplotlib.mlab as mlab
import Image

class Graph(object):
    def __init__(self, xlabel="", ylabel=""):
        self.xlabel=xlabel
        self.ylabel=ylabel
        self.colors = ['blue', 'green', 'red','cyan', 'magenta','yellow', 
          'indigo', 'gold', 'firebrick', 'indianred', 'darkolivegreen', 
          'darkseagreen', 'mediumvioletred', 'mediumorchid', 'chartreuse', 
          'mediumslateblue', 'springgreen', 'crimson', 'lightsalmon', 
          'brown', 'turquoise', 'olivedrab', 'skyblue', 
          'darkturquoise', 'goldenrod', 'darkgreen', 'darkviolet', 
          'darkgray', 'lightpink', 'teal', 'darkmagenta', 
          'lightgoldenrodyellow', 'lavender', 'yellowgreen', 'thistle', 
          'violet', 'navy', 'orchid', 'blue', 'ghostwhite', 'honeydew', 
          'cornflowerblue', 'darkblue', 'darkkhaki', 'mediumpurple', 
          'cornsilk', 'red', 'bisque', 'slategray', 'darkcyan', 'khaki', 
          'wheat', 'deepskyblue', 'darkred', 'steelblue', 'aliceblue']
    
    def plot(self):
        pylab.xlabel(self.xlabel)
        pylab.ylabel(self.ylabel)
    
    def show(self):
        pass
    
class LineGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        print "Generating graph %s" % graphfile
        Graph.plot(self)        
        largestY = None
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            if len(y) == 0:
                largestY = 0
            else:
                largestY = max(largestY,max(y))
            pylab.plot(x,y)
        

        pylab.ylim(0, largestY * 1.05)            
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            print "Generating thumbnail %s" % thumbfile
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
        print "Generating graph %s" % graphfile
        Graph.plot(self)
        largestY = None
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            if len(y) == 0:
                largestY = 0
            else:
                largestY = max(largestY,max(y))
            pylab.plot(x,y, linestyle="steps")
        

        pylab.ylim(0, largestY * 1.05)
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            print "Generating thumbnail %s" % thumbfile
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
    
    def show(self):
        pylab.show()
        
        
class PointAndLineGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        print "Generating graph %s" % graphfile
        Graph.plot(self)        
        largestY = None
        colors = iter(self.colors)
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y1 = [p[1] for p in dataset]
            y2 = [p[2] for p in dataset]
            largestY = max(largestY,max(y1),max(y2))
            color = colors.next()
            pylab.plot(x,y1, 'o', color=color)
            pylab.plot(x,y2, color=color)
        
        pylab.ylim(0, largestY * 1.05)            
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            print "Generating thumbnail %s" % thumbfile
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
    
    def show(self):
        pylab.show()
        
class CumulativeGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,"Percentile",ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        print "Generating graph %s" % graphfile
        Graph.plot(self)
        largestY = None
        for dataset in self.data:
            values = [p[1] for p in dataset]
            n, bins = mlab.hist(values, len(values))
            xaccum = [0]
            for m in n:
                xaccum.append(xaccum[-1] + m)
            xaccum = [ (float(x) / len(xaccum)) * 100 for x in xaccum]
            y = [0] + list(bins)
            largestY = max(largestY,max(y))
            pylab.plot(xaccum,y)
        

        pylab.xlim(0, 105)
        pylab.ylim(0, largestY * 1.05)
        pylab.gca().yaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.gca().xaxis.set_major_formatter(FormatStrFormatter('%d'))
        pylab.legend(self.legends, loc='best')
        
        pylab.savefig(graphfile)
        pylab.gcf().clear()
        
        if thumbfile != None:
            print "Generating thumbnail %s" % thumbfile
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
    
    def show(self):
        pylab.show()

        
class ScatterGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=None, limx=None, limy=None):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends
        self.limx = limx
        self.limy = limy
        if self.limx==None:
            smallestX = min([min([p[0] for p in l]) for l in self.data])
            largestX = max([max([p[0] for p in l]) for l in self.data])
            self.limx=(smallestX,largestX)
        if limy==None:
            smallestY = min([min([p[1] for p in l]) for l in self.data])
            largestY = max([max([p[1] for p in l]) for l in self.data])
            self.limy=(smallestY,largestY)

    def plotToFile(self, graphfile, thumbfile=None):
        print "Generating graph %s" % graphfile
        Graph.plot(self)
        
        colors = iter(self.colors)
        legendpolys = []
        for dataset in self.data:
            pylab.gca().set_xscale('log')
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            size = [p[2] for p in dataset]
            color=colors.next()
            poly = pylab.scatter(x,y,s=size, c=color, linewidths=(0.4,), alpha=0.75)
            # kluge suggested on matplotlib mailing list, since scatter does not
            # support having legends
            legendpolys.append(pylab.Rectangle( (0,0), 1,1, facecolor=color))

        pylab.xlim(self.limx[0] - 5, self.limx[1] + 5)
        pylab.ylim(self.limy[0] - 5, self.limy[1] + 5)
        if self.legends != None:
            pylab.legend(legendpolys, self.legends, loc='lower right')
        
        pylab.savefig(graphfile)
        pylab.clf()
        
        if thumbfile != None:
            print "Generating thumbnail %s" % thumbfile
            im = Image.open(graphfile)
            im.thumbnail((640, 480), Image.ANTIALIAS)
            im.save(thumbfile)
    
    def show(self):
        pylab.show()