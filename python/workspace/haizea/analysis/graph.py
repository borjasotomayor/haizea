import pylab
from matplotlib.ticker import FormatStrFormatter
import matplotlib.mlab as mlab
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
        colors = ['blue', 'green', 'red','cyan', 'magenta','yellow', 
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
        colors = iter(colors)
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
        
# TODO
class ScatterGraph(Graph):
    def __init__(self, data, xlabel="", ylabel="", legends=[]):
        Graph.__init__(self,xlabel,ylabel)
        self.data = data
        self.legends = legends

    def plotToFile(self, graphfile, thumbfile=None):
        print "Generating graph %s" % graphfile
        Graph.plot(self)
        
        for dataset in self.data:
            x = [p[0] for p in dataset]
            y = [p[1] for p in dataset]
            size = [p[2] for p in dataset]
            # TODO
        

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