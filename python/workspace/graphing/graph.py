from matplotlib.colors import colorConverter
import matplotlib
import pylab
from pylab import array, arange

class Graph(object):
    def __init__(self):
        pass
    
    def plot(self):
        pass
    
    def show(self):
        pass


#TODO: Factor out stuff like labels, etc.
class ScheduleGraph(object):
    def __init__(self, data, legends=[], subdata=[], sublegends=[], firstempty=False):
        self.data = data
        self.legends = legends
        self.firstempty = firstempty
        self.subdata = subdata
        self.sublegends = sublegends
    
    def plot(self):
        #pylab.axes([0.1,0.1,0.65,0.8])
        numTracks = len(self.data[0])
        numDivisions = len(self.data)
        yLabels = ['%d' % (x+1) for x in range(len(self.data[0]))]
        xLabels = [] # Not used
        
        
        #TODO: Check for empty legends
        colors = map(lambda x: x.color,self.legends)
        #subcolors = filter(lambda x: x!=None, self.sublegends)
        subcolors = map(lambda x: x.color, self.sublegends)
        
        #TODO: Use constants
        width = 0.6     # the width of the bars
        ind = arange(numTracks) * (width *1.5)  # the y locations for the tracks
        print ind
        if self.firstempty:
            xoff= array(self.data[0])
            divs = xrange(numDivisions-1)
        else:
            xoff = array([0.0] * numTracks) # the left values for stacked bar chart
            divs = xrange(numDivisions)
        
        patches = []
        subrectangles = []

        #TODO: Replace with "for row in self.data:"?
        for div in divs:
            if self.firstempty: 
                row = div + 1
            else:
                row = div
            print self.data[row]
            print colors[row]
            rectangles = pylab.barh(self.data[row], ind, width, left=xoff, color=colors[row])
            patches.append(rectangles[0])
            if len(self.subdata)>0 and len(self.subdata[row]) > 0:
                rectangles = pylab.barh(self.subdata[row], ind, width-0.3, left=xoff, color=subcolors[row])
                subrectangles.append(rectangles[0])
            xoff = xoff + self.data[row]
    
        patches += subrectangles
        
        pylab.ylabel("Virtual Workspace")
        pylab.xlabel("Time (s)")
        accum = array([0.0]*numTracks)
        for row in self.data:
            accum += array(row)
        vals = arange(0, max(accum), 500)
        pylab.xticks(vals, ['%d' % val for val in vals])
        pylab.yticks(ind, yLabels)
        pylab.title('Trace')

        if self.firstempty:
            legendText = map(lambda x: x.name,self.legends[1:]) + map(lambda x: x.name,self.sublegends[1:])
        else:
            legendText = map(lambda x: x.name,self.legends) #+ map(lambda x: x.name,self.sublegends)

        legendText = filter(lambda x: x!=None, legendText)

        pylab.legend(patches, legendText, (0.5,0.1))

    
    def show(self):
        pylab.show()
        
class PointGraph(Graph):
    def __init__(self, data, legends=[]):
        self.data = data
        self.legends = legends

    def plot(self):
        for dataset in self.data:
            x = map(lambda x: x[0], dataset)
            y = map(lambda x: x[1], dataset)
            pylab.plot(x,y)
    
    def show(self):
        pylab.show()
        
class Legend(object):
    def __init__(self, name=None, color=None):
        self.name = name
        self.color = color


class Figure(object):
    def __init__(self, graphs=[]):
        self.graphs=graphs
        
    def addGraph(self, graph):
        self.graphs.append(graph)
        
    def plot(self):

        numGraphs=len(self.graphs)
        axes = None
        for i,graph in enumerate(self.graphs):
            #TODO: This requires the funky 3-digit format. 
            #Using rows,col,num doesn't work. Maybe this has
            #been fixed in latest version?
            #TODO: Currently, default is to share x axis. This
            #should be configurable
            axes = pylab.subplot((numGraphs*100)+10+i+1, sharex=axes)
            graph.plot()
            
    def show(self):
        pylab.show()
        

if __name__ == "__main__":    
    data = [[  66386,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015],
            [  58230,  381139,   78045,   99308,  160454,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015],
            [  89135,   80552,  152558,  497981,  603535,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015],
            [  78415,   81858,  150656,  193263,   69638,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015],
            [ 139361,  331509,  343164,  781380,   52269,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015,  174296,   75131,  577908,   32015]]
    
    legends = [ Legend("foobar","r"), 
               Legend("transfer","g"), 
               Legend("foobaz","b"), 
               Legend("razzle","c"), 
               Legend("dazzle","m")]
    
    fig = Figure()
    sched = ScheduleGraph(data,legends, firstempty=True)
    fig.addGraph(sched)
    sched.show()
    #fig.show()
    

        