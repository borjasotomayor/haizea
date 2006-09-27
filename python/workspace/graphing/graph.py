from matplotlib.colors import colorConverter
import matplotlib
import pylab
from pylab import array, arange

class ScheduleGraph(object):
    def __init__(self, data, legends=[], firstempty=False):
        self.data = data
        self.legends = legends
        self.firstempty = firstempty
        self.patches = []
    
    def plot(self):
        pylab.axes([0.1,0.1,0.7,0.8])
        numTracks = len(self.data[0])
        numDivisions = len(self.data)
        yLabels = ['%d' % (x+1) for x in range(len(data[0]))]
        xLabels = [] # Not used
        
        #TODO: Check for empty legends
        colors = map(lambda x: x.color,self.legends)
        
        #TODO: Use constants
        width = 0.4     # the width of the bars
        ind = arange(numTracks) * (width *1.35)  # the y locations for the tracks
        if self.firstempty:
            xoff= array(data[0])
            divs = xrange(numDivisions-1)
        else:
            xoff = array([0.0] * numTracks) # the left values for stacked bar chart
            divs = xrange(numDivisions)

        for div in divs:
            if self.firstempty: 
                row = div + 1
            else:
                row = div
            rectangles = pylab.barh(data[row], ind, width, left=xoff, color=colors[row])
            self.patches.append(rectangles[0])
            #barh(data[row+1], ind, width-0.2, left=yoff, color=colours[row])
            xoff = xoff + data[row]
    
        pylab.ylabel("Virtual Workspace")
        pylab.xlabel("Time (s)")
        accum = array([0.0]*numTracks)
        for row in data:
            accum += array(row)
        vals = arange(0, max(accum), 500000)
        pylab.xticks(vals, ['%d' % val for val in vals])
        pylab.yticks(ind, yLabels)
        pylab.title('Trace')

        if self.firstempty:
            legendText = map(lambda x: x.name,self.legends[1:])
        else:
            legendText = map(lambda x: x.name,self.legends)

        pylab.legend(self.patches, legendText, (1.05,0.5))

    
    def show(self):
        pylab.show()
        
        
class Legend(object):
    def __init__(self, name, color):
        self.name = name
        self.color = color

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
    
    g = ScheduleGraph(data,legends, firstempty=True)
    g.plot()
    g.show()

