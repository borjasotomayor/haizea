import sys
from workspace.graphing.graph import ScheduleGraph, PointGraph, Legend, Figure

class TraceEntry(object):
    pos = {
                     0:"time",
                     1:"uri",
                     2:"size",
                     3:"numNodes",
                     4:"mode",
                     5:"deadline",
                     6:"duration"
                     }
    numFields = len(pos)
    
    def __init__(self, fields={}):
        self.fields = fields
        
    def toLine(self):
        line = ""
        for i in range(len(TraceEntry.pos)):
            line += self.fields[TraceEntry.pos[i]] + ";"  
        return line.rstrip(";")
        
    @classmethod
    def fromLine(cls,line):
        dictFields = {}
        fields = line.split(";")
        if len(fields)!=TraceEntry.numFields:
            raise Exception, "Unexpected number of fields in line"
        for i,field in enumerate(fields):
            dictFields[TraceEntry.pos[i]] = field
        return cls(dictFields)    
    
        

class TraceFile(object):
    def __init__(self, entries=[]):
        self.entries=entries
        
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        entries = []
        for line in file:
            entry = TraceEntry.fromLine(line.strip())
            entries.append(entry)
        return cls(entries)
        
    def toFile(self,file):
        for entry in self.entries:
            print >>file, entry.toLine()
            
    def toGraph(self, bandwidth=1.0):
        #TODO: Hardcoding is bad
        data = [[],[],[]]
        subdata = [[],[],[]]        
        for entry in self.entries:
            startTime = int(entry.fields["time"])
            timeToDeadline = int(entry.fields["deadline"])
            duration = int(entry.fields["duration"])
            imageSize=int(entry.fields["size"])
            data[0].append(startTime)
            data[1].append(timeToDeadline)
            data[2].append(duration)
            subdata[1].append(imageSize/bandwidth)
            
        legends = [ Legend("Not submitted","gray"), 
           Legend("Staging","orange"), 
           Legend("Running","lightgreen")]

        sublegends = [ Legend(), 
           Legend("Transfer time (%dMb/s)" % (bandwidth*8),"red"), 
           Legend()]
        
        graph = ScheduleGraph(data, legends, subdata, sublegends, firstempty=True)
        return graph
        
        
if __name__ == "__main__":
    print "I've been run!"
    trace = TraceFile.fromFile("example.trace")
    #trace.toFile(sys.stdout)
    fig = Figure()
    graph = trace.toGraph()
    fig.addGraph(graph)
    
    data = [[(0,10),(1000,5),(2000,3),(3000,8),(4000,1),(5000,5)],
            [(500,4),(1500,1),(2500,7),(3500,2),(4500,7),(4800,3)]]
    
    legends = [ Legend("foobar","r"), 
               Legend("barfoo","g")]
    
    graph2 = PointGraph(data,legends)
    fig.addGraph(graph2)
    
    fig.plot()
    fig.show()
    
    
    
    