import sys
import re
from workspace.graphing.graph import *


class TraceEntry(object):
    pos = {
                     0:"time",
                     1:"uri",
                     2:"size",
                     3:"numNodes",
                     4:"mode",
                     5:"deadline",
                     6:"duration",
                     7:"tag"
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
    
    @staticmethod
    def compare(a, b):
        return int(a.fields["time"]) - int(b.fields["time"])
        

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
            
    def toScheduleGraph(self, bandwidth=1.0):
        #TODO: Hardcoding is bad
        data = [[],[],[]]
        subdata = [[],[],[]]        
        for entry in self.entries:
            startTime = int(entry.fields["time"])
            if entry.fields["deadline"] == "NULL":
                timeToDeadline = 0
            else:
                timeToDeadline = int(entry.fields["deadline"])
            duration = int(entry.fields["duration"])
            if entry.fields["size"] == "NULL":
                imageSize=0
            else:
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

    def toImageHistogram(self):
        data = []
        for entry in self.entries:
            imageName=entry.fields["uri"].split("/").pop()
            data.append(imageName)
        graph = DiscreteHistogram(data,"Images","# of images", normalized=True)
        return graph
        
    def toDurationHistogram(self):
        data = []
        for entry in self.entries:
            duration = float(entry.fields["duration"])
            data.append(duration)
        graph = ContinuousHistogram(data,"Duration","# of images",normalized=True)
        return graph   

class SWFEntry(object):
    pos = {
                     0:"jobnum",
                     1:"t_submit",
                     2:"t_wait",
                     3:"t_run",
                     4:"proc_alloc",
                     5:"avgcpu",
                     6:"mem",
                     7:"proc_req",
                     8:"t_run_req",
                     9:"mem_req",
                     10:"status",
                     11:"uid",
                     12:"gid",
                     13:"exec_num",
                     14:"queue",
                     15:"partition",
                     16:"job_prec",
                     17:"t_think"
                     }
    numFields = len(pos)
    
    def __init__(self, fields={}):
        self.fields = fields
        
    def toLine(self):
        line = ""
        for i in range(len(SWFEntry.pos)):
            line += self.fields[SWFEntry.pos[i]] + " "  
        return line.rstrip(" ")
        
    @classmethod
    def fromLine(cls,line):
        dictFields = {}
        fields = re.split("[ \t]*", line)
        if len(fields)!=SWFEntry.numFields:
            raise Exception, "Unexpected number of fields in line"
        for i,field in enumerate(fields):
            dictFields[SWFEntry.pos[i]] = field
        return cls(dictFields)    


class SWFFile(object):
    def __init__(self, entries=[]):
        self.entries=entries

    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        entries = []
        for line in file:
            if line[0]!=";":
                entry = SWFEntry.fromLine(line.strip())
                entries.append(entry)
        return cls(entries)
        
    def toFile(self,file):
        for entry in self.entries:
            print >>file, entry.toLine()      
            
    def toTrace(self, imageDist, imageSizes, maxnodes=-1, maxduration=-1, truncateDurations = False, range=None, partition=None, queue=None):
        tEntries = []
        if range == None:
            range = (0,len(self.entries)-1)
        for entry in self.entries[range[0]:range[1]]:
            status = int(entry.fields["status"])
            numNodes = entry.fields["proc_alloc"]
            duration = entry.fields["t_run"]
            entryqueue = entry.fields["queue"]
            entrypartition = entry.fields["partition"]
            if status in (1,-1) \
            and (maxnodes == -1 or int(numNodes) <= maxnodes) \
            and duration >= 1 \
            and (maxduration == -1 or truncateDurations or int(duration) <= maxduration) \
            and (partition == None or entrypartition == partition) \
            and (queue == None or entryqueue == queue):
                fields = {}
                fields["time"] = entry.fields["t_submit"]
                image = imageDist.get()
                fields["uri"] = image
                fields["size"] = str(imageSizes[image])
                fields["numNodes"] = str(numNodes) 
                fields["mode"] = "RW"
                fields["deadline"] = "NULL"
                fields["tag"]="BATCH"
                if truncateDurations and int(duration) > maxduration:
                    fields["duration"] = str(maxduration)
                else:
                    fields["duration"] = duration                    
                tEntries.append(TraceEntry(fields))
        return TraceFile(tEntries)


        
if __name__ == "__main__":
    print "I've been run!"
#    trace = TraceFile.fromFile("example.trace")
#    #trace.toFile(sys.stdout)
#    fig = Figure()
#    graph = trace.toScheduleGraph()
#    imageGraph = trace.toImageHistogram()
#    durGraph = trace.toDurationHistogram()
#    
#    data = [[(0,10),(1000,5),(2000,3),(3000,8),(4000,1),(5000,5)],
#            [(500,4),(1500,1),(2500,7),(3500,2),(4500,7),(4800,3)]]
#    
#    legends = [ Legend("foobar","r"), 
#               Legend("barfoo","g")]
#    
#    graph2 = PointGraph(data,legends)
#    fig.addGraph(imageGraph,3,2,1)
#    fig.addGraph(durGraph,3,2,2)
#    fig.addGraph(graph,3,1,2)
#    fig.addGraph(graph2,3,1,3, sharex=graph)
#    
#    
#    fig.plot()
#    fig.show()
    c = workspace.traces.cooker.SWF2TraceConf.fromFile("exampleswf2trace.desc")
    swf = SWFFile.fromFile("example.swf")
    print c.range
    trace = swf.toTrace(imageDist=c.imageDist, imageSizes=c.imageSizes, maxnodes=c.maxNodes, maxduration=c.maxDuration, range=c.range)
    trace.toFile(sys.stdout)
    therm = workspace.traces.cooker.Thermometer(trace)
    therm.printStats()
    g = trace.toScheduleGraph()
    g.plot()
    g.show()
    