import sys

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
        
if __name__ == "__main__":
    print "I've been run!"
    trace = TraceFile.fromFile("example.trace")
    trace.toFile(sys.stdout)
    
    
    
    
    