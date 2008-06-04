class LWFEntry(object):
    pos = {
                     0:("reqTime",int),
                     1:("startTime",int),
                     2:("duration",int),
                     3:("realDuration",int),
                     4:("numNodes",int),
                     5:("CPU",int),
                     6:("mem",int),
                     7:("disk",int),
                     8:("vmImage",str),
                     9:("vmImageSize",int)
                     }
    numFields = len(pos)
    
    def __init__(self):
        self.reqTime = None
        self.startTime = None
        self.endTime = None
        self.prematureEndTime = None
        self.numNodes = None
        self.CPU = None
        self.mem = None
        self.disk = None
        self.vmImage = None
        self.vmImageSize = None
        
    def toLine(self):
        line = " ".join([`self.__getattribute__(self.pos[i][0])` for i in range(self.numFields)])
        return line
        
    @classmethod
    def fromLine(cls,line):
        c = cls()
        fields = line.split()
        if len(fields)!=cls.numFields:
            raise Exception, "Unexpected number of fields in line"
        for i,field in enumerate(fields):
            attrname = cls.pos[i][0]
            fieldtype = cls.pos[i][1]
            c.__setattr__(attrname, fieldtype(field))
        return c


class LWF(object):
    def __init__(self, entries=[]):
        self.entries=entries
        
    @classmethod
    def fromFile(cls, filename):
        file = open (filename, "r")
        entries = []
        for line in file:
            if line[0]!='#':
                entry = LWFEntry.fromLine(line.strip())
                entries.append(entry)
        file.close()
        return cls(entries)
    
    def toFile(self,file):
        f = open(file, "w")
        for entry in self.entries:
            print >>f, entry.toLine()
        f.close()