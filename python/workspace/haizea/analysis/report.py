import os
import workspace.haizea.common.constants as constants
import workspace.haizea.analysis.graph as graphs
from workspace.haizea.common.utils import genDataDirName, genTraceInjName
from pickle import Unpickler
from mx.DateTime import now
from operator import itemgetter, or_
import shutil

class Section(object):
    def __init__(self, title, filename, graphtype, tablefinal = None, maxmin = False, clip = None, cliptype = None):
        self.title = title
        self.filename = filename
        self.graphtype = graphtype
        self.graphfile = self.filename + "_" + str(graphtype) + ".png"
        self.thumbfile = self.filename + "_" + str(graphtype) + "-thumb.png"
        self.tablefinal = tablefinal
        self.profiles = None
        self.final = {}
        self.maxmin = maxmin
        self.data = {}
        self.clip = clip
        self.cliptimes = None
        self.clipIDs = None
        self.cliptype = cliptype
        
    def findClips(self, dir):
        file = open (dir + "/" + constants.COMPLETEDFILE, "r")
        u = Unpickler(file)
        besteffortcompleted = u.load()
        file.close()
        numreq = len(besteffortcompleted)
        startclip = int( (self.clip[0] / 100.0) * numreq)
        endclip = int(numreq - ((self.clip[1]/100.0) * numreq))

        # Find what the top and bottom 5% leases are
        besteffortcompleted.sort(key=itemgetter(1))
        startID = besteffortcompleted[startclip][1]
        endID = besteffortcompleted[endclip][1]
        endTime = besteffortcompleted[endclip][0]
        
        # Find when the bottom 5% lease was submitted
        file = open (dir + "/" + constants.QUEUESIZEFILE, "r")
        u = Unpickler(file)
        queuesize = u.load()
        file.close()
        queuesize = [e for e in queuesize if e[1] == startID]
        startTime = queuesize[0][0]
        
        self.clipIDs = (startID, endID)
        self.cliptimes = (startTime, endTime)
        
        
        
    def loadData(self, dirs):
        self.profiles = dirs.keys()
        self.profiles.sort()
        for p in self.profiles:
            dir = dirs[p]
            file = open (dir + "/" + self.filename, "r")
            u = Unpickler(file)
            data = u.load()
            if self.clip != None:
                self.findClips(dir)  
                if self.cliptype == constants.CLIP_BYTIME:
                    data = [e for e in data if e[0] >= self.cliptimes[0] and e[0] <= self.cliptimes[1]]
                elif self.cliptype == constants.CLIP_BYLEASE:
                    data = [e for e in data if e[1] >= self.clipIDs[0] and e[1] <= self.clipIDs[1]]
            self.data[p] = data
            file.close()
            
        # If we are going to produce a table, create it now
        if self.tablefinal == constants.TABLE_FINALTIME:
            for p in self.profiles:
                final = self.data[p][-1][0]
                self.final[p] = final
        if self.tablefinal == constants.TABLE_FINALVALUE:
            for p in self.profiles:
                final = self.data[p][-1][2]
                self.final[p] = final
        if self.tablefinal == constants.TABLE_FINALAVG:
            for p in self.profiles:
                final = self.data[p][-1][3]
                self.final[p] = final
        
    def generateGraph(self, outdir):
        if self.graphtype in [constants.GRAPH_LINE_VALUE, constants.GRAPH_STEP_VALUE, constants.GRAPH_POINT_VALUE]:
            values = [[(v[0],v[2]) for v in self.data[p]] for p in self.profiles]
        elif self.graphtype in [constants.GRAPH_LINE_AVG]:
            values = [[(v[0],v[3]) for v in self.data[p]] for p in self.profiles]
        elif self.graphtype in [constants.GRAPH_POINTLINE_VALUEAVG]:
            values = [[(v[0],v[2],v[3]) for v in self.data[p]] for p in self.profiles]

        if self.graphtype in [constants.GRAPH_LINE_VALUE, constants.GRAPH_LINE_AVG]:
            graph = graphs.LineGraph
            legends = self.profiles
        elif self.graphtype in [constants.GRAPH_STEP_VALUE]:
            graph = graphs.StepGraph
            legends = self.profiles
        elif self.graphtype in [constants.GRAPH_POINTLINE_VALUEAVG]:
            graph = graphs.PointAndLineGraph
            legends = []
            for l in self.profiles:
                legends.append(l)
                legends.append(l + " (avg)")
            
        g = graph(values, "Time (s)", self.title, legends)
        graphfile = outdir + "/" + self.graphfile
        thumbfile = outdir + "/" + self.thumbfile
        g.plotToFile(graphfile, thumbfile)
        
    def generateHTML(self):
        html  = "<div class='image'>"
        html += "<a href='%s'><img src='%s' border='0'/></a>" % (self.graphfile, self.thumbfile)
        html += "</div>"
        
        if self.tablefinal != None:
            html += "<table align='center' border='1' cellpadding='5'>"
            html += "<tr>"
            if self.tablefinal == constants.TABLE_FINALTIME:
                title = "Final Times"
                col = "Time"
            if self.tablefinal == constants.TABLE_FINALVALUE:
                title = "Final Values"
                col = "Value"
            if self.tablefinal == constants.TABLE_FINALAVG:
                title = "Final Values"
                col = "Average"
            html += "<th colspan='2'>%s</th>" % title
            html += "</tr>"
            html += "<tr><th>Profile</th><th>%s</th></tr>" % col
            for p in self.profiles:
                html += "<tr><td>%s</td>" % p
                html += "<td>%.2f</td></tr>" % self.final[p]
            html += "</table>"
        
        return html

class Report(object):
    def __init__(self, config, statsdir):
        self.config = config
        self.statsdir = statsdir

        confs = self.config.getConfigsToReport()

        profiles = set([c.getProfile() for c in confs])
        self.profiles = list(profiles)

        tracefiles = set([c.getTracefile() for c in confs])
        injectfiles = set([c.getInjectfile() for c in confs])
        self.traces = []
        for t in tracefiles:
            for i in injectfiles:
                self.traces.append((t,i,genTraceInjName(t,i)))        
        
        
        self.css = self.config.getCSS()
        self.outdir = self.config.getReportDir()
  
        if self.config.isClipping():
            self.clip = self.config.getClips()
        else:
            self.clip = None
                
        self.sections = [
                 Section("CPU Utilization", constants.CPUUTILFILE, constants.GRAPH_STEP_VALUE, clip=self.clip, cliptype=constants.CLIP_BYTIME),
                 Section("CPU Utilization (avg)", constants.CPUUTILFILE, constants.GRAPH_LINE_AVG, tablefinal = constants.TABLE_FINALAVG, maxmin = True, clip=self.clip, cliptype=constants.CLIP_BYTIME),
                 Section("Best-effort Leases Completed", constants.COMPLETEDFILE, constants.GRAPH_STEP_VALUE, tablefinal = constants.TABLE_FINALTIME, clip=self.clip, cliptype=constants.CLIP_BYLEASE),
                 Section("Queue Size", constants.QUEUESIZEFILE, constants.GRAPH_STEP_VALUE),
                 #Section("Best-Effort Wait Time (Queue only)", constants.QUEUEWAITFILE, constants.GRAPH_POINTLINE_VALUEAVG, profilesdirs, tablefinal = constants.TABLE_FINALAVG, maxmin = True),
                 Section("Best-Effort Wait Time (from submission to lease start)", constants.EXECWAITFILE, constants.GRAPH_POINTLINE_VALUEAVG, tablefinal = constants.TABLE_FINALAVG, maxmin = True, clip=self.clip, cliptype=constants.CLIP_BYLEASE),
                 Section("'Client happiness' ratio", constants.UTILRATIOFILE, constants.GRAPH_POINTLINE_VALUEAVG, tablefinal = constants.TABLE_FINALAVG, maxmin = True, clip=self.clip, cliptype=constants.CLIP_BYLEASE)
                 #Section("Best-Effort Wait Time (from submission to lease start) [NOCLIP]", constants.EXECWAITFILE, constants.GRAPH_POINTLINE_VALUEAVG, tablefinal = constants.TABLE_FINALAVG, maxmin = True)
                 ]
        
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        
        


    def generate(self):
        self.generateIndex()
        for t in self.traces:
            profilesdirs = [(p, self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for p in self.profiles]
            profilesdirs = dict([(p,d) for p,d in profilesdirs if os.path.exists(d)])
            if len(profilesdirs) > 0:
                self.generateReport(t[2],profilesdirs)
        for p in self.profiles:
            tracesdirs = [(t[2], self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for t in self.traces]
            tracesdirs = dict([(t,d) for t,d in tracesdirs if os.path.exists(d)])
            if len(tracesdirs) > 0:
                self.generateReport(p, tracesdirs)

    def generateIndex(self):
        indexfile = open(self.outdir + "/index.html", "w")
        header = self.generateHTMLHeader()
        heading = self.generateHeading("Experiment Results")
        indexfile.write(header + heading)
        indexfile.write("<hr/>")
        
        traces = self.traces
        traces.sort()
        profiles = self.profiles
        profiles.sort()
        
        html  = "<h3>Profile reports</h3>"
        html += "<ul>"
        for p in profiles:
            html += "<li>"
            tracesdirsexist = [os.path.exists(self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for t in self.traces]
            tracesdirsexist = [e for e in tracesdirsexist if e == True]
            if len(tracesdirsexist) > 0:                    
                html += "<a href='%s/index.html'>%s</a> (%i)" % (p,p,len(tracesdirsexist))
            else:
                html += p
            html += "</li>"
        html += "</ul>"
        indexfile.write(html)
        indexfile.write("<hr/>")

        html  = "<h3>Trace reports</h3>"
        html += "<ul>"
        for t in traces:
            html += "<li>"
            profilesdirsexists = [os.path.exists(self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for p in self.profiles]
            profilesdirsexists = [e for e in profilesdirsexists if e == True]
            if len(profilesdirsexists) > 0:                    
                html += "<a href='%s/index.html'>%s</a> (%i)" % (t[2],t[2], len(profilesdirsexists))
            else:
                html += t[2]
            html += "</li>"
            html += "</li>"
        html += "</ul>"
        indexfile.write(html)
        indexfile.write("<hr/>")
        listitems = ""
        for p in profiles:
            for t in traces:
                name = genDataDirName(p,t[0],t[1])
                dir = self.statsdir + "/" + name
                if not os.path.exists(dir):
                    listitems += "<li>%s</li>" % name
        if listitems != "":
            html  = "<h3>Simulations that haven't completed yet</h3>"
            html += "<ul>" + listitems + "</ul>"
            indexfile.write(html)
            indexfile.write("<hr/>")
        

        footer = self.generateHTMLFooter()
        indexfile.write(footer)
        
        indexfile.close()
        
        if self.css != None:
            shutil.copy(self.css, self.outdir)
        
    def generateReport(self, name, dirs):
        outdir = self.outdir + "/" + name
        if not os.path.exists(outdir):
            os.makedirs(outdir)
            
        # Load data
        for s in self.sections:
            s.loadData(dirs)
            
        # Generate graphs
        for s in self.sections:
            s.generateGraph(outdir)
            
        reportfile = open(outdir + "/index.html", "w")
        
        header = self.generateHTMLHeader()
        heading = self.generateHeading(name)
        reportfile.write(header + heading)
        reportfile.write("<hr/>")
        
        toc = self.generateTOC()
        reportfile.write(toc)
        reportfile.write("<hr/>")

        for i, s in enumerate(self.sections):
            html = "<h3><a name='%i'></a>%s</h3>" % (i, s.title)
            reportfile.write(html)
            html = s.generateHTML()
            reportfile.write(html)
            reportfile.write("<hr/>")
        
        html = "<h3><a name='table'></a>Tabular Summary</h3>"
        reportfile.write(html)
        table = self.generateTableSummary()
        reportfile.write(table)
        
        csvfile = "summary.csv"
        html  = "<div class='center'><div class='small'>"
        html += "[ <a href='%s'>CSV file</a> ]" % csvfile
        html += "</div></div>"
        reportfile.write(html)

        self.generateCSVSummary(self.outdir + "/" + csvfile)
            
        footer = self.generateHTMLFooter()
        reportfile.write(footer)
        
        reportfile.close()
        
        if self.css != None:
            shutil.copy(self.css, outdir)
        
    def generateHTMLHeader(self):
        header = """<html>
<head>
    <title>Experiment results</title>
    <meta http-equiv="Content-Type" content="text/html" />
    <meta http-equiv="Content-Language" content="en"/>
    <link rel="stylesheet" type="text/css" href="report.css" media="screen" />
</head>
"""
        return header
    
    def generateHeading(self, title):
        heading = """
<body>
<h1>%s</h1>
<hr/>
<p>
<strong>Report generation date:</strong> %s
</p>
""" % (title, now())
        return heading
    
    def generateTOC(self):
        toc = "<h5>Table of contents</h5>"
        toc += "<ul>"
        for i, s in enumerate(self.sections):
            toc += "<li><a href='#%i'>%s</a></li>" % (i, s.title)
        toc += "<li><a href='#table'>Tabular summary</a></li>"
        toc += "</ul>"
        
        return toc
    
    def generateTableSummary(self):   
        profiles = self.profiles
        profiles.sort()
        sections = [s for s in self.sections if s.tablefinal!=None] 

        html  = "<table align='center' border='1' cellpadding='5'>"
        html += "<tr>"
        html += "<th colspan='%i'>Summary</th>" % (len(self.sections)+1)
        html += "</tr>"
        html += "<tr><th>Profile</th>"
        for s in sections:
            html += "<th>%s</th>" % s.title
        html += "</tr>"
        for p in profiles:
            html += "<tr><td>%s</td>" % p
            for s in sections:
                html += "<td>%.2f</td>" % s.final[p]
            html += "</tr>"
        html += "</table>"        
        return html
    
    def generateCSVSummary(self, csvfile):  
        profiles = self.profiles
        profiles.sort()
        sections = [s for s in self.sections if s.tablefinal!=None] 
        
        f=open(csvfile, 'w')
        
        headers = ["Profiles"] + [s.title for s in sections]
        csvheader=",".join(headers)
        f.write("%s\n" % csvheader)
        for p in profiles:
            fields = [p] + ["%.2f" % s.final[p] for s in sections]
            csvline=",".join(fields)
            f.write("%s\n" % csvline)
        f.close()
    
    def generateHTMLFooter(self):
        footer = """
</body>
</html>
"""
        return footer
        

