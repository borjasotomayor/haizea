import os
import workspace.haizea.common.constants as constants
import workspace.haizea.analysis.graph as graphs
from pickle import Unpickler
from mx.DateTime import now
import shutil

class Section(object):
    def __init__(self, title, filename, graph, profilesdirs, tablefinal = None, maxmin = False):
        self.title = title
        self.filename = filename
        self.graph = graph
        self.graphfile = self.filename + ".png"
        self.thumbfile = self.filename + "-thumb.png"
        self.profilesdirs = profilesdirs
        self.profiles = profilesdirs.keys()
        self.profiles.sort()
        self.tablefinal = tablefinal
        self.final = {}
        self.maxmin = maxmin
        self.data = {}
        
    def loadData(self):
        for p in self.profiles:
            profiledir = self.profilesdirs[p]
            file = open (profiledir + "/" + self.filename, "r")
            u = Unpickler(file)
            self.data[p] = u.load()
            file.close()
            
        # If we are going to produce a table, create it now
        if self.tablefinal == constants.TABLE_FINALTIME:
            for p in self.profiles:
                final = self.data[p][-1][0]
                self.final[p] = final
        if self.tablefinal == constants.TABLE_FINALVALUE:
            for p in self.profiles:
                final = self.data[p][-1][1]
                self.final[p] = final
        
    def generateGraph(self, outdir):
        values = [self.data[p] for p in self.profiles]
        g = self.graph(values, "Time (s)", self.title, self.profiles)
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
            html += "<th colspan='2'>%s</th>" % title
            html += "</tr>"
            html += "<tr><th>Profile</th><th>%s</th></tr>" % col
            for p in self.profiles:
                html += "<tr><td>%s</td>" % p
                html += "<td>%.2f</td></tr>" % self.final[p]
            html += "</table>"
        
        return html

 

class MultiProfileReport(object):
    def __init__(self, profilesdirs, outdir, css = None):
        self.profilesdirs = profilesdirs
        self.outdir = outdir
        self.css = css
        
        # TODO Factor the following out to a configuration file
        self.sections = [
                         Section("CPU Utilization", constants.CPUUTILFILE, graphs.StepGraph, profilesdirs),
                         Section("CPU Utilization (avg)", constants.CPUUTILAVGFILE, graphs.PointGraph, profilesdirs, tablefinal = constants.TABLE_FINALVALUE, maxmin = True),
                         Section("Best-effort Leases Completed", constants.COMPLETEDFILE, graphs.StepGraph, profilesdirs, tablefinal = constants.TABLE_FINALTIME),
                         Section("Queue Size", constants.QUEUESIZEFILE, graphs.StepGraph, profilesdirs)
                         ]
        
    def generate(self):
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)
            
        # Load data
        for s in self.sections:
            s.loadData()
            
        # Generate graphs
        for s in self.sections:
            s.generateGraph(self.outdir)
            
        reportfile = open(self.outdir + "/index.html", "w")
        
        header = self.generateHTMLHeader()
        reportfile.write(header)
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
            shutil.copy(self.css, self.outdir)
        
    def generateHTMLHeader(self):
        header = """
<?xml version="1.0"?>
<html>
<head>
    <title>Experiment results</title>
    <meta http-equiv="Content-Type" content="text/html" />
    <meta http-equiv="Content-Language" content="en"/>
    <link rel="stylesheet" type="text/css" href="report.css" media="screen" />
</head>

<body>
<h1>Experiment results</h1>
<hr/>
<p>
<strong>Report generation date:</strong> %s
</p>
""" % now()
        return header
    
    def generateTOC(self):
        toc = "<h5>Table of contents</h5>"
        toc += "<ul>"
        for i, s in enumerate(self.sections):
            toc += "<li><a href='#%i'>%s</a></li>" % (i, s.title)
        toc += "<li><a href='#table'>Tabular summary</a></li>"
        toc += "</ul>"
        
        return toc
    
    def generateTableSummary(self):   
        profiles = self.profilesdirs.keys()
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
        profiles = self.profilesdirs.keys()
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
        

