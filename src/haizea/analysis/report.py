# -------------------------------------------------------------------------- #
# Copyright 2006-2008, Borja Sotomayor                                       #
# Copyright 2008, Distributed Systems Architecture Group, Universidad        #
# Complutense de Madrid (dsa-research.org)                                   #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
# -------------------------------------------------------------------------- #

import os
import haizea.common.constants as constants
import haizea.resourcemanager.datastruct as ds
import haizea.analysis.graph as graphs
from haizea.common.config import RMMultiConfig
from haizea.common.utils import genDataDirName, genTraceInjName, generateCondorHeader, generateCondorQueueEntry
from cPickle import load
from mx.DateTime import now
from operator import itemgetter, or_
import shutil

leasescache = {}

def getleases(dir):
    if not leasescache.has_key(dir):
        print "Loading lease data from %s" % dir
        file = open (dir + "/" + constants.LEASESFILE, "r")
        leases = load(file)
        file.close()
        leasescache[dir] = leases
    return leasescache[dir]

class Section(object):
    def __init__(self, title, datafile, graphtype, tablefinal = None, clip = None, slideshow = False):
        self.title = title
        self.filename = datafile
        self.graphtype = graphtype
        self.tablefinal = tablefinal
        self.profiles = None
        self.final = {}
        self.data = {}
        self.clip = clip
        self.leases = {}
        self.slideshow = slideshow
        if clip != None: clipstr = "clip"
        else: clipstr = "noclip"
        self.graphfile = self.filename + "_" + str(graphtype) + "_" + clipstr + ".png"
        self.thumbfile = self.filename + "_" + str(graphtype) + "_" + clipstr + "-thumb.png"
        
    def loadData(self, dirs, profilenames=None):
        if profilenames==None:
            self.profiles = dirs.keys()
            self.profiles.sort()
        else:
            self.profiles = profilenames
        for p in self.profiles:
            dir = dirs[p]
            
            f = dir + "/" + self.filename
            print "Loading %s" % f
            file = open (f, "r")
            data = load(file)
            file.close()
            
            leases = getleases(dir)

            if self.clip != None:
                startclip = self.clip[0]
                endclip = self.clip[1]
                
                if startclip[0] == constants.CLIP_PERCENTSUBMITTED:
                    percent = startclip[1]
                    time = leases.getPercentSubmittedTime(percent, leasetype=ds.BestEffortLease)
                    data = [e for e in data if e[0] >= time]
                elif startclip[0] == constants.CLIP_TIMESTAMP:
                    time = startclip[1]
                    data = [e for e in data if e[0] >= time]

                if endclip[0] == constants.CLIP_PERCENTSUBMITTED:
                    percent = endclip[1]
                    time = leases.getPercentSubmittedTime(percent, leasetype=ds.BestEffortLease)
                    data = [e for e in data if e[0] <= None]
                elif endclip[0] == constants.CLIP_TIMESTAMP:
                    time = endclip[1]
                    data = [e for e in data if e[0] <= time]
                elif endclip[0] == constants.CLIP_LASTSUBMISSION:
                    time = leases.getLastSubmissionTime(leasetype=ds.BestEffortLease)
                    data = [e for e in data if e[0] <= time]

                # Recompute average
                accum=0
                count=0
                newdata = []
                for v in data:
                    value = v[2]
                    accum += value
                    count += 1
                    avg = accum/count
                    newdata.append((v[0], v[1], value, avg))
                data = newdata
            self.data[p] = data
            self.leases[p] = leases
            
        # If we are going to produce a table, create it now
        if self.tablefinal == constants.TABLE_FINALTIME:
            for p in self.profiles:
                if len(self.data[p]) > 0:
                    final = self.data[p][-1][0]
                    self.final[p] = final
                else:
                    self.final[p] = 0
        if self.tablefinal == constants.TABLE_FINALVALUE:
            for p in self.profiles:
                if len(self.data[p]) > 0:
                    final = self.data[p][-1][2]
                    self.final[p] = final
                else:
                    self.final[p] = 0
        if self.tablefinal == constants.TABLE_FINALAVG:
            for p in self.profiles:
                if len(self.data[p]) > 0:
                    final = self.data[p][-1][3]
                    self.final[p] = final
                else:
                    self.final[p] = 0
        
    def generateGraph(self, outdir, filename=None, titlex=None, titley=None):
        if self.graphtype in [constants.GRAPH_LINE_VALUE, constants.GRAPH_STEP_VALUE, constants.GRAPH_POINT_VALUE, constants.GRAPH_CUMULATIVE]:
            values = [[(v[0],v[2]) for v in self.data[p]] for p in self.profiles]
        elif self.graphtype in [constants.GRAPH_LINE_AVG]:
            values = [[(v[0],v[3]) for v in self.data[p]] for p in self.profiles]
        elif self.graphtype in [constants.GRAPH_POINTLINE_VALUEAVG]:
            values = [[(v[0],v[2],v[3]) for v in self.data[p]] for p in self.profiles]
        elif self.graphtype in [constants.GRAPH_NUMNODE_LENGTH_CORRELATION_SIZE, constants.GRAPH_NUMNODE_LENGTH_CORRELATION_Y, constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_SIZE, constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_Y]:
            values = []
            for p in self.profiles:
                pvalues = []
                for v in self.data[p]:
                    lease = self.leases[p].getLease(v[1])
                    numnodes = lease.numnodes
                    length = (lease.maxdur - lease.remdur).seconds
                    reqlength = lease.maxdur
                    if self.graphtype == constants.GRAPH_NUMNODE_LENGTH_CORRELATION_SIZE:
                        pvalues.append((length, numnodes, v[2]))
                    elif self.graphtype == constants.GRAPH_NUMNODE_LENGTH_CORRELATION_Y:
                        pvalues.append((length, v[2], numnodes))
                    elif self.graphtype == constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_SIZE:
                        pvalues.append((reqlength, numnodes, v[2]))
                    elif self.graphtype == constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_Y:
                        pvalues.append((reqlength, v[2], numnodes))
                    
                values.append(pvalues)
        
        if sum([len(l) for l in values]) == 0:
            pass
            # TODO: print out an error message
        else:
            if self.graphtype in [constants.GRAPH_LINE_VALUE, constants.GRAPH_LINE_AVG]:
                graph = graphs.LineGraph
                legends = self.profiles
            elif self.graphtype in [constants.GRAPH_STEP_VALUE]:
                graph = graphs.StepGraph
                legends = self.profiles
            elif self.graphtype in [constants.GRAPH_CUMULATIVE]:
                graph = graphs.CumulativeGraph
                legends = self.profiles
            elif self.graphtype in [constants.GRAPH_POINTLINE_VALUEAVG]:
                graph = graphs.PointAndLineGraph
                legends = []
                for l in self.profiles:
                    legends.append(l)
                    legends.append(l + " (avg)")
            elif self.graphtype in [constants.GRAPH_NUMNODE_LENGTH_CORRELATION_SIZE]:
                graph = graphs.ScatterGraph
                legends = self.profiles
                titlex = "Length of lease (s)"
                titley = "Number of nodes"
            elif self.graphtype in [constants.GRAPH_NUMNODE_LENGTH_CORRELATION_Y]:
                graph = graphs.ScatterGraph
                legends = self.profiles
                titlex = "Length of lease (s)"
                titley = self.title
            elif self.graphtype in [constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_SIZE]:
                graph = graphs.ScatterGraph
                legends = self.profiles
                titlex = "Requested length of lease (s)"
                titley = "Number of nodes"
            elif self.graphtype in [constants.GRAPH_NUMNODE_REQLENGTH_CORRELATION_Y]:
                graph = graphs.ScatterGraph
                legends = self.profiles
                titlex = "Requested length of lease (s)"
                titley = self.title
                
            if titlex==None:
                titlex = "Time (s)"
            if titley==None:
                titley = self.title
            


            if self.slideshow:
                smallestX = min([min([p[0] for p in l]) for l in values])
                largestX = max([max([p[0] for p in l]) for l in values])
                limx=(smallestX,largestX)
                smallestY = min([min([p[1] for p in l]) for l in values])
                largestY = max([max([p[1] for p in l]) for l in values])
                limy=(smallestY,largestY)
                for p, v in zip(self.profiles, values):
                    g = graph([v], titlex, titley, None, limx=limx, limy=limy)
                    if filename==None:
                        graphfile = outdir + "/s_" + p + "-" + self.graphfile
                        thumbfile = outdir + "/s_" + p + "-" + self.thumbfile
                    else:
                        graphfile = outdir + "/s_" + p + "-" + filename + ".png"
                        thumbfile = outdir + "/s_" + p + "-" + filename + "-thumb.png"                    
                    g.plotToFile(graphfile, thumbfile)
            else:
                g = graph(values, titlex, titley, legends)
                if filename==None:
                    graphfile = outdir + "/" + self.graphfile
                    thumbfile = outdir + "/" + self.thumbfile
                else:
                    graphfile = outdir + "/" + filename + ".png"
                    thumbfile = outdir + "/" + filename + "-thumb.png"                    
                g.plotToFile(graphfile, thumbfile)
        
    def generateHTML(self):
        if self.slideshow:
            graphfile = "s_" + self.profiles[0] + "-" + self.graphfile
            thumbfile = "s_" + self.profiles[0] + "-" + self.thumbfile
            html = ""
            html += "<div class='image'>"
            html += "<a href='%s'><img src='%s' id='graph%i' border='0'/></a>" % (graphfile, thumbfile, id(self))
            html += "<br/><b><span id='graphtitle%i'>%s</span></b><br/>" % (id(self), self.profiles[0])
            for p in self.profiles:
                graphfile = "s_" + p + "-" + self.graphfile
                thumbfile = "s_" + p + "-" + self.thumbfile
                html+= "[<a href='#' onclick=\"document.getElementById('graph%i').src='%s';" % (id(self), thumbfile)
                html+= "document.getElementById('graph%i').href='%s';" % (id(self), graphfile)
                html+= "document.getElementById('graphtitle%i').innerHTML='%s';" % (id(self), p)
                html+= "return false"
                html+= "\">%s</a>]&nbsp;&nbsp;&nbsp;" % p
            html += "</div>"
        else:
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
    def __init__(self, configfile, statsdir, htmlonly, mode=constants.REPORT_ALL):
        self.configfile = configfile
        self.config = RMMultiConfig.fromFile(self.configfile)
        self.statsdir = statsdir
        self.htmlonly = htmlonly
        self.mode = mode

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
  
        self.sections = []
        graphs = self.config.getGraphSections()
        for g in graphs:
            title = self.config.getGraphTitle(g)
            datafile = self.config.getGraphDatafile(g)
            graphtype = self.config.getGraphType(g)
            tablefinal = self.config.getGraphTable(g)
            clip = self.config.getGraphClip(g)
            slideshow = self.config.getGraphSlideshow(g)
            
            s = Section(title = title, datafile = datafile, graphtype = graphtype,
                        tablefinal = tablefinal, clip = clip, slideshow = slideshow)
            self.sections.append(s)

        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        
        


    def generate(self, onlyprofile=None, onlytrace=None, configfilename=None):
        def getProfilesDirs(t):
            profilesdirs = [(p, self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for p in self.profiles]
            profilesdirs = dict([(p,d) for p,d in profilesdirs if os.path.exists(d)])
            return profilesdirs

        def getTracesDirs(p):
            tracesdirs = [(t[2], self.statsdir + "/" + genDataDirName(p,t[0],t[1])) for t in self.traces]
            tracesdirs = dict([(p,d) for p,d in tracesdirs if os.path.exists(d)])
            return tracesdirs
        
        if self.mode == constants.REPORT_ALL:
            self.generateIndex()
            for t in self.traces:
                print "Generating report for trace %s" % t[2]
                profilesdirs = getProfilesDirs(t)
                if len(profilesdirs) > 0:
                    self.generateReport(t[2],profilesdirs)
            for p in self.profiles:
                print "Generating report for profile %s" % p
                tracesdirs = getTracesDirs(p)
                if len(tracesdirs) > 0:
                    self.generateReport(p, tracesdirs)
        elif self.mode == constants.REPORT_BASH or self.mode == constants.REPORT_CONDOR:
            self.generateIndex()
            if self.mode == constants.REPORT_CONDOR:
                print generateCondorHeader(logname="report")
            elif self.mode == constants.REPORT_BASH:
                print "#!/bin/bash\n\n"
            for t in self.traces:
                profilesdirs = getProfilesDirs(t)
                if len(profilesdirs) > 0:
                    command  = "/home/borja/bin/vw/haizea-report-single-trace -c %s -s %s" % (self.configfile, self.statsdir)
                    command += " --trace %s" % t[0]
                    command += " --inj %s" % t[1]
                    if self.mode == constants.REPORT_CONDOR:
                        print generateCondorQueueEntry(command=command, dir="./")
                    elif self.mode == constants.REPORT_BASH:
                        print "python2.5", command
            for p in self.profiles:
                tracesdirs = getTracesDirs(p)
                if len(tracesdirs) > 0:
                    command  = "/home/borja/bin/vw/haizea-report-single-profile -c %s -s %s" % (self.configfile, self.statsdir)
                    command += " --profile %s" % p
                    if self.mode == constants.REPORT_CONDOR:
                        print generateCondorQueueEntry(command=command, dir="./")
                    elif self.mode == constants.REPORT_BASH:
                        print "python2.5", command
        elif self.mode == constants.REPORT_SINGLE_TRACE:
            print "Generating report for trace %s" % onlytrace[2]
            profilesdirs = getProfilesDirs(onlytrace)
            if len(profilesdirs) > 0:
                self.generateReport(onlytrace[2],profilesdirs)
        elif self.mode == constants.REPORT_SINGLE_PROFILE:
            print "Generating report for profile %s" % onlyprofile
            tracesdirs = getTracesDirs(onlyprofile)
            if len(tracesdirs) > 0:
                self.generateReport(onlyprofile,tracesdirs)
            

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
        if not self.htmlonly:
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
        table = self.generateTableSummary(dirs)
        reportfile.write(table)
        
        csvfile = "summary.csv"
        html  = "<div class='center'><div class='small'>"
        html += "[ <a href='%s'>CSV file</a> ]" % csvfile
        html += "</div></div>"
        reportfile.write(html)

        self.generateCSVSummary(outdir + "/" + csvfile, dirs)
            
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
    
    def generateTableSummary(self, dirs):   
        profiles = dirs.keys()
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
    
    def generateCSVSummary(self, csvfile, dirs):  
        profiles = dirs.keys()
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
        

