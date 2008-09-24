from haizea.resourcemanager.configfile import HaizeaConfig
from haizea.common.config import OPTTYPE_INT, OPTTYPE_FLOAT, OPTTYPE_STRING, OPTTYPE_BOOLEAN, OPTTYPE_DATETIME, OPTTYPE_TIMEDELTA 
from docutils.core import publish_string
import re

for s in HaizeaConfig.sections:
    print "\\section{Section \\texttt{[%s]}}" % s.name
    print
    print s.get_doc()
    print
    if s.required:
        print "\\emph{This section is required}"
    else:
        if s.required_if:
            print "This section is required when:"
            print "\\begin{itemize}"
            for r in s.required_if:
                sec,opt = r[0]
                val = r[1]
                print "\\item"
                print "Option \\texttt{%s} (in section \\texttt{[%s]})" % (opt,sec)
                print "is set to \\texttt{%s}" % val
            print "\\end{itemize}"
        else:
            print "This section is optional."
    print
    
    for opt in s.options:
        print "\\subsection{Option \\texttt{%s}}" % opt.name
        print "\\begin{description}"
        print "\\item[Valid values:]"
        if opt.valid:
            print ", ".join(["\\texttt{%s}" % v for v in opt.valid])
        else:
            if opt.type == OPTTYPE_INT:
                print "An integer number"
            elif opt.type == OPTTYPE_FLOAT:
                print "A real number"
            elif opt.type == OPTTYPE_STRING:
                print "Any string"
            elif opt.type == OPTTYPE_BOOLEAN:
                print "\texttt{True} or \texttt{False}"
            elif opt.type == OPTTYPE_DATETIME:
                print "An ISO timestamp: i.e., \\texttt{YYYY-MM-DD HH:MM:SS}"
            elif opt.type == OPTTYPE_TIMEDELTA:
                print "A duration in the format \\texttt{HH:MM:SS}"

        print "\\item[Required:]"
        if opt.required:
            print "Yes"
        else:
            if opt.required_if:
                print "Only if"
                print "\\begin{itemize}"
                for r in opt.required_if:
                    sec,o = r[0]
                    val = r[1]
                    print "\\item"
                    print "Option \\texttt{%s} (in section \\texttt{[%s]})" % (o,sec)
                    print "is set to \\texttt{%s}" % val
                print "\\end{itemize}"
            else:
                print "No"
                if opt.default:
                    print "(default is \\texttt{%s})" % opt.default

        print "\\item[Description:]"
        optdoc = opt.get_doc()
        latexoptdoc = publish_string(optdoc,  writer_name="latex")
        latexoptdoc = re.compile("\\\\begin{document}\n\n\\\\setlength{\\\\locallinewidth}{\\\\linewidth}\n\n(.*)\\\\end{document}", flags=re.DOTALL).search(latexoptdoc)
        print latexoptdoc.group(1)
        print "\\end{description}"
        print 
        