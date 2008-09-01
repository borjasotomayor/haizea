import haizea.cli.commands as cmd
import haizea.cli.rpc_commands as rpccmd
from docutils.core import publish_string
import re

commands = [cmd.haizea, rpccmd.haizea_request_lease, rpccmd.haizea_cancel_lease, rpccmd.haizea_list_leases,
            rpccmd.haizea_show_queue, rpccmd.haizea_list_hosts, cmd.haizea_generate_configs, cmd.haizea_generate_scripts, cmd.haizea_convert_data]



for command in commands:
    c = command([])
    print "\\section{\\texttt{%s}}" % command.name
    print
    doc = command.__doc__
    latexdoc = publish_string(doc,  writer_name="latex")
    latexdoc = re.compile("\\\\begin{document}\n\n\\\\setlength{\\\\locallinewidth}{\\\\linewidth}\n\n(.*)\\\\end{document}", flags=re.DOTALL).search(latexdoc)
    print latexdoc.group(1)
    print
    print "\\begin{center}\\begin{tabular}{|l|p{6cm}|}"
    print "\\hline"
    print "\\sffamily\\bfseries Option & \\sffamily\\bfseries Description \\\\ \\hline\\hline"
    opts = c.optparser.option_list
    c.optparser.formatter.store_option_strings(c.optparser)
    for opt in opts:
        if opt.action != "help":
            opt_string = c.optparser.formatter.option_strings[opt]            
            print "\\texttt{%s} & \\sffamily %s \\\\ \\hline" % (opt_string, opt.help)
    print "\\end{tabular}\\end{center}"
    print
        