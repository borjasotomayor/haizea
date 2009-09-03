from haizea.common.utils import rst2latex
from haizea.pluggable.accounting import probe_class_mappings
from haizea.pluggable.accounting.leases import ARProbe, BEProbe, IMProbe
from haizea.pluggable.accounting.utilization import  CPUUtilizationProbe, DiskUsageProbe

probes = [ARProbe, BEProbe, IMProbe, CPUUtilizationProbe, DiskUsageProbe]

inv_mappings = dict([(v,k) for k,v in probe_class_mappings.items()])

for probe in probes:
    fullname = probe.__module__ + "." + probe.__name__
    print "\\section{\\texttt{%s}}" % probe.__name__
    print "\\noindent\\textbf{Full name:} \\texttt{%s}" % fullname
    if inv_mappings.has_key(fullname):
        print "\\\\ \\textbf{Short name:} \\texttt{%s}" % inv_mappings[fullname]
    print "\\\\ \\textbf{Description:} \\\\"
    print rst2latex(probe.__doc__)