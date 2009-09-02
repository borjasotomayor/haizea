#!/bin/bash

python gen_cli_doc.py > appendix_cli.tex
python gen_config_doc.py > appendix_conf.tex
python gen_probe_doc.py > appendix_probes.tex

latex2html -split 4 -dir ../../html/manual -show_section_numbers -local_icons -toc_depth 3 -link 2 -top_navigation -bottom_navigation manual.tex
tar cvzf ../../html/downloads/haizea-manual-multiple.tar.gz ../../html/manual

latex2html -split 0 -no_navigation -dir ../../html/manual_single -show_section_numbers -local_icons -toc_depth 3 manual.tex 
tar cvzf ../../html/downloads/haizea-manual-single.tar.gz ../../html/manual_single
#tidy --clean y --doctype "transitional" --output-xhtml y --indent "auto" --wrap "90" --char-encoding "utf8" --logical-emphasis y

cp manual.pdf ../../html/haizea_manual.pdf



