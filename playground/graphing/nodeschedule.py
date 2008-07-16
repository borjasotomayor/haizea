from haizea.common.constants import DOING_IDLE, DOING_VM_RUN, DOING_VM_SUSPEND, DOING_VM_RESUME
from pyx import *

HEIGHT_BAR=1
SCALE=100.0

doing = { 1: [ (   0,   60, DOING_IDLE, None),
               (  60,  540, DOING_VM_RUN, 1),
               ( 540,  600, DOING_VM_SUSPEND, 1),
               ( 600,  900, DOING_VM_RUN, 2),
               ( 900,  960, DOING_VM_RESUME, 1),
               ( 960, 1800, DOING_VM_RUN, 1)],
               
          2: [ (   0,  600, DOING_IDLE, None),
               ( 600,  900, DOING_VM_RUN, 2),
               ( 900, 1800, DOING_IDLE, None)],
               
          3: [ (   0,  600, DOING_IDLE, None),
               ( 600,  900, DOING_VM_RUN, 2),
               ( 900, 1800, DOING_IDLE, None)],
               
          4: [ (   0,  600, DOING_IDLE, None),
               ( 600,  900, DOING_VM_RUN, 2),
               ( 900, 1800, DOING_IDLE, None)]
          }

colors = { DOING_IDLE: color.gray(0.99),
           DOING_VM_RUN: color.cmyk.SpringGreen,
           DOING_VM_SUSPEND: color.cmyk.Dandelion,
           DOING_VM_RESUME: color.cmyk.CornflowerBlue}

def scale(n):
    return n/SCALE

def draw_node(canvas, x, y, doing, node):
    max_x = scale(doing[-1][1])
    canvas.stroke(path.rect(x, y, max_x, HEIGHT_BAR), [style.linewidth(0.1), color.cmyk.Gray])
    c.text(x-2, y+(HEIGHT_BAR/2.0), "Node %i" % node, [text.parbox(3), text.halign.boxleft, text.valign.middle])


    for d in doing:
        start_x = x + scale(d[0])
        width = scale(d[1] - d[0])
        canvas.fill(path.rect(start_x, y, width, HEIGHT_BAR), [colors[d[2]]])

x, y = 0, 0
max_x = scale(doing[1][-1][1])
#c = canvas.canvas()
p = graph.axis.painter.regular(outerticklength=graph.axis.painter.ticklength.normal,
                               innerticklength=0)
c=graph.axis.pathaxis(path.line(x, y-3, max_x, y-3),
                        graph.axis.linear(min=0, max=doing[1][-1][1], title="Time (s)", painter=p))


for node in doing:
    draw_node(c, x, y, doing[node], node)
    y -= HEIGHT_BAR
    
    
c.writePDFfile("/tmp/haizea-test.pdf")
