import sys

filename = sys.argv[1]
out = sys.argv[2]


file = open(filename, "r")
outfile = open(out, "w")

first = None
for line in file:
    if line[0]!=";":
        fields = line.split()
	queue = int(fields[14])
	runtime = int(fields[3])
	status = int(fields[10])
	if int(fields[14]) in (1,2,3,4):
            time = int(fields[1])
	    if first == None:
		first = time
	    fields[1] = time - first

	    nprocs = int(fields[7])
	    fields[7] = nprocs / 8

	    outfile.write("%s\n" % " ".join([str(s) for s in fields]))
	

file.close()
outfile.close()