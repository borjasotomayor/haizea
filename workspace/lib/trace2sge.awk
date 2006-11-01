#!/usr/bin/awk  -f

BEGIN	{
    FS=";"
    i=0
    seconds=0
    timestamp=systime()
    print "#!/bin/bash"
    print
    print "source /usr/workspace/bin/common.sh"
}

{
    second=$1;
    if (second != seconds)
    {
	print "sleep " (second - seconds);
	seconds=second;
    }
    img_uri=$2;
    uri_tokens=split(img_uri, img_uri_split, "/");
    img_id=img_uri_split[uri_tokens];
    img_size=$3
    num_nodes=$4
    rw=$5
    deadline=$6

    duration=$7
    trace=$8
    print "LOG_ID=\"" tracename "\"; JOB_NAME=\"VW" i "\"; log \"--SUBMIT_BEGIN--\""
    if (configfile=="")
	configopt=""
    else
	configopt="-c " configfile

    if(deadline=="" || deadline=="NULL")
	dlopt=""
    else
    {
	deadline=$6+timestamp+seconds
	dlopt="-z " deadline
    }

    if (duration=="" || duration=="NULL")
	duropt=""
    else
	duropt="-d " duration

    if (trace=="" || trace=="NULL")
	traceopt=""
    else
	traceopt="-t " trace

    transfersopt=""

    if (transfersched == "JIT")
    {
	transfertime = num_nodes * img_size * 1024 / netbw
        transfertime += 10
	jobstart=deadline - (transfertime + 15)
	jobstart=strftime("%Y%m%d%H%M.%S", jobstart)
	sgedlopt="-a " jobstart	
    } else if (transfersched == "AsJob")
    {
	jobstart=deadline
	jobstart=strftime("%Y%m%d%H%M.%S", jobstart)
	sgedlopt="-a " jobstart	
    }
    else if (transfersched == "SmartJIT")
    {
        submittime = timestamp + seconds
	transfertime = num_nodes * img_size * 1024 / netbw
        transfertime += num_nodes * 10
	sgedl=deadline - transfertime 
	#if ((sgedl-submittime) > transfertime)
	#    sgestart=submittime + (1/3)*(sgedl-submittime)
	#else
	sgestart=submittime
	sgestart=strftime("%Y%m%d%H%M.%S", sgestart)
	sgedl=strftime("%Y%m%d%H%M.%S", sgedl)
	sgedlopt="-dl " sgedl 
        transfersopt="-hard -l transfers=" (1/num_nodes)
    } else if (transfersched == "Aggressive")
    {
	sgedlopt=""
    }

    if (deadline == "" || deadline=="NULL")
    {
	print "vw-run -N VW" i " -n " num_nodes " -i " img_uri " -l " tracename " " duropt " " configopt " " traceopt " &"
    }
    else
    {
	print "qsub " sgedlopt " -q transfer.q -soft -l vm_image=\"*(" img_id ")*\" " transfersopt " -t 1-" num_nodes " -N VW" i " " ENVIRON["WORKSPACE_DIR"] "/bin/vw-transfer-image-sgewrapper -i " img_uri " -s " img_size " -l " tracename " " configopt " " duropt " " traceopt " " dlopt
    }
    i++;
}
