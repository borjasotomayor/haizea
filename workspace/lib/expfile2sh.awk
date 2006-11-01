BEGIN	{
    print "#!/bin/bash"
    print
    print "source /usr/workspace/bin/common.sh"
    dirty=1
    state="pre"
    if (timestamp=="")
    {
	if (forcetimestamp=="")
	    timestamp=systime()
	else
	    timestamp=forcetimestamp
	regenerate = "false"
	print "echo " timestamp " > timestamp"
        clearCaches()
    }
    else
	regenerate = "true"
    tag = "NONE"
}

$1 == "default"	{
    default[$2]=$3
}

$1 == "define" && $2 == "config" {
    parseQuotedFields()
    configName=qf2str($3)
    for (i=5;i<=NF;i++)
    {
	split($i, namevalue, "=")
	config[configName,namevalue[1]]=qf2str(namevalue[2])
    }  
}

$1 == "define" && $2 == "multirun" {
    parseQuotedFields()
    multirunName=qf2str($3)
    for (i=5;i<=NF;i++)
    {
	multirun[multirunName, (i-4)]=qf2str($i)
    }
}

$1 == "EXPERIMENT" {
    if (state == "pre")
    {
	state = "experiment"
    }
    else if (state == "experiment")
    {

    }
    else if (state == "experiment_graph")
    {
	state = "experiment"
    }

    experiment=$2
}

$1 == "using" && $2 == "trace" {
    if (state != "experiment")
	exit 1
    traceFile=ENVIRON["WORKSPACE_DIR"] "/traces/" $3
}


$1 == "multirun" && regenerate=="false" {
    getSubArray(multirun, $2, runs)
    for (run in runs)
    {
	getSubArray(config, runs[run], configValues)
	createConfigFile(configValues)
	
	configName=runs[run]
	gsub(" ", "", configName)
	logID = timestamp "-" experiment "-" configName
	runCmd = "vw-trace-run -f " traceFile " -l " logID " -c $CONFIG_FILE"
        print "echo \">>> " runCmd "\""
	print runCmd
	waitForSubmissions()
	clearCaches()
    }
}

$1 == "tag" {
    tag = $2
    if(tag=="NONE")
	tagopt = ""
    else
	tagopt = "-g " tag
}

$1 == "graph" {
    parseQuotedFields()
    log_ids=""
    if ($2=="hitmiss")
    {
        graphdirpost="hitmiss-"
	graphDir=getGraphDir(3) "-" graphdirpost
	log_ids=getLogIDs(3)
	runCmd = "vw-log-plot-cachehitmiss -d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir " -o png " tagopt " " log_ids
    }
    else if ($2=="avg")
    {
	begin=$3
	end=$4
	graphDir=getGraphDir(5)
	log_ids=getLogIDs(5)
	runCmd = "vw-plot-avg-duration -d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir " -o png -b " begin " -e " end " "  tagopt " " log_ids
    }
    else if ($2=="count")
    {
	event=$3
        graphdirpost="count" event "-"
	graphDir=getGraphDir(4) "-" graphdirpost
	log_ids=getLogIDs(4)
	runCmd = "vw-plot-count -d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir " -o png -e " event " "  tagopt " "log_ids
    }
    else if ($2=="durationstats")
    {
	avgflag = cumulflag = durflag = ""
	graphdirpost="duration-"
	if ($3~"true")
	{
	    avgflag=" -a "
	    graphdirpost=graphdirpost "avg"
	}
	if ($4~"true")
	{
	    cumulflag=" -c "
	    graphdirpost=graphdirpost "cum"
	}
	if ($5~"true")
	{
	    durflag=" -t "
	    graphdirpost=graphdirpost "dur"
	}
	begin=$6
	end=$7
	graphDir=getGraphDir(8) "-" graphdirpost
	log_ids=getLogIDs(8)
	runCmd = "vw-plot-duration-stats " avgflag cumulflag durflag "-d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir " -o png -b " begin " -e " end " "  tagopt " " log_ids
    }    
    else if ($2=="ratiostats")
    {
	avgflag = durflag = ""
	graphdirpost="ratio-"
	if ($3~"true")
	{
	    avgflag=" -a "
	    graphdirpost=graphdirpost "avg"
	}
	if ($4~"true")
	{
	    ratioflag=" -r "
	    graphdirpost=graphdirpost "rat"
	}
	event1=$5
	event2=$6
	graphDir=getGraphDir(7) "-" graphdirpost
	log_ids=getLogIDs(7)
	runCmd = "vw-plot-ratio-stats " avgflag ratioflag "-d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir " -o png -x " event1 " -y " event2 " " tagopt " " log_ids
    }   else if ($2=="t2d")
    {
	# Will only work in experiment section
	configName=qf2str($3)
	gsub(" ", "", configName)
	graphDir=getGraphDir(4)
	tags=getTags(4)
	logid=timestamp "-" experiment "-" configName
	runCmd = "vw-plot-t2d -l " logid " -d " ENVIRON["WORKSPACE_VAR"] "/graphs/" graphDir "-" configName " -o png "  tags
    }
    print "echo \">>> " runCmd "\""
    print runCmd
}


$1 == "GRAPHS" {
    state = "graphs"
    experiment = ""
}

END	{

}
