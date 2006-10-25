BEGIN	{
    print "<?xml version='1.0' encoding='UTF-8'?>"
    print "<run-experiments runDate='" strftime("%a %b %d %H:%M:%S %Z %Y", timestamp) "' genDate='" strftime()  "'>"
    state="pre"
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
	print "<experiments>"
	state = "experiment"
    }
    else if (state == "experiment")
    {
	print "</experiment-group>"
    }
    else if (state == "experiment_graph")
    {
	print "</graphs>"
	print "</experiment-group>"
	state = "experiment"
    }

    print "<experiment-group id='" $2 "'>"
    experiment=$2
}

$1 == "using" && $2 == "trace" {
    if (state != "experiment")
	exit 1
    print "<trace>" $3 "</trace>"
}

$1 == "description" {
    if (state != "experiment")
	exit 1
    desc = $2
    for (i=3;i<=NF;i++)
	desc = desc " " $i
    print "<description>" desc "</description>"
}

$1 == "multirun" {
    getSubArray(multirun, $2, runs)
    for (run in runs)
    {
	print "<experiment label='" runs[run] "'>"
	getSubArray(config, runs[run], configValues)
	print "<configuration>"
	for (key in configValues)
	    print "<config name='" key "' value='" configValues[key] "'/>"
	for (key in default)
	    print "<config name='" key "' value='" default[key] "'/>"
	print "</configuration>"
	print "</experiment>"
    }
}

$1 == "graph" {
    parseQuotedFields()
    if (state=="experiment")
    {
	print "<graphs>"
	state = "experiment_graph"
    }
    if ($2=="hitmiss")
    {
	graphTitle="Cache hit/miss"
	graphDir=getGraphDir(3)
    }
    else if ($2=="avg")
    {
	graphTitle="Average duration between " $3 " and " $4
	graphDir=getGraphDir(5)
    }
    else if ($2=="count")
    {
	graphTitle="Accumulation of " $3
	graphDir=getGraphDir(4)
    }
    else if ($2=="durationstats")
    {
	graphTitle="Duration stats for [" $6 " ->  " $7"]:"
        graphdirpost= "duration-"

	if ($3~"true")
	{
            graphTitle = graphTitle " --AVERAGE--"
            graphdirpost=graphdirpost "avg"
	}
        if ($4~"true")
	{
            graphTitle = graphTitle " --CUMULATIVE--"
            graphdirpost=graphdirpost "cum"
	}
        if ($5~"true")
	{
            graphTitle = graphTitle " --DURATION--"
            graphdirpost=graphdirpost "dur"
	}

        graphDir=getGraphDir(8) "-" graphdirpost
        log_ids=getLogIDs(8)
    }
    else if ($2=="ratiostats")
    {
	graphTitle="Stats for ratio [" $5 " /  " $6"]:"
        graphdirpost= "ratio-"

	if ($3~"true")
	{
            graphTitle = graphTitle " --AVERAGE--"
            graphdirpost=graphdirpost "avg"
	}
        if ($4~"true")
	{
            graphTitle = graphTitle " --RATIO--"
            graphdirpost=graphdirpost "rat"
	}

        graphDir=getGraphDir(7) "-" graphdirpost
        log_ids=getLogIDs(7)
    }
    else if ($2=="t2d")
    {
	configName=qf2str($3)
        gsub(" ", "", configName)
	graphTitle="Transfer-to-duration ratio for configuration " configName
        graphDir= getGraphDir(4) "-" configName
    }
    else 
    {
        graphTitle="Unknown graph type"
	graphDir="UNKNOWN"
    }
    print "<graph title='" graphTitle "' graphDir='" graphDir "'/>"
}

$1 == "GRAPHS" {
    if (state == "experiment")
    {
	print "</experiment-group>"	
	print "</experiments>"
	print "<graphs>"
	state = "graphs"
	experiment = ""
    }
    else if (state == "experiment_graph")
    {
	print "</graphs>"	
	print "</experiment-group>"	
	print "</experiments>"
	print "<graphs>"
	state = "graphs"
	experiment = ""
    }
}

END	{

    if (state == "graphs")
	print "</graphs>"
    if (state == "experiment_graph")
    {
	print "</graphs>"
	print "</experiment-group>"
	print "</experiments>"
    }
    if (state == "experiment")
    {
	print "</experiment-group>"
	print "</experiments>"
    }
    print "</run-experiments>"
}
