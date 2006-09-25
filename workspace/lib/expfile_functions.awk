function parseQuotedFields()
{
    do
    {
	old=$0
	$0=gensub(/\"([^ ]*)\"/, "'\\1'", "g")
	$0=gensub(/(\"[^ ]*)[ ](.*\")/, "\\1"SUBSEP"\\2","")
    }
    while (old!=$0)
}

function qf2str(qf)
{
    gsub("'","",qf)
    gsub(SUBSEP," ",qf)
    return qf
}

function getSubArray(a, i, subarray)
{	
    for(multiindex in a)
    {
	split (multiindex, indices, SUBSEP)
	if (indices[1] == i)
	    subarray[indices[2]]=a[multiindex]
    }
}


function waitForSubmissions()
{
    print "echo \">>> Waiting for submission to end...\""
    print "WAIT=0"
    print "DONE_WAIT=false"
    print "while ! $DONE_WAIT;"
    print "do"
    print "     sleep 30"
    print "     WAIT=$(($WAIT + 30))"
    print "     QSTAT=$(qstat | wc -c)"
    print "     if [ $QSTAT -eq 0 ];"
    print "     then"
    print "             DONE_WAIT=true"
    print "     elif [ $WAIT -gt 5400 ];"
    print "     then"
    print "             echo 'Waited 1h30m for this trace to run. Something must be wrong. Aborting!'"
    print "             qdel -u root"
    print "             DONE_WAIT=true"
    print "     fi"
    print "done"

}

function clearCaches()
{
    print "vw-cache-clear-all"
    print "sleep 45"
    dirty = 0
}


function createConfigFile(c)
{
    print "CONFIG_FILE=`tempfile -d "  ENVIRON["WORKSPACE_DIR"] "/etc -p config -m 644`"
    for (key in configValues)
	print "echo '" key ": " configValues[key] "' >> $CONFIG_FILE"
    for (key in default)
	print "echo '" key ": " default[key] "' >> $CONFIG_FILE"
}



function getGraphDir(start)
{
    graphDir=timestamp "/"
    if(experiment!="")
    {
        graphDir=graphDir experiment 
        for (i=start;i<=NF;i++)
        {
	    logid=qf2str($i)
    	    gsub(" ", "", logid)
	    graphDir = graphDir "-" logid
	}
    }
    else
    {
	graphDir=graphDir "common"
        for (i=start;i<=NF;i++)
        {
	    logid=qf2str($i)
    	    gsub(" ", "", logid)
	    graphDir = graphDir "-" logid
	}    
    }
    return graphDir
}

function getLogIDs(start)
{
    log_ids=""
    if(experiment!="")
    {
        for (i=start;i<=NF;i++)
        {
	    logid=qf2str($i)
    	    gsub(" ", "", logid)
	    logID = timestamp "-" experiment "-" logid
	    log_ids = log_ids logID " "
	}
    }
    else
    {
        for (i=start;i<=NF;i++)
        {
	    split($i,explog,":")
	    expaux=explog[1]
	    logid=qf2str(explog[2])
    	    gsub(" ", "", logid)
	    logID = timestamp "-" expaux "-" logid
	    log_ids = log_ids logID " "
	}    
    }
    return log_ids
}

function getTags(start)
{
    tags=""
    for (i=start;i<=NF;i++)
    {
        tags = tags $i " "
    }
    return tags
}

