# Common functions and variables

LOCAL_CACHE_DB="$WORKSPACE_VAR/image_cache_db"
GLOBAL_CACHE_DB="$WORKSPACE_VAR/global/image_cache_db"
CACHE_DIR="$WORKSPACE_VAR/image_cache"
IMG_DIR="$WORKSPACE_VAR/images"
LOCALIMG_DIR="$WORKSPACE_VAR/image_local"
LOG_DIR="$WORKSPACE_VAR/global/logs"
LOCK_DIR="$WORKSPACE_VAR/lock"

CACHED_IMG="$CACHE_DIR/$IMG_ID"
DOWNLOAD_IMG="$CACHED_IMG.downloading"
if [ "$LOCALIMGTAG" == "" ];
then
    LOCAL_IMG="$LOCALIMG_DIR/$IMG_ID.$JOB_ID.$SGE_TASK_ID"
else
    LOCAL_IMG="$LOCALIMG_DIR/$IMG_ID.$LOCALIMGTAG"
fi

getConfigValue()
{
    echo `grep $1 $CONFIG_FILE | cut -d";" -f1 | cut -d: -f2 | xargs`
}

readConfig()
{
    CACHE_TYPE=`getConfigValue "caching_algorithm"`
    CACHE_GLOBAL=`getConfigValue "global_caching"`

    # 16384 is a kludge. du also counts the directory's own inode (4kb, I'm overcounting just in case)
    # This amount is negligible for huge files, but annoying when running tests
    CACHE_SIZE=$((`getConfigValue "cache_size"` * 1024 * 1024 + 16384)) 

    DEBUG=`getConfigValue "debug"`
    CHECKSUM=`getConfigValue "checksum"`
    TRANSFERSCHED=`getConfigValue "transfer_scheduling"`
    NETBW=`getConfigValue "network_bandwidth"`
}

log()
{
    local MSG=$1
    local TIMESTAMP=`date +"%s.%N"`

    local LOGMSG="$TIMESTAMP `hostname` [$JOB_NAME/$JOB_ID/$SGE_TASK_ID/$TAG] $MSG"

    echo $LOGMSG | tee -a $LOG_DIR/imagetransfer_${LOG_ID}_`hostname`.log
}

debug()
{
    if [ $DEBUG ] ;
    then
        log "    [DEBUG] $1"
    fi
}

lockAndWait()
{
    local FILE=$1
    debug "Attempting to lock file $1 (waiting if necessary)"
    if dotlockfile $1.lock;
    then
	debug "Locked file $1"
    else
	log "Could not lock file $1. Error code: $?"
    fi
}

lock()
{
    local FILE=$1
    debug "Attempting to lock file $1 (not waiting)"
    dotlockfile -r 0 $1.lock
    return $?
}

testLock()
{
    local FILE=$1
    dotlockfile -c $1.lock
    return $?
}

unlock()
{
    local FILE=$1
    debug "Attempting to unlock file $1"
    if dotlockfile -c $1.lock;
    then
	dotlockfile -u $1.lock
	debug "Unlocked file $1"
	return 0
    else
	log "Could not unlock file $1"
	if [ -f $1.lock ];
	then
	    log "Lockfile does exist (stale lockfile?). Deleting..."
	    rm $1.lock
	else
	    log "Lockfile does not exist"
	fi
	return 1
    fi
}

downloadLock()
{
    lock "$1.download"
    return $?
}

downloadUnlock()
{
    unlock "$1.download"
    return $?
}

waitForJob()
{
    local JOB_ID=$1
    local SEC=$2
    local SLEEP=$3

    WAIT=0
    DONE_WAIT=false
    log "Waiting for job $JOB_ID"
    while ! $DONE_WAIT;
    do
        sleep $SLEEP
        WAIT=$(($WAIT + $SLEEP))
        QSTAT=$(qstat -j $JOB_ID | wc -c)
        if [ $QSTAT -eq 0 ];
        then
                DONE_WAIT=true
        elif [ $WAIT -gt $SEC ];
        then
                log 'Waited $SEC seconds for job $JOB_ID. Aborting!'
                qdel $JOB_ID
                DONE_WAIT=true
        fi
    done
}
