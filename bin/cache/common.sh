# Cache managing functions
# Algorithm-independent functions
# Borja Sotomayor 08-01-06


# Gets a cached image, given its identifier.
# Paramters:
#     IMG_ID: Image identifier
function isImageCached()
{
    local IMG_ID=$1
    if [ -e $CACHED_IMG ];
    then
	# Must also show up in the cache db
	if grep "^$IMG_ID;" $LOCAL_CACHE_DB > /dev/null;
	then
	    return 0 # Cache hit
	else
	    return 1 # Cache miss
	fi
    else
	return 1 # Cache miss
    fi
}

function updateCacheFile()
{
    local CACHE_DB=$1
    local IMG_ID=$2
    local TIMESTAMP=$3
    lockAndWait $CACHE_DB
    #If the image was not previously in the cache, add it
    if ! grep "^$IMG_ID;" $CACHE_DB > /dev/null;
    then
        echo "$IMG_ID;$TIMESTAMP;1;0" >> $CACHE_DB
    else
        awk -F";" -v img_id="$IMG_ID" -v timestamp="$TIMESTAMP" '$1==img_id {print $1";"timestamp";"$3+1";"$4} $1!=img_id {print}' $CACHE_DB > $CACHE_DB.new
        mv $CACHE_DB.new $CACHE_DB
    fi
    unlock $CACHE_DB
}

function updateDurationCacheFile()
{
    local CACHE_DB=$1
    local IMG_ID=$2
    local DURATION=$3
    local TIMESTAMP=`date +%s`
    lockAndWait $CACHE_DB
    #If the image was not previously in the cache, add it
    if ! grep "^$IMG_ID;" $CACHE_DB > /dev/null;
    then
        echo "$IMG_ID;$TIMESTAMP;1;$DURATION" >> $CACHE_DB
    else
        awk -F";" -v img_id="$IMG_ID" -v duration=$DURATION '$1==img_id {print $1";"$2";"$3";"$4+duration} $1!=img_id {print}' $CACHE_DB > $CACHE_DB.new
        mv $CACHE_DB.new $CACHE_DB
    fi
    unlock $CACHE_DB
}

function updateCache()
{
    local IMG_ID=$1
    local TIMESTAMP=`date +%s`

    # If the cache doesn't exist, create it
    if [ ! -e $LOCAL_CACHE_DB ];
    then
	echo "$IMG_ID;$TIMESTAMP;1;0" > $LOCAL_CACHE_DB
    else
        updateCacheFile $LOCAL_CACHE_DB $IMG_ID $TIMESTAMP
    fi	
    
    if [ $CACHE_GLOBAL == "true" ];
    then
	if [ ! -e "$GLOBAL_CACHE_DB" ];
        then
        	echo "$IMG_ID;$TIMESTAMP;1;0" > $GLOBAL_CACHE_DB
	else
	    updateCacheFile $GLOBAL_CACHE_DB $IMG_ID $TIMESTAMP
	fi
    fi
}

function freeCache()
{
    SIZE=$1

    AVAILABLE=$(($CACHE_SIZE - `du -b $CACHE_DIR/ | cut -f1`))
    while [ $AVAILABLE -lt $SIZE ];
    do
	log "Not enough cache space (have $AVAILABLE bytes, need $SIZE bytes)"
        lockAndWait $LOCAL_CACHE_DB
        if [ $CACHE_GLOBAL == "true" ];
	then
	    lockAndWait $GLOBAL_CACHE_DB
	    getReplaceableImage REPL_IMG $GLOBAL_CACHE_DB
	    unlock $GLOBAL_CACHE_DB
        else
	    getReplaceableImage REPL_IMG $LOCAL_CACHE_DB
	fi	
	log "Replacing image $REPL_IMG"
	lockAndWait $CACHE_DIR/$REPL_IMG
	rm $CACHE_DIR/$REPL_IMG
        awk -F";" -v img_id="$REPL_IMG" '$1==img_id {} $1!=img_id {print}' $LOCAL_CACHE_DB > $LOCAL_CACHE_DB.new
        mv $LOCAL_CACHE_DB.new $LOCAL_CACHE_DB
	unlock $LOCAL_CACHE_DB
	unlock $CACHE_DIR/$REPL_IMG
        AVAILABLE=$(($CACHE_SIZE - `du -b $CACHE_DIR/ | cut -f1`))
    done
    debug "Cache has $AVAILABLE available bytes"
}
