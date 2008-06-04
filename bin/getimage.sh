# Image transfer functions

function downloadImage()
{
    local DST=$1

    case $IMG_PROTOCOL in
	"nfs") 
	    IMAGE_PATH=`echo $IMG | cut -f2 -d: | cut -c3-` 
	    downloadNFS $IMAGE_PATH $DST
	    RETURN=$?
	;;
	"ftp") 
	    IMAGE_SERVER=`echo $IMG | cut -f2 -d/`
	    IMAGE_PATH=`echo $IMG | cut -f3- -d/`
	    downloadFTP $IMAGE_SERVER $IMAGE_PATH $DST
	    RETURN=$?
	;;
	"gridftp") 
	    IMAGE_SERVER=`echo $IMG | cut -f2 -d/`
	    IMAGE_PATH=`echo $IMG | cut -f3- -d/`
	    downloadGridFTP $IMAGE_SERVER $IMAGE_PATH $DST
	    RETURN=$?
	;;
	*)
	    RETURN=1
	;;
    esac
    return $RETURN
}

function verifyChecksum()
{
	if [ "$CHECKSUM" == "true" ];
	then	
	    log "--CHECKSUM_START-- Computing checksum for $LOCAL_IMG..."
	    LOCALMD5=$(md5sum $LOCAL_IMG | cut -f1 -d" ")
	    GOODMD5=$(grep $IMG_ID $IMG_DIR/checksum | cut -f1 -d" ")
	    if [ $LOCALMD5 == $GOODMD5 ];
	    then
		log "--CHECKSUM_END-- Local image has correct checksum."
	    else
		log "ERROR: Checksums don't coincide. $LOCALMD5 != $GOODMD5"
		ERROR=1
	    fi
	else
	    log "--CHECKSUM_START--"
	    log "Skipping checksum"
	    log "--CHECKSUM_END--"
	fi
}

function copyToLocal()
{
    local IMG_ID=$1

    log "--LOCAL_COPY_START-- Copying $CACHED_IMG -> $LOCAL_IMG"
    START=`date +%s`
    if cp $CACHED_IMG $LOCAL_IMG;
    then
        END=`date +%s`
	TIME=$(($END - $START))
	log "--LOCAL_COPY_END-- Copied $CACHED_IMG -> $LOCAL_IMG in $TIME seconds"
	verifyChecksum
    else
	log "ERROR: Could not copy $CACHED_IMG -> $LOCAL_IMG"
	ERROR=1
    fi
}


function getImageDirectly()
{
    log "--GETIMAGE_START--"
    if downloadImage $LOCAL_IMG;
    then
	verifyChecksum
    else
	log "ERROR: Could not get image $IMG directly"
    fi
    log "--GETIMAGE_END--"
}

function getImageThroughCache()
{
    log "--GETIMAGE_START--"
    if isImageCached $IMG_ID;
    then
        log "--CACHE_HIT-- Image: $IMG_ID"
    else
	#TODO: Images that can't fit in the cache should be downloaded directly to the
	#      image directory (not to the local cache)
        if lock $DOWNLOAD_IMG;
        then
	    log "--CACHE_MISS-- Image: $IMG_ID"
            freeCache $IMG_SIZE
	    downloadImage $CACHED_IMG
            unlock $DOWNLOAD_IMG
        else
	    log "--CACHE_HALFMISS-- Image: $IMG_ID"
	    log "--DOWNLOAD_WAIT_START-- Image is already being downloaded by another process. Sleeping."
            while testLock $DOWNLOAD_IMG;
            do
                sleep 5
            done
	    log "--DOWNLOAD_WAIT_END-- Awake: Other process finished downloading image."
        fi
    fi
    log "--GETIMAGE_END--"

    if testLock $CACHED_IMG || lock $CACHED_IMG || true;
    then
	touch $CACHED_IMG.lock.$$
        log "Locked image $CACHED_IMG for cache update and copy to local directory"
	if [ -f $CACHED_IMG ];
	then
	    updateCache $IMG_ID
	    copyToLocal $IMG_ID
	    # The following 5 lines should be a critical section
	    rm $CACHED_IMG.lock.$$
	    if ! (ls $CACHED_IMG.lock.* 2> /dev/null );
	    then
		unlock $CACHED_IMG
	    fi
	else
	    log "ERROR: Another process deleted $CACHED_IMG"
	    unlock $CACHED_IMG
	    # TODO: Treat this as a cache miss
	    ERROR=1
	fi
    else
	log "ERROR: Couldn't lock file for cache update and copy"
	ERROR=1
    fi    
}
