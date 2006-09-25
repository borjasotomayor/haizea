# Image transfer functions for different protocols
# Borja Sotomayor 08-01-06

function downloadNFS
{
    local IMAGE_PATH=$1
    local DST=$2

    if [ ! -e $IMAGE_PATH ];
    then
	log "ERROR: Can't download $IMAGE_PATH. Reason: Does not exist (is NFS drive mounted?)"
	return 1
    fi
    log "--DOWNLOAD_START-- Starting NFS download of $IMAGE_PATH"
    START=`date +%s`
    cp $IMAGE_PATH $DST.downloading
    mv $DST.downloading $DST
    END=`date +%s`
    TIME=$(($END - $START))
    if [ $TIME -eq 0 ];
    then
	RATE="infinity"
    else
        RATE=$(($IMG_SIZE / $TIME))
    fi
    log "--DOWNLOAD_END-- NFS download finished. Time: $TIME seconds. Speed: $RATE b/s"
    return 0
}

function downloadFTP
{
    log "FTP is not currently supported"
    return 1
}

function downloadGridFTP
{
    log "GridFTP is not currently supported"
    return 1
}