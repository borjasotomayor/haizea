function getReplaceableImage()
{
    local CACHE_DB=$2
    found=false;
    REPL_IMAGE=""
    EXCLUDE=""
    while ! $found;
    do
        if [ "$EXCLUDE" == "" ];
        then
            REPL_IMAGE=`awk -F";" 'NR==1{lru=$1;lru_time=$2} NR>1 {if($2<lru_time) {lru=$1;lru_time=$2}} END {print lru}' $CACHE_DB`
        else
            REPL_IMAGE=`grep -v "$EXCLUDE" $CACHE_DB | awk -F";" 'NR==1{lru=$1;lru_time=$2} NR>1 {if($2<lru_time) {lru=$1;lru_time=$2}} END {print lru}'`
        fi
	debug "Apparently, I should replace $REPL_IMAGE"
        if grep $REPL_IMAGE $LOCAL_CACHE_DB > /dev/null ;
        then
            found=true
        else
            if [ "$EXCLUDE" == "" ];
            then
                EXCLUDE="$REPL_IMAGE"
            else
                EXCLUDE="$EXCLUDE\|$REPL_IMAGE"
            fi
	    debug "But $REPL_IMAGE is no good. EXCLUDE=$EXCLUDE"
        fi
    done

    eval "$1=$REPL_IMAGE"
}