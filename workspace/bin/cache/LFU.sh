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
            REPL_IMAGE=`awk -F";" 'NR==1{lfu=$1;lfu_num=$3} NR>1 {if($3<lfu_num) {lfu=$1;lfu_num=$3}} END {print lfu}' $CACHE_DB`
        else
            REPL_IMAGE=`grep -v "$EXCLUDE" $CACHE_DB | awk -F";" 'NR==1{lfu=$1;lfu_num=$3} NR>1 {if($3<lfu_num) {lfu=$1;lfu_num=$3}} END {print lfu}'`
        fi
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
        fi
    done

    eval "$1=$REPL_IMAGE"
}