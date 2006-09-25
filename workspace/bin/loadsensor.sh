#!/bin/bash

CACHE_DB=/var/workspace/image_cache_db

myhost=$(uname -n)

while [ 1 ]; do
    read input
    result=$?
    if [ $result != 0 ]; then
        exit 1
    fi
    if [ "$input" = "quit" ]; then
        exit 0
    fi

    IMG_LIST=""

    if [ -e $CACHE_DB ];
    then
        for line in $(cat $CACHE_DB)
        do
            IMG=$(echo $line | cut -f1 -d";")
            IMG_LIST="$IMG_LIST($IMG)"
        done
        IMG_LIST="($IMG_LIST)"
    fi
    echo begin
    echo "$myhost:vm_image:$IMG_LIST"
    echo "$myhost:logins:9"
    echo end
done

exit 0