#!/bin/bash

MNT="/tmp/giga_c"
NUM_FUSE_CLI="/tmp/.giga_clients"

if [ $# -lt 1 ]
then
    ## 'f' foreground or 'n' normal
    ##
    echo "Usage : $0 <f (foreground) | n (normal)> [n (FUSE instances)]"   
    exit
fi


## clean up all mountpoints by looking under the client directory.
##
for dir in $MNT/*
do
    echo "Cleanup (unmount and remove) $dir ..."
    fusermount -u -z $dir
    rm -rf $dir
done
rm -rf /tmp/dbg.log.c.*

## look at the command-line arguments and take appropriate action
##
case $1 in

f)  # debugging mode (only single client)
    #
fusermount -u -z $MNT 
../giga_client -d $MNT
;;

n)  # background, default mode (check for multiple clients)
    #
if [ $# -eq 2 ]
then
    #echo $2 > $NUM_FUSE_CLI   # write to a file that "test_run.sh" will read
    echo '' > $NUM_FUSE_CLI     # track PIDs of FUSE clients

    if [ $2 -gt 1 ]
    then
        for (( i=1; i<=$2; i++)) 
        do
            dir=${MNT}/${i}
            echo "Setup (mkdir and mount) client-$i at $dir ..."
            mkdir -p $dir
            ../giga_client $dir
        done
    fi
else
    ../giga_client $MNT
fi
pidof giga_client | tr ' ' '\n' > $NUM_FUSE_CLI 
;;

*) echo "Invalid option: <$1>"
   exit
   ;;

esac

