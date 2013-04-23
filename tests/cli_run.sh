#!/bin/bash

MNT="/tmp/giga_c"
NUM_FUSE_CLI="/tmp/.giga_clients"

if [ $# -lt 1 ]
then
    ## 'f' foreground or 'n' normal
    ##
    echo "Usage : $0 <f | g | n | c > [m (FUSE instances)]"
    echo "where ... {f=foreground, g=gdb, n=normal, c=cleanup}"
    exit
fi


## clean up all giga_client mountpoints
##
for dir in `cat /proc/mounts | grep 'giga_client' | cut -d' ' -f2`
do
    echo "Cleanup (unmount and delete) $dir ..."
    fusermount -u -z $dir
    rm -rf $dir
done

killall -9 giga_client
rm -rf /tmp/dbg.log.c.*
rm -rf $MNT
mkdir -p $MNT

## look at the command-line arguments and take appropriate action
##
case $1 in

c)  # cleanup and exit
    #
;;

f)  # debugging mode (only single client)
    #
fusermount -u -z $MNT
../giga_client -d $MNT
;;

g)  # GDB mode (only single client)
    #
fusermount -u -z $MNT
gdb --args ../giga_client -d $MNT
;;


n)  # background, default mode (check for multiple clients)
    #
echo '' > $NUM_FUSE_CLI     # track PIDs of FUSE clients
if [ $# -eq 2 ]
then
  echo "creating $2 FUSE instances ..."
    if [ $2 -gt 1 ]
    then
        for (( i=1; i<=$2; i++))
        do
            dir=${MNT}/${i}
            echo "Setup (mkdir and mount) client-$i at $dir ..."
            mkdir -p $dir
            ../giga_client $dir
        done
    else
        echo "Start client at $MNT ..."
        ../giga_client $MNT
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

