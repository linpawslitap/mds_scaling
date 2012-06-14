#!/bin/bash

MNT="/tmp/giga_c"
NUM_FUSE_CLI="/tmp/.giga_clients"

if [ $# -lt 1 ]
then
    echo "Usage : $0 [num_files]"
    exit
fi

## deprecated: replaced by a scan of file to see how many lines in that file.
##
## cli=`cat ${NUM_FUSE_CLI}`   # read from a file that "cli_run.sh" wrote to

cli=`cat ${NUM_FUSE_CLI} | wc -l`
host=`hostname | cut -d'.' -f 1`

if [ $cli -gt 1 ]
then
    for (( i=1; i<=$cli; i++)) 
    do
        echo "test instance $i ..."
        dir=${MNT}/${i}
        out="~/_perf/$host.$i"
        ( time ./mknod_test ${dir} $1 ) > ~/_perf/$host.$i 2>&1 &
    done
else
    out="~/_perf/$host.0"
    ( time ./mknod_test ${MNT} $1 ) > ~/_perf/$host.$i 2>&1 &
fi



