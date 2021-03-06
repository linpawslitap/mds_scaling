#!/bin/bash

MNT="/tmp/giga_c"
NUM_FUSE_CLI="/tmp/.giga_clients"

## defaults
##
cli="1"
apps="1"
id="1"

## deprecated: replaced by a scan of file to see how many lines in that file.
##
## cli=`cat ${NUM_FUSE_CLI}`   # read from a file that "cli_run.sh" wrote to

cli=`cat ${NUM_FUSE_CLI} | wc -l`
host=`hostname | cut -d'.' -f 1`

if [ $# -lt 1 ]
then
    echo "Usage : $0 [num_files]  __num_APPS__   _num_FUSE_ _id_"
    exit
fi

echo $1 $2 $3 $4 $5
apps=$2
cli=$3
treeid=$4
id=$5
echo $id

files=$(( $1/$(( $apps*$cli)) ))

#if [ $cli -gt 1 ]
#then
echo "creating $apps benchmarks on $cli FUSE instances"
count=0
for (( i=1; i<=$cli; i++))
do
    for ((j=1; j<=$apps; j++))
    do
        echo "test app $j of FUSE instance $i creating $files files"
        dir=${MNT}
        if (( $cli>1 ))
        then
            dir=${MNT}/${i}
        fi
        count=$(($count+1))
        ./tree_select_test mknod ../traces/tree$treeid/tree$treeid.log \
        ../traces/tree$treeid/tree.client.$id.dat $files $id $count \
        > ~/_perf/$host.$i.$j 2>&1 & 

        #( time ./mknod_test ${dir} $files ) > ~/_perf/$host.$i.$j 2>&1 &
        #./smallfile_lib_test / $filesize $files > ~/_perf/$host.$i.$j 2>&1
    done
done
