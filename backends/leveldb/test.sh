#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-09-06 20:05:39
# File Name: ./test.sh
# Description:
#########################################################################

buffer=16777216
num=30000000
dir=/mnt/data/hadoop
for valsize in 300
do
for dbtype in 2 1
do
  rdate=$(date '+%Y%m%d-%H-%M')
  ./db_bench --benchmarks=fillrandom,readrandom --histogram=1 --num=$num \
    --value_size=$valsize --dbtype=$dbtype --bloom_bits=14\
    --write_buffer_size=$buffer --db=$dir | tee run$rdate.$buffer.$valsize.$dbtype.log
  rm -rf $dir/*
done
done
