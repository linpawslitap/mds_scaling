#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2012-05-30 10:51:34
# File Name: ./run.sh
# Description:
#########################################################################

mkdir /tmp/fuse
../giga_client /tmp/fuse
./mknod_test /tmp/fuse/ 10000
