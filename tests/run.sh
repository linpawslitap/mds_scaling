#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2012-05-30 10:51:34
# File Name: ./run.sh
# Description:
#########################################################################

mkdir /tmp/fuse1
mkdir /tmp/fuse2
sudo umount /tmp/fuse1
sudo umount /tmp/fuse2
../giga_client /tmp/fuse1
../giga_client /tmp/fuse2
./mknod_test /tmp/fuse1/ 1000 &
./mknod_test /tmp/fuse2/ 1000 &
