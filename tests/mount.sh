#!/bin/bash

sh umount.sh

rm -rf /tmp/ldb
rm -rf /tmp/fuse
mkdir /tmp/ldb
mkdir /tmp/fuse

gdb ../giga_server
#../giga_client /tmp/fuse

