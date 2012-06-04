#!/bin/bash

fusermount -u /tmp/fuse
ps -ef | grep "giga" | grep -v grep | cut -c 9-15 |xargs kill -9


rm -rf /tmp/ldb
rm -rf /tmp/fuse
mkdir /tmp/ldb
mkdir /tmp/fuse

gdb ../giga_server
#../giga_client /tmp/fuse

