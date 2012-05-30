#!/bin/bash

fusermount -u /tmp/fuse
ps -ef | grep "giga" | grep -v grep | cut -c 9-15 |xargs kill -9

