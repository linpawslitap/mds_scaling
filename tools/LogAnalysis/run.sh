#!/bin/bash
#########################################################################
# Created Time: 2013-09-28 20:29:07
# File Name: ./run.sh
# Description:
#########################################################################


libs=dist/analyzer.jar:lib/commons-compress-1.6.jar
echo $libs
time java -Xmx8G -cp $libs SlidingWindow /media/ssd/linkedin/trace0.log
