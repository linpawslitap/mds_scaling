#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-06-16 21:50:03
# File Name: ./run.sh
# Description:
#########################################################################

export LD_LIBRARY_PATH=./:../
java -classpath /usr/share/java/jna.jar:.:../hadoop/hadoop-common-project/hadoop-common/target/hadoop-common-0.23.7.jar GigaClient
#java -classpath /usr/share/java/jna.jar:. BytesTest
