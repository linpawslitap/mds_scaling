#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-06-16 21:50:03
# File Name: ./run.sh
# Description:
#########################################################################

export LD_LIBRARY_PATH=../../../
HADOOP_CONF=/home/kair/Software/hadoop-0.23.7/etc/hadoop
HADOOP_HOME=/home/kair/Software/hadoop-0.23.7
export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | tr '\n' ':'):/usr/share/java/jna.jar:./target/hadoop-common-0.23.7.jar:/home/kair/Software/hadoop-0.23.7/share/hadoop/common/lib/*

java -classpath ${CLASSPATH} org.apache.hadoop.fs.gtfs.GTFileSystem
