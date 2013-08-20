#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-08-19 00:40:18
# File Name: ./hdfs.sh
# Description:
#########################################################################

export JAVA_HOME=/usr/lib/jvm/java-1.7.0-sun-amd64/
export JVM_ARCH=64
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/lib/jvm/java-1.7.0-sun-amd64/jre/lib/amd64/server/
HADOOP_CONF=/home/kair/Software/hadoop-0.23.7/etc/hadoop
HADOOP_HOME=/home/kair/Software/hadoop-0.23.7
export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | tr '\n' ':')
gdb --args ./ldb_hdfs_test
