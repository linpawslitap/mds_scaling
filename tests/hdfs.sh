#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-08-19 00:40:18
# File Name: ./hdfs.sh
# Description:
#########################################################################

HADOOP_CONF=/home/kair/Software/hadoop-0.23.7/etc/hadoop
HADOOP_HOME=/home/kair/Software/hadoop-0.23.7
export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | tr '\n' ':')
./ldb_hdfs_test
