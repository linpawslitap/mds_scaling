#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2012-05-30 10:51:34
# File Name: ./run.sh
# Description:
#########################################################################

sudo rm -rf /l0/giga_ldb
sudo rm -rf /l0/giga_srv
sudo rm -rf /tmp/_splits
mkdir /l0/giga_ldb
mkdir /l0/giga_srv
mkdir /tmp/_splits
echo 'localhost' > /tmp/.giga
../giga_server
