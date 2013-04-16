#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2013-04-15 07:35:51
# File Name: ./unittest.sh
# Description:
#########################################################################

rm -rf /mnt/test/ldb1
rm -rf /mnt/test/ldb2
rm -rf /mnt/test/split
mkdir /mnt/test/split
./metadb_fs_test /mnt/test/ldb1 /mnt/test/ldb2 /mnt/test/split
