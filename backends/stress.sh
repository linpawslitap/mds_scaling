#!/bin/bash
#########################################################################
# Author: Kai Ren
# Created Time: 2012-08-16 12:14:00
# File Name: ./test.sh
# Description:
#########################################################################

part=sdc1

function start_blktrace {
  ssh kmonitor "sudo blktrace -l -D /l/data/blktrace&" &
  sleep 5
  sudo blktrace -d /dev/$part -h kmonitor &
}

function end_blktrace {
  ps -ef | grep "blktrace" | grep -v grep | cut -c 9-15 |sudo xargs kill -15
  ssh kmonitor "ps -ef | grep blktrace | grep -v grep | cut -c 9-15 | sudo xargs kill -9"

}

mkdir /mnt/test
for ratio in 50 90 10
do
  sudo umount /dev/$part
  sudo mkfs.ext4 /dev/$part
  sudo mount /dev/$part /mnt/test/
  sudo chmod 777 /mnt/test

  free=$((1024*1024*1024))
  total=`free -b | grep Mem: | awk '{ printf $2 }'`
  consume=$((($total-$free)/1024/1024))

  start_blktrace

  date
  sudo nice -n -10 ionice -c 1 -n 1 ./metadb_stress_test /mnt/test/giga_ldb /home/kair/Code/tablefs/traces/fourm.txt $consume create 2000000 2>&1 | tee /tmp/create$ratio.log
  date

  sudo umount /dev/$part
  sudo mount /dev/$part /mnt/test

  date
  sudo nice -n -10 ionice -c 1 -n 1 ./metadb_stress_test /mnt/test/giga_ldb /home/kair/Code/tablefs/traces/fourm.txt $consume query 1000000 $ratio 2000000 2>&1 | tee /tmp/query$ratio.log
  date

  end_blktrace

  sleep 5
done
