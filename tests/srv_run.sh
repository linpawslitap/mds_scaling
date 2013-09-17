#!/bin/bash

MNT="/l0"

GIGA="${MNT}/giga_srv/"
LDB="${MNT}/giga_ldb/"
PID="/tmp/giga_server.pid"

if [ $# -lt 1 ]
then
    echo "Usage : $0 <v | g | n | c>"
    echo "where ... {v=valgrind, g=gdb, f=fore, n=normal, c=clean}"
    exit
fi

#killall giga_server
#ulimit -c unlimited
. ~/.bashrc

case $1 in

c) # cleanup and exit
   #

sudo mkdir $MNT
sudo chmod 777 $MNT
killall -9 giga_server
rm $PID
rm -rf $GIGA
rm -rf $LDB
mkdir $GIGA
mkdir $LDB
#ps -ef | grep "giga_server" | grep -v grep | cut -c 9-15 |sudo xargs kill -9
#rm -rf /panfs/test_vol$2/giga/
#mkdir /panfs/test_vol$2/giga/
#mkdir /panfs/test_vol$2/giga/splits/
#mkdir /panfs/test_vol$2/giga/files/
#mkdir /tmp/giga_srv
exit
;;

v) # valgrind execution mode
   #
valgrind --tool=memcheck --leak-check=yes ../giga_server
;;

g) # GDB execution
   #
gdb --args ../giga_server
;;

f) # foreground server execution
   #
../giga_server
;;

n) # normal server execution
   #
echo "lauch giga_server $PWD"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/lib64/openmpi/lib/:/usr/libexec/dropbox/
../giga_server > /tmp/server.log &
;;

u)
echo "umount giga_server"
;;

*) echo "Invalid option: <$1>"
   exit
   ;;
esac

exit
