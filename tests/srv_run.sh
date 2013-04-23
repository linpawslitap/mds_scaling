#!/bin/bash

MNT="/l0"
GIGA="${MNT}/giga_srv/"
LDB="${MNT}/giga_ldb/"

if [ $# -lt 1 ]
then
    echo "Usage : $0 <v | g | n | c>"
    echo "where ... {v=valgrind, g=gdb, f=fore, n=normal, c=clean}"
    exit
fi

killall giga_server

case $1 in

c) # cleanup and exit
   #
#rm -rf $GIGA
#rm -rf $LDB
#mkdir $GIGA
#mkdir $LDB
ps -ef | grep "giga_server" | grep -v grep | cut -c 9-15 |sudo xargs kill -9
rm -rf /panfs/test_vol$2/giga/
mkdir /panfs/test_vol$2/giga/
mkdir /panfs/test_vol$2/giga/splits/
mkdir /panfs/test_vol$2/giga/files/
mkdir /tmp/giga_srv
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
../giga_server &
;;

*) echo "Invalid option: <$1>"
   exit
   ;;
esac

exit
