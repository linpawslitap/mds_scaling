#!/bin/bash

GIGA="/tmp/giga_s/"
LDB="/tmp/ldb/"

if [ $# -lt 1 ]
then
    echo "Usage : $0 [v (valgrind) | g (gdb) | n (normal)]"
    exit
fi

rm -rf $GIGA 
rm -rf $LDB
mkdir $GIGA
mkdir $LDB

case $1 in

v) valgrind --tool=memcheck --leak-check=yes ../giga_server
   ;;

g) gdb --args ../giga_server
   ;;

n) ../giga_server
   ;;

*) echo "Invalid option: <$1>"
   exit
   ;;
esac

