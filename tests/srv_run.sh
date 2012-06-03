#!/bin/bash

rm -rf /tmp/giga_s/*
rm -rf /tmp/ldb/*

if [ $# -lt 1 ]
then
    echo "Usage : $0 [v | g | n]"
    exit
fi

case $1 in

v) valgrind --tool=memcheck --leak-check=yes ../giga_server
   ;;

g) gdb --args ../giga_server
   ;;

n) ../giga_server
   ;;

esac

