#!/bin/bash

MNT="/tmp/giga_c"

if [ $# -lt 1 ]
then
    echo "Usage : $0 [f | n]"   ## 'f' foreground or 'n' normal
    exit
fi

fusermount -u -z $MNT 

case $1 in

f) ../giga_client -d $MNT
   ;;

n) ../giga_client $MNT
   ;;

esac

