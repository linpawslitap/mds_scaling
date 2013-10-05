#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-09-16 11:10:00
# File Name: ./genop.py
# Description:
#########################################################################

import sys
import numpy as np

def gen_ops(nbucket, nclient, prefix):
    global option
    for i in range(nclient):
        f = open(prefix+"."+str(i)+".dat", "w")
        count = nbucket / nclient
        #print count
        f.write("%d 2\n"%(count))
        pid = i
        for j in range(count):
           f.write("%d %d %d\n"%(pid, option['nop']*j, option['nop']*(j+1)))
           pid += nclient
        f.close()

option = {
    'nclient': 128,
    'distribution':'zipfan',
    'nop': 10,
    'treefile': 'tree9/tree9.log',
    'prefix': 'tree9/tree.client'
}

f = open(option['treefile'], 'r')
nbucket = int(f.readline()[:-1])
f.close()
gen_ops(nbucket, option['nclient'], option['prefix'])
