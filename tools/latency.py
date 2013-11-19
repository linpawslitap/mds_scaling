#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-10-27 20:09:01
# File Name: ./latency.py
# Description:
#########################################################################

f = open('/media/ssd/linkedin/trace0.log')
t = 15.0
tot = 0.0
nent = 0.0
lcnt = 0
for l in f:
    lcnt += 1
    if lcnt % 10000 == 0:
        print lcnt
    ls = l.split()
    op = ls[2]
    plen = len(ls[3].split('/'))
    if op in ['rename', 'delete', 'setPermission']:
        tot += t / plen
        nent += 1
f.close()

print tot / nent
