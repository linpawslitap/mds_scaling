#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-09-15 13:26:54
# File Name: ./cala_distribution.py
# Description:
#########################################################################

fn = './tree.txt'
f = open(fn, 'r')
f.readline()
count = {}
filenames = {}
for l in f:
    if l[0] == '/':
        ls = l.split()
        server = ls[-1]
        if server not in count:
            count[server] = 1
        else:
            count[server] += 1
        name = int(hash(ls[0])) % 253
        if name in filenames:
            filenames[name]+=1
        else:
            filenames[name]=1
f.close()
#for server in sorted(count.keys()):
#    print server, count[server]
print sorted(count.values())
print len(count.values())
print sum(count.values())
print sorted(filenames.values())
print len(filenames.values())
