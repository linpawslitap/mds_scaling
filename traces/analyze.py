#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2011-10-30 22:43:24
# File Name: ./analyze.py
# Description: 
#########################################################################

f = open('./test.log', 'r')
stat = {'meta':{'insert':0, 'read':0},
        'data':{'insert':0, 'read':0}}
for l in f:
    items = l.split()
    stat[items[1][-4:]][items[0]] += 1
f.close()

for i in ('meta', 'data'):
    for j in ('insert', 'read'):
        print stat[i][j],
    print

#pattern:
#one data insert, three meta insert, five meta read 
#read meta, insert meta, read meta, read meta, insert data, read meta, insert meta, read meta, insert meta 
