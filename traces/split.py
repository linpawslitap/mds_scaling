#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-09-16 17:07:50
# File Name: ./split.py
# Description:
#########################################################################

import sys, getopt, os

option = {
    'nclient': 20,
    'treefile': 'tree3/tree3.log',
    'prefix': 'tree3/tree3.log'
}
curdir = os.path.dirname( os.path.abspath( __file__ ))
os.chdir(curdir)

option['prefix'] = curdir + '/' + option['prefix']

opts, args = getopt.getopt(sys.argv[1:], "c:p:", ["client=", "parent="])
for opt, arg in opts:
    if opt in ("-c", "--client"):
        option['nclient'] = int(arg)
    elif opt in  ("-p", "--parent"):
        option['prefix'] = arg


print(option['nclient'])

print option['prefix']
f = open(option['treefile'], 'r')
nfiles = int(f.readline()[:-1])
nfiles_perc = nfiles / option['nclient']
for i in range(option['nclient']):
    of = open("%s.%d"%(option['prefix'],i), 'w')
    if (i == 0):
        nfs = nfiles_perc+nfiles%option['nclient']
    else:
        nfs = nfiles_perc
    of.write("%d\n"%(nfs))
    for j in range(nfs):
        of.write(f.readline())
    of.close()
f.close()
