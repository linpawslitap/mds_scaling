#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-04-18 16:07:45
# File Name: ./sum.py
# Description:
#########################################################################

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

num_node = 32
num_client = 8 
num_apps = 1
nodes = []
#resdir = '/users/kair/_perf'
testname = 'perf-32-8'
resdir = '/users/kair/code/mds_scaling/results/%s'%(testname)
for i in range(0, num_node):
    clients = []
    for j in range(1, num_client+1):
        for jj in range(1, num_apps+1):
            f = open('%s/h%d.%d.%d'%(resdir, i, j, jj), 'r')
            ts = []
            for l in f:
                try:
                    thpt = int(l)
                    ts.append(thpt)
                except:
                    pass
            f.close()
            clients.append(ts)
    nodes.append(clients)

plt.clf()
tot = []
for i in range(num_node):
    sums = []
    for j in range(num_client*num_apps):
        for k in range(len(nodes[i][j])):
            if k >= len(sums):
                sums.append(0)
            sums[k] += nodes[i][j][k]
#    plt.plot(range(len(sums)), sums, label='Vol%d'%(i))

    for k in range(len(sums)):
        if k >= len(tot):
            tot.append(0)
        tot[k] += sums[k]

plt.plot(range(len(tot)), tot, label='Total')
leg = plt.legend(loc=0)
leg.get_frame().set_alpha(0.0)
plt.title(testname)
plt.ylabel('Throughput (creates/sec)')
plt.xlabel('Time (seconds)')
plt.savefig('%s.pdf'%(testname))
