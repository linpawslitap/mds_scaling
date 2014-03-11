#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-11-20 11:05:04
# File Name: ./draw.py
# Description:
#########################################################################

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

def draw(y, name):
    x = np.arange(0, 100) / 100.0
    sumy = sum(y)
    y = np.array(y) / sumy
    cdfy = [y[0]]
    for i in range(1, len(y)):
        cdfy.append(cdfy[-1]+y[i])
    plt.plot(x, cdfy, label=name)
    plt.legend(loc=0)
    plt.ylim((0, 1))
#    plt.show()

filename = './final.txt'
for l in open(filename, 'r'):
    if not l.startswith('Mean'):
        if l.find('op') < 0:
            continue
        pos = l.find('is')
        if l.find('is', pos+1) >= 0:
            pos = l.find('is', pos+1)
        ls = l[pos+3:].split()
        y = [float(x) for x in ls]
        draw(y, l[:pos])

plt.show()
