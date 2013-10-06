#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-09-16 11:10:00
# File Name: ./genop.py
# Description:
#########################################################################

import sys
import numpy as np

def gen_bucket_uniform(nbucket, nop):
    buckets = [nop / nbucket + 1] * nbucket
    return buckets

def gen_bucket_zipfan(nbucket, nop):
    buckets = np.random.zipf(2., nbucket)
    norm = nop / float(sum(buckets))
    for i in range(len(buckets)):
        buckets[i] = buckets[i] * norm + 1
    return buckets

def gen_bucket(nbucket, nop, distribution):
    if distribution == 'uniform':
        return gen_bucket_uniform(nbucket, nop)
    if distribution == 'zipfan':
        return gen_bucket_zipfan(nbucket, nop)

def gen_ops(bucket, prefix):
    f = open(prefix, "w")
    f.write("%d 1\n"%(len(bucket)))
    for j in range(len(bucket)):
       f.write("%d %d\n"%(0, bucket[j]))
    f.close()

tid=11
option = {
    'nclient': 128,
    'distribution':'uniform',
    'nop': 2000000,
    'treefile': 'tree%d/tree%d.log'%(tid,tid),
    'prefix': 'tree%d/tree.client.zipf.log'%(tid)
}

f = open(option['treefile'], 'r')
nbucket = int(f.readline()[:-1])
f.close()
bucket = gen_bucket(nbucket, option['nop'], option['distribution'])
gen_ops(bucket, option['prefix'])
