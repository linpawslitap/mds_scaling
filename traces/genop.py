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

def gen_ops(bucket, nclient, prefix):
    for i in range(nclient):
        f = open(prefix+"."+str(i)+".dat", "w")
        f.write("%d\n"%(len(bucket)))
        for j in range(len(bucket)):
           f.write("%d %d\n"%(bucket[j]*i, bucket[j]*(i+1)))
        f.close()

option = {
    'nclient': 64,
    'distribution':'zipfan',
    'nop': 1000000,
    'treefile': 'tree7/tree7.log',
    'prefix': 'tree7/tree.client'
}

f = open(option['treefile'], 'r')
nbucket = int(f.readline()[:-1])
f.close()
bucket = gen_bucket(nbucket, option['nop'], option['distribution'])
gen_ops(bucket, option['nclient'], option['prefix'])
