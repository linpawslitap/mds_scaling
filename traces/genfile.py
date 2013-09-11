#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2011-10-30 22:23:36
# File Name: ./genfile.py
# Description: 
#########################################################################

import random
import os
import sys
import time

def gen_content(size):
    random.seed(1023)
    content = '' 
    for i in range(size):
        content += chr(random.randint(48, 123))
    return content

def gen_file(work_dir, n, content):
    ind = 0
    for i in range(n / 1024 + 1):
        os.mkdir("%s/%d" % (work_dir, i))
    for i in range(n):
        fn = "%s/%d/%08x.txt" % (work_dir, ind, i)
        if i % 1024 == 0:
            ind += 1
        f = open(fn, 'w')
        f.write(content)
        f.close()

def read_file(work_dir, n):
    ind = 0
    for i in range(n):
        fn = "%s/%d/%08x.txt" % (work_dir, ind, i)
        if i % 1024 == 0:
           ind += 1
        f = open(fn, 'r')
        content = f.read()
        f.close()

def run(work_dir, n, contentsize):
    print contentsize
    content = gen_content(contentsize)
    start = time.time()
    gen_file(work_dir, n, content)
    read_file(work_dir, n)
    end = time.time()
    print end - start

if __name__ == '__main__':
    run("/mnt/share/test", 1024 * 1024, int(sys.argv[1]))
