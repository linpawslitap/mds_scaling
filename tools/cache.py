#!/usr/bin/python
#########################################################################
# Author: Kai Ren
# Created Time: 2013-10-27 20:09:01
# File Name: ./latency.py
# Description:
#########################################################################

class CacheEntry:
    def __init__(self, path):
        self.next_entry = None
        self.prev_entry = None
        self.path = path

class Cache:

    def __init__(self, size):
        self.entry = {}
        self.head = None
        self.tail = None
        self.limit = size

    def __pop(self):
        if self.head is None:
            return
        item = self.head
        self.entry.pop(item.path)
        self.head = item.next_entry
        if self.head is not None:
            self.head.prev_entry = None
        else:
            self.tail = None

    def __append(self, item):
        if self.tail is None:
            self.head = item
            self.tail = item
        else:
            self.tail.next_entry = item
            item.prev_entry = self.tail
            item.next_entry = None
            self.tail = item

    def __delete(self, item):
        if self.head == item:
            self.head = item.next_entry
            if self.head is not None:
                self.head.prev_entry = None
        elif self.tail == item:
            self.tail = item.prev_entry
            if self.tail is not None:
                self.tail.next_entry = None
            item.prev_entry = None
            item.next_entry = None
            return
        else:
            item.prev_entry.next_entry = item.next_entry
            item.next_entry.prev_entry = item.prev_entry
        item.prev_entry = None
        item.next_entry = None
        return

    def put(self, path):
        if path in self.entry:
            return
        if len(self.entry) == self.limit:
            self.__pop()
        item = CacheEntry(path)
        self.__append(item)
        self.entry[path] = item

    def get(self, path):
        if path not in self.entry:
            return None
        item = self.entry[path]
        if self.tail == item:
            return item
        self.__delete(item)
        self.__append(item)
        return item

    def evict(self, path):
        if path not in self.entry:
            return
        item = self.entry[path]
        self.__delete(item)
        self.entry.pop(path)

def execute(cache, cmd, path):
    global lookup_cnt, hit_cnt, write_cnt

    path_comp = path.split('/')
    father = '0'
    lookup_cnt += len(path_comp) - 1
    for node in path_comp[1:-1]:
        key = father+'/'+node
        if cache.get(key) is None:
            cache.put(key)
        else:
            hit_cnt += 1
        father = node
    node = path_comp[-1]
    key = father+'/'+node
    if cmd == 'rename' or cmd == 'setPermission' or cmd == 'delete':
        cache.evict(key)
        write_cnt += 1
    else:
        lookup_cnt += 1
        if cache.get(key) is None:
            cache.put(key)
        else:
            hit_cnt += 1

lookup_cnt = 0
hit_cnt = 0
write_cnt = 0

cache = Cache(10000)
f = open('/media/ssd/linkedin/trace0.log')
lcnt = 0
for l in f:
    lcnt += 1
    if lcnt % 1000000 == 0:
        print lcnt
        print float(hit_cnt)/float(lookup_cnt)
    ls = l.split()
    op = ls[2]
    src = ls[3]
    dst = ls[4]
    if op in ['rename']:
        execute(cache, op, src)
        execute(cache, 'create', dst)
    else:
        execute(cache, op, src)
f.close()

print lookup_cnt, hit_cnt, write_cnt
