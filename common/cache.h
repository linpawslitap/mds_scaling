#ifndef CACHE_H
#define CACHE_H

#include <pthread.h>

#include "giga_index.h"
#include "uthash.h"

typedef int DIR_handle_t;

#define MAX_NUM MAX_GIGA_PARTITIONS

struct giga_directory {
    DIR_handle_t handle;                // directory ID for directory "d"
    struct giga_mapping_t mapping;      // giga mapping for "d"
    int partition_size;

    pthread_mutex_t split_mtx;
    int             split_flag;         // flag to store the partion id of the
                                        // partition undergoing split.

    pthread_mutex_t partition_mtx;

    int refcount;
    struct giga_directory* prev;
    struct giga_directory* next;

    UT_hash_handle hh;
};

struct fuse_cache_entry {
    char* pathname;
    DIR_handle_t inode_id;
    int zeroth_server;
    time_t inserted_time;
    UT_hash_handle hh;
};

int cache_init();

struct giga_directory* cache_fetch(DIR_handle_t *handle);

int cache_update(DIR_handle_t *handle, struct giga_mapping_t *new_copy);

void cache_insert(DIR_handle_t *handle,
                  struct giga_directory* giga_dir);

struct giga_directory* cache_lookup(DIR_handle_t *handle);

void cache_release(struct giga_directory* handle);

void cache_evict(DIR_handle_t *handle);

void cache_destory();

struct giga_directory* new_cache_entry(DIR_handle_t *handle, int srv_id);

struct giga_directory* new_cache_entry_with_mapping(DIR_handle_t *handle,
                                          struct giga_mapping_t *mapping);

void fuse_cache_insert(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id, int zeroth_server);

DIR_handle_t fuse_cache_lookup(DIR_handle_t dir_id, const char* path,
                               time_t *time, int *zeroth_server);

void fuse_cache_update(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id, int zeroth_server);

#endif
