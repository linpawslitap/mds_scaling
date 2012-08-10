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
    int partition_size[MAX_NUM];        // num dirents in each partition

    pthread_mutex_t split_mtx;
    int             split_flag;         // flag to store the partion id of the
                                        // partition undergoing split.

    // XXX: think again??
    //
    pthread_mutex_t partition_mtx[MAX_NUM];        // num dirents in each partition

    int refcount;
    struct giga_directory* prev;
    struct giga_directory* next;
    UT_hash_handle hh;
};


// initialize the directory cache
int cache_init();

// get the skye_directory object for a given PVFS_object_ref.
struct giga_directory* cache_fetch(DIR_handle_t *handle);

int cache_update(DIR_handle_t *handle, struct giga_mapping_t *new_copy);

// return a previously fetched skye_directory.  This is necessairy because we
// refcount skye_directory objects
//void cache_return(struct giga_directory *dir);

void cache_insert(DIR_handle_t *handle,
                  struct giga_directory* giga_dir);

struct giga_directory* cache_lookup(DIR_handle_t *handle);

void cache_release(struct giga_directory* handle);

void cache_evict(DIR_handle_t *handle);

void cache_destory();

#endif
