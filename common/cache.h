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
    int split_flag;                     // flag to store the partion id of the
                                        // partition undergoing split.
    int refcount;                       // ???

    // XXX: think again??
    //
    pthread_mutex_t partition_mtx[MAX_NUM];        // num dirents in each partition
    //UT_hash_handle hh; //FIXME:
};


// initialize the directory cache
int cache_init();

// get the skye_directory object for a given PVFS_object_ref.
struct giga_directory* cache_fetch(DIR_handle_t *handle);

// return a previously fetched skye_directory.  This is necessairy because we
// refcount skye_directory objects 
//void cache_return(struct giga_directory *dir);

#endif
