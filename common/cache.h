#ifndef CACHE_H
#define CACHE_H

#include "giga_index.h"
#include "uthash.h"

typedef int DIR_handle_t;

struct giga_directory {
    DIR_handle_t handle;
    struct giga_mapping_t mapping;
    int refcount;
    UT_hash_handle hh;

};


/* initialize the directory cache */
int cache_init();

/* get the skye_directory object for a given PVFS_object_ref. */
struct giga_directory* cache_fetch(DIR_handle_t *handle);

/* return a previously fetched skye_directory.  This is necessairy because we
 * refcount skye_directory objects */
void cache_return(struct giga_directory *dir);

#endif
