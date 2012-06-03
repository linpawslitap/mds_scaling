
#include "cache.h"
#include "connection.h"
#include "debugging.h"
#include "giga_index.h"
#include "options.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>

#define CACHE_LOG LOG_DEBUG

/* FIXME: this file is not thread safe */
static struct giga_directory *dircache = NULL;

/*
static 
void fill_bitmap(struct giga_mapping_t *mapping, DIR_handle_t *handle)
{
    (void)mapping;
    (void)handle;

    if (handle == NULL)
        printf("dosomething");
        //TODO: get bitmap from disk

}
*/

/*
static 
struct giga_directory* new_directory(DIR_handle_t *handle)
{
    int i = 0;

    struct giga_directory *dir = malloc(sizeof(struct giga_directory));
    if (!dir) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        return NULL;
    }

    memcpy(&dir->handle, handle, sizeof(DIR_handle_t));
    
    int zeroth_srv = 0; //FIXME: how do you get zeroth server info?
    
    // FIXME: what should flag be?
    giga_init_mapping(&dir->mapping, -1, zeroth_srv, giga_options_t.num_servers);
    dir->refcount = 1;
    //for (i=0; i < (int)sizeof(dir->partition_size); i++)
    for (i=0; i < MAX_BMAP_LEN; i++)
        dir->partition_size[i] = 0;

    HASH_ADD(hh, dircache, handle, sizeof(DIR_handle_t), dir);

    //TODO: get biubitmap from disk???
    //fill_bitmap(&(dir->mapping), handle);
   
    logMessage(LOG_TRACE, __func__, "Cache_CREATE: dir(%d)", *handle);

    return dir;
}

int cache_init()
{
   return 0; 
}

struct giga_directory* cache_fetch(DIR_handle_t *handle)
{
    struct giga_directory *dir = NULL;

    HASH_FIND(hh, dircache, handle, sizeof(DIR_handle_t), dir);

    if (!dir) {
        logMessage(LOG_DEBUG, __func__, "Cache_MISS: dir(%d)", *handle); 
        if ((dir = new_directory(handle)) == NULL) {
            logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
            return NULL;
        }
    }
    else
        logMessage(LOG_DEBUG, __func__, "Cache_HIT: dir(%d)", *handle); 


    dir->refcount++;

    return dir;
}

void cache_return(struct giga_directory *dir)
{
    assert(dir->refcount > 0);
    dir->refcount--;
}

// when an object is deleted
void cache_destroy(struct giga_directory *dir)
{
    assert(dir->refcount > 1);

    // once to release from the caller
    __sync_fetch_and_sub(&dir->refcount, 1);

    HASH_DEL(dircache, dir);

    if (__sync_sub_and_fetch(&dir->refcount, 1) == 0)
        free(dir);
}
*/

int cache_init()
{
    int i = 0;

    dircache = (struct giga_directory*)malloc(sizeof(struct giga_directory));
    if (!dircache) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }

    dircache->handle = 0;
    
    int zeroth_srv = 0; //FIXME: how do you get zeroth server info?
    giga_init_mapping(&dircache->mapping, -1, zeroth_srv, giga_options_t.num_servers);
    dircache->refcount = 1;
    dircache->split_flag = -1;
    //for (i=0; i < (int)sizeof(dir->partition_size); i++)
    logMessage(CACHE_LOG, __func__, "partition size arraylen = %d", ARR_LEN);
    for (i=0; i < ARR_LEN; i++)
        dircache->partition_size[i] = 0;

    logMessage(LOG_TRACE, __func__, "Cache_CREATE: dir(%d)", dircache->handle);

    return 0; 
}

struct giga_directory* cache_fetch(DIR_handle_t *handle)
{
    if (dircache == NULL) {
        logMessage(LOG_DEBUG, __func__, "Cache_NULL: dir(%d)", *handle); 
        cache_init();
    }
    
    if (dircache->handle != *handle) {
        logMessage(LOG_DEBUG, __func__, "Cache_MISS: dir(%d)", *handle); 
        return NULL;
    }
    
    return dircache;
}

