
#include "cache.h"
#include "connection.h"
#include "debugging.h"
#include "giga_index.h"
#include "options.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>

#define CACHE_LOG LOG_DEBUG

#define NUM_SHARDS_BITS 2
#define NUM_SHARDS (1 << NUM_SHARDS_BITS)

/* FIXME: this file is not thread safe */
static struct giga_directory *dircache = NULL;

static struct fuse_cache_entry* fuse_cache = NULL;

typedef struct LRUCache {
    size_t capacity_;
    size_t length_;
    pthread_mutex_t mutex_;
    struct giga_directory* table;

    //Dummy head for LRU list, prev is the newest entry, next is the oldest
    struct giga_directory dummy;
} lru_cache_t;

typedef struct ShardCache {
    struct LRUCache shards[NUM_SHARDS];
} shard_cache_t;

static shard_cache_t my_dircache;


void double_list_remove(struct giga_directory* entry) {
    entry->next->prev = entry->prev;
    entry->prev->next = entry->next;
}

void double_list_append(struct giga_directory* dummy,
                        struct giga_directory* entry) {
    entry->next = dummy;
    entry->prev = dummy->prev;
    entry->prev->next = entry;
    entry->next->prev = entry;
}

void lru_cache_unref(struct giga_directory* entry) {
    assert(entry->refcount > 0);
    --entry->refcount;
    if (entry->refcount <= 0) {
        logMessage(LOG_DEBUG, __func__, "entry(%d)", entry->handle);
        free(entry);
    }
}

void lru_cache_init(lru_cache_t* lru, size_t capacity) {
    lru->capacity_ = capacity;
    lru->length_ = 0;
    pthread_mutex_init(&(lru->mutex_), NULL);
    lru->table = NULL;
    lru->dummy.prev = &(lru->dummy);
    lru->dummy.next = &(lru->dummy);
}

void lru_cache_destroy(lru_cache_t* lru) {
    struct giga_directory* e;
    for (e = lru->dummy.next; e != &(lru->dummy); ) {
        struct giga_directory* next = e->next;
        assert(e->refcount == 1);
        lru_cache_unref(e);
        e = next;
    }

    //TODO: clean mutex?
}

void lru_cache_insert(lru_cache_t* lru,
                      DIR_handle_t key,
                      struct giga_directory* entry) {

    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_insert(%d)", key);

    //TODO: DEBUG ONLY
    entry->mapping.id = key;

    struct giga_directory* old;
    HASH_FIND_INT(lru->table, &key, old);
    if (old != NULL) {
        assert(old->handle == key);
        HASH_DEL(lru->table, old);
        double_list_remove(old);
        lru_cache_unref(old);
    } else {
        ++lru->length_;
    }

    assert(entry->handle == key);
    HASH_ADD_INT(lru->table, handle, entry);
    double_list_append(&(lru->dummy), entry);
    entry->refcount++;

    while (lru->length_ > lru->capacity_ && lru->dummy.next != &(lru->dummy)) {
        struct giga_directory* old = lru->dummy.next;
        assert(old->handle != key);
        HASH_DEL(lru->table, old);
        double_list_remove(old);
        lru_cache_unref(old);
        --lru->length_;
    }
    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_insert(%d)", key);
}

struct giga_directory* lru_cache_lookup(lru_cache_t* lru,
                                        DIR_handle_t handle) {
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_lookup(%d)", handle);

    struct giga_directory* entry;
    HASH_FIND_INT(lru->table, &handle, entry);

    if (entry != NULL) {
        entry->refcount ++;
        double_list_remove(entry);
        double_list_append(&(lru->dummy), entry);
    }

    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_lookup(%d)", handle);

    return entry;
}

void lru_cache_release(lru_cache_t* lru,
                       struct giga_directory* entry) {

    int handle = entry->handle;
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_release(%d)", handle);
    lru_cache_unref(entry);
    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_release(%d)", handle);
}

void lru_cache_erase(lru_cache_t* lru,
                     DIR_handle_t handle) {
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_erase(%d)", handle);
    struct giga_directory* old;
    HASH_FIND_INT(lru->table, &handle, old);

    if (old != NULL) {
        double_list_remove(old);
        HASH_DEL(lru->table, old);
        lru_cache_unref(old);
        --lru->length_;
    }
    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_erase(%d)", handle);
}

static int shard_cache_get_shard(DIR_handle_t handle) {
    return handle & (NUM_SHARDS - 1);
}

static void shard_cache_init(shard_cache_t* dircache, size_t capacity) {
    size_t per_shard_size = (capacity + NUM_SHARDS - 1) / (NUM_SHARDS);
    int s;
    for (s = 0; s < NUM_SHARDS; s++) {
        lru_cache_init(&(dircache->shards[s]), per_shard_size);
    }
}

static void shard_cache_destroy(shard_cache_t* dircache) {
    int s;
    for (s = 0; s < NUM_SHARDS; s++) {
        lru_cache_destroy(&(dircache->shards[s]));
    }
}

static void shard_cache_insert(shard_cache_t* dircache,
                               DIR_handle_t handle,
                               struct giga_directory* entry) {
    lru_cache_insert(&(dircache->shards[shard_cache_get_shard(handle)]),
                     handle, entry);
}

static struct giga_directory* shard_cache_lookup(shard_cache_t* dircache,
                                                 DIR_handle_t handle) {
    return lru_cache_lookup(&(dircache->shards[shard_cache_get_shard(handle)]),
                            handle);
}

void shard_cache_release(shard_cache_t* dircache,
                         struct giga_directory* entry) {
    lru_cache_release(&(dircache->shards[shard_cache_get_shard(entry->handle)]),
                      entry);
}

void shard_cache_erase(shard_cache_t* dircache,
                       DIR_handle_t handle) {
    lru_cache_erase(&(dircache->shards[shard_cache_get_shard(handle)]),
                    handle);
}

struct giga_directory* cache_lookup(DIR_handle_t *handle)
{
    logMessage(LOG_DEBUG, __func__, "%d", *handle);

    struct giga_directory* dir =
        shard_cache_lookup(&my_dircache, *handle);
    return dir;
}

void cache_insert(DIR_handle_t *handle,
                  struct giga_directory* giga_dir) {
    logMessage(LOG_DEBUG, __func__, "%d", *handle);

    shard_cache_insert(&my_dircache, *handle, giga_dir);
}

void cache_release(struct giga_directory* giga_dir) {
    logMessage(LOG_DEBUG, __func__, "%d", giga_dir->handle);

    giga_print_mapping(&giga_dir->mapping);
    shard_cache_release(&my_dircache, giga_dir);
}

void cache_evict(DIR_handle_t *handle) {
    shard_cache_erase(&my_dircache, *handle);
}

void cache_destory() {
    shard_cache_destroy(&my_dircache);
}

struct giga_directory* new_cache_entry_with_mapping(DIR_handle_t *handle,
                                          struct giga_mapping_t* mapping)
{
    int i = 0;

    struct giga_directory *d =
        (struct giga_directory*) malloc(sizeof(struct giga_directory));
    if (d == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }

    d->handle = *handle;
    if (mapping != NULL) {
      d->mapping = *mapping;
    }

    d->refcount = 1;
    d->split_flag = 0;
    pthread_mutex_init(&d->split_mtx, NULL);

    logMessage(CACHE_LOG, __func__, "init %d  partitions ...", MAX_NUM);
    for (i=0; i < MAX_NUM; i++) {
        d->partition_size[i] = 0;
        pthread_mutex_init(&d->partition_mtx[i], NULL);
    }

    logMessage(CACHE_LOG, __func__, "Cache_CREATE: dir(%d)", d->handle);

    return d;
}

struct giga_directory* new_cache_entry(DIR_handle_t *handle, int srv_id)
{
    struct giga_directory* d =
      new_cache_entry_with_mapping(handle, NULL);
    giga_init_mapping(&d->mapping, -1, d->handle, srv_id,
                      giga_options_t.num_servers);
    return d;
}

int cache_init()
{
    shard_cache_init(&my_dircache, DEFAULT_DIR_CACHE_SIZE);

    dircache = NULL;

    fuse_cache = NULL;

    return 0;
}

/*
struct giga_directory* cache_lookup(DIR_handle_t *handle)
{
  struct giga_directory* ret;
  HASH_FIND_INT(dircache, handle, ret);
  return ret;
}

void cache_insert(DIR_handle_t *handle,
                  struct giga_directory* giga_dir) {
    giga_dir->handle = *handle;
    HASH_ADD_INT(dircache, handle, giga_dir);
}

void cache_release(struct giga_directory* giga_dir) {
  (void) giga_dir;
}

void cache_evict(DIR_handle_t *handle) {
  (void) handle;
}

void cache_destory() {
}
*/

inline char* build_fuse_cache_key(DIR_handle_t dir_id, const char* path) {
  char* key = (char*) malloc(16+strlen(path)+1);
  sprintf(key, "%016x", dir_id);
  strcpy(key+16, path);
  key[16+strlen(path)]='\0';
  return key;
}

void fuse_cache_insert(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id, int zeroth_server)
{
    char* key = build_fuse_cache_key(dir_id, path);
    struct fuse_cache_entry* entry;
    HASH_FIND_STR(fuse_cache, key, entry);
    if (entry == NULL) {
        entry = (struct fuse_cache_entry*)
                  malloc(sizeof(struct fuse_cache_entry));
        entry->pathname = key;
        entry->inode_id = inode_id;
        entry->inserted_time = time(NULL);
        entry->zeroth_server = zeroth_server;

        HASH_ADD_KEYPTR(hh, fuse_cache, entry->pathname,
                        strlen(entry->pathname), entry);
        //HASH_ADD_STR(fuse_cache, pathname, entry);
        logMessage(LOG_DEBUG, __func__, "insert::[%d][%s]-->[%d][%d]",
                   dir_id, path, (int)inode_id, zeroth_server);
    } else {
        free(key);
        entry->inode_id = inode_id;
        entry->inserted_time = time(NULL);
        entry->zeroth_server = zeroth_server;
    }
}

DIR_handle_t fuse_cache_lookup(DIR_handle_t dir_id, const char* path,
                               time_t *timestamp, int *zeroth_server)
{
    struct fuse_cache_entry* ret;
    char* key = build_fuse_cache_key(dir_id, path);

    HASH_FIND_STR(fuse_cache, key, ret);
    free(key);
    if (ret == NULL) {
        logMessage(LOG_DEBUG, __func__, "lookup::[%d][%s]-->[-1]",
                   dir_id, path);
        return -1;
    } else {
        logMessage(LOG_DEBUG, __func__, "lookup::[%d][%s]-->[%d][%d]",
                   dir_id, path, (int)ret->inode_id, ret->zeroth_server);
        *timestamp = ret->inserted_time;
        *zeroth_server = ret->zeroth_server;
        return ret->inode_id;
    }
}

void fuse_cache_update(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id, int zeroth_server) {
    struct fuse_cache_entry* ret;
    char* key = build_fuse_cache_key(dir_id, path);
    HASH_FIND_STR(fuse_cache, key, ret);
    free(key);
    if (ret != NULL) {
        logMessage(LOG_DEBUG, __func__, "lookup::[%d][%s]-->[%d][%d]",
                   dir_id, path, (int)ret->inode_id, zeroth_server);
        ret->inode_id = inode_id;
        ret->inserted_time = time(NULL);
        ret->zeroth_server = zeroth_server;
    }
}

