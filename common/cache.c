
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

#define NUM_SHARDS_BITS 4
#define NUM_SHARDS (1 << NUM_SHARDS_BITS)

/* FIXME: this file is not thread safe */
static struct giga_directory *dircache = NULL;

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

static struct fuse_cache_entry* fuse_cache;

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
                      DIR_handle_t handle,
                      struct giga_directory* entry) {
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_insert(%d)", handle);

    // printf("INSERT %d %d, length: %ld, cap: %ld\n", handle, entry->split_flag, lru->length_+1, lru->capacity_);

    struct giga_directory* old;
    HASH_FIND_INT(lru->table, &handle, old);
    if (old != NULL) {
        HASH_DEL(lru->table, old);
        double_list_remove(old);
        lru_cache_unref(old);
    } else {
        ++lru->length_;
    }

    HASH_ADD_INT(lru->table, handle, entry);
    double_list_append(&(lru->dummy), entry);
    entry->refcount = 2;

    while (lru->length_ > lru->capacity_ && lru->dummy.next != &(lru->dummy)) {
        struct giga_directory* old = lru->dummy.next;

        // printf("EVICT ENTRY %d\n", old->handle);

        HASH_DEL(lru->table, old);
        double_list_remove(old);
        lru_cache_unref(old);
        --lru->length_;
    }

    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_insert(%d)", handle);
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
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_release(%d)", entry->handle);
    lru_cache_unref(entry);
    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_release(%d)", entry->handle);
}

void lru_cache_erase(lru_cache_t* lru,
                     DIR_handle_t handle) {
    ACQUIRE_MUTEX(&(lru->mutex_), "lru_cache_erase(%d)", handle);
    struct giga_directory* old;
    HASH_FIND_INT(lru->table, &handle, old);

    if (old != NULL) {
//        printf("DEL %d %d %d\n", old->handle, old->split_flag, old->refcount);

        double_list_remove(old);
        HASH_DEL(lru->table, old);
        lru_cache_unref(old);
        --lru->length_;
    }
    RELEASE_MUTEX(&(lru->mutex_), "lru_cache_erase(%d)", handle);
}

static int shard_cache_get_shard(DIR_handle_t handle) {
    return handle >> ((sizeof(DIR_handle_t) << 3) - NUM_SHARDS_BITS);
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
    return shard_cache_lookup(&my_dircache, *handle);
}

void cache_insert(DIR_handle_t *handle,
                  struct giga_directory* giga_dir) {
    shard_cache_insert(&my_dircache, *handle, giga_dir);
}

void cache_release(struct giga_directory* giga_dir) {
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

    //d->refcount = 1;
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

    fuse_cache = NULL;

    return 0;
}

struct giga_directory* cache_fetch(DIR_handle_t *handle)
{
    if (dircache == NULL) {
        cache_init();
    }

    if (dircache->handle != *handle) {
        return NULL;
    }

    return dircache;
}

int cache_update(DIR_handle_t *handle, struct giga_mapping_t *mapping)
{
    (void) handle;
    giga_update_cache(&dircache->mapping, mapping);
    return 1;
}

inline char* build_fuse_cache_key(DIR_handle_t dir_id, const char* path) {
  char* key = (char*) malloc(16+strlen(path)+1);
  sprintf(key, "%016x", dir_id);
  strcpy(key+16, path);
  return key;
}

void fuse_cache_insert(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id)
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

        HASH_ADD_KEYPTR(hh, fuse_cache, entry->pathname,
                        strlen(entry->pathname), entry);
        logMessage(LOG_DEBUG, __func__, "insert::[%d][%s]-->[%d]",
                   dir_id, path, (int)inode_id);
    } else {
        entry->inode_id = inode_id;
        entry->inserted_time = time(NULL);
    }
}

DIR_handle_t fuse_cache_lookup(DIR_handle_t dir_id, const char* path,
                               time_t *timestamp)
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
        logMessage(LOG_DEBUG, __func__, "lookup::[%d][%s]-->[%d]",
                   dir_id, path, (int)ret->inode_id);
        *timestamp = ret->inserted_time;
        return ret->inode_id;
    }
}

void fuse_cache_update(DIR_handle_t dir_id, const char* path,
                       DIR_handle_t inode_id) {
    struct fuse_cache_entry* ret;
    char* key = build_fuse_cache_key(dir_id, path);
    HASH_FIND_STR(fuse_cache, key, ret);
    free(key);
    if (ret != NULL) {
        logMessage(LOG_DEBUG, __func__, "lookup::[%d][%s]-->[%d]",
                   dir_id, path, (int)ret->inode_id);
        ret->inode_id = inode_id;
        ret->inserted_time = time(NULL);
    }
}

