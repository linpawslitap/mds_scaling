
#include "fhlist.h"
#include "common/options.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <pthread.h>

#define CACHE_LOG LOG_DEBUG

//entry for file handle list
typedef struct {
    UT_hash_handle hh;
    int open_count;
    int path_len;
    giga_dir_id dir_id;
    char path[];
} fhlist_entry_t;

typedef struct {
    giga_dir_id dir_id;
    char path[];
} fhlist_lookup_key_t;

static fhlist_entry_t* fhlist;
static pthread_rwlock_t fhlist_lock;

int fhlist_init()
{
    fhlist = NULL;
    if (pthread_rwlock_init(&fhlist_lock, NULL) != 0) {
      //LOG_ERR("ERR_init_fhlist_rwlock",);
      exit(1);
    }
    return 0;
}

void fhlist_insert(giga_dir_id dir_id, giga_pathname path)
{
    fhlist_entry_t* entry
      = (fhlist_entry_t *) malloc(sizeof(fhlist_entry_t) + strlen(path));
    memset(entry, 0, sizeof(fhlist_entry_t) + strlen(path));
    entry->path_len = strlen(path);
    entry->dir_id = dir_id;
    entry->open_count = 0;
    memcpy(entry->path, path, entry->path_len);

    unsigned keylen = offsetof(fhlist_entry_t, path)
                     + entry->path_len
                     - offsetof(fhlist_entry_t, dir_id);

    HASH_ADD(hh, fhlist, dir_id, keylen, entry);
}

fhlist_entry_t* fhlist_get(giga_dir_id dir_id, giga_pathname path)
{
    fhlist_lookup_key_t* lookup_key;
    fhlist_entry_t* entry;
    unsigned path_len = strlen(path);
    lookup_key =
      (fhlist_lookup_key_t*) malloc(sizeof(fhlist_lookup_key_t) + path_len);
    memset(lookup_key, 0, sizeof(fhlist_lookup_key_t) + path_len);
    lookup_key->dir_id = dir_id;
    memcpy(lookup_key->path, path, path_len);
    unsigned keylen = offsetof(fhlist_lookup_key_t, path) + path_len
                     - offsetof(fhlist_lookup_key_t, dir_id);
    HASH_FIND(hh, fhlist, &lookup_key->dir_id, keylen, entry);
    free(lookup_key);
    return entry;
}

void fhlist_open(giga_dir_id dir_id, giga_pathname path) {
    if (pthread_rwlock_wrlock(&fhlist_lock) != 0) {
        //LOG_ERR("ERR_get_fhlist_wrlock: fhlist_open");
        return;
    }
    fhlist_entry_t* entry = fhlist_get(dir_id, path);
    if (entry == NULL) {
      fhlist_insert(dir_id, path);
    } else {
      entry->open_count += 1;
    }
    pthread_rwlock_unlock(&fhlist_lock);
}

void fhlist_close(giga_dir_id dir_id, giga_pathname path) {
    if (pthread_rwlock_wrlock(&fhlist_lock) != 0) {
        //LOG_ERR("ERR_get_fhlist_wrlock: fhlist_close");
        return;
    }
    fhlist_entry_t* entry = fhlist_get(dir_id, path);
    if (entry != NULL) {
      if (entry->open_count > 1) {
        entry->open_count -= 1;
      } else {
        HASH_DEL(fhlist, entry);
      }
    }
    pthread_rwlock_unlock(&fhlist_lock);
}

int fhlist_get_count(giga_dir_id dir_id, giga_pathname path) {
    if (pthread_rwlock_rdlock(&fhlist_lock) != 0) {
        //LOG_ERR("ERR_get_fhlist_rdlock: fhlist_get_count");
        return 0;
    }
    fhlist_entry_t* entry = fhlist_get(dir_id, path);
    if (entry != NULL) {
      return entry->open_count;
    } else {
      return 0;
    }
    pthread_rwlock_unlock(&fhlist_lock);
}
