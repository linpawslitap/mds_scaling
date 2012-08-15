
#include "common/connection.h"
#include "common/debugging.h"

#include "operations.h"

#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <assert.h>

#define METADB_LOG LOG_DEBUG

#define DEFAULT_LEVELDB_CACHE_SIZE 100000
#define DEFAULT_WRITE_BUFFER_SIZE  (16 << 20)
#define DEFAULT_MAX_OPEN_FILES     128
#define DEFAULT_MAX_BATCH_SIZE     1024
#define DEFAULT_SSTABLE_SIZE       (2 << 20)
#define DEFAULT_METRIC_SAMPLING_INTERVAL 1
#define DEFAULT_METADB_LOG_FILE "/tmp/metadb.log" // Default metadb log file location
#define MAX_FILENAME_LEN 1024
#define METADB_KEY_LEN (sizeof(metadb_key_t))
#define METADB_INTERNAL_KEY_LEN (sizeof(metadb_key_t)+8)

#define metadb_error(phase, cond)                                        \
  if (cond != NULL) {                                                    \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, cond); \
    abort();                                                             \
  }

static struct stat INIT_STATBUF;

static
void init_meta_obj_key(metadb_key_t *mkey,
                       metadb_inode_t dir_id,
                       int partition_id,
                       const char* path)
{
    mkey->parent_id = dir_id;
    mkey->partition_id = (uint64_t) partition_id;
    memset(mkey->name_hash, 0, sizeof(mkey->name_hash));
    if (path != NULL)
        giga_hash_name(path, mkey->name_hash);
}

static
void init_meta_obj_seek_key(metadb_key_t *mkey,
                            metadb_inode_t dir_id,
                            int partition_id,
                            const char* name_hash)
{
    mkey->parent_id = dir_id;
    mkey->partition_id = partition_id;
    if (name_hash == NULL) {
        memset(mkey->name_hash, 0, sizeof(mkey->name_hash));
    } else {
        memcpy(mkey->name_hash, name_hash, sizeof(mkey->name_hash));
    }
}

/*
static
size_t metadb_header_valsize(const metadb_val_header_t* mobj) {
    size_t mobj_size = sizeof(metadb_val_header_t) +
                       (mobj->objname_len)  +
                       (mobj->realpath_len) + 2;
    if (((mobj->statbuf.st_mode) & S_IFDIR) > 0) {
        mobj_size += sizeof(metadb_val_dir_t);
    } else {
        mobj_size += mobj->statbuf.st_size;
    }
    return mobj_size;
}
*/

static
size_t metadb_header_size(metadb_val_t* mobj_val) {
    metadb_val_header_t* mobj = (metadb_val_header_t*) (mobj_val->value);
    size_t header_size = sizeof(metadb_val_header_t) +
                       (mobj->objname_len)  +
                       (mobj->realpath_len) + 2;
    return header_size;
}

static
metadb_val_t init_meta_val(const metadb_inode_t inode_id,
                           const size_t objname_len,
                           const char* objname,
                           const size_t realpath_len,
                           const char* realpath)
{
    metadb_val_t mobj_val;
    mobj_val.size = sizeof(metadb_val_header_t)
                       + realpath_len +objname_len + 2;
    mobj_val.value = (char*) malloc(mobj_val.size);

    metadb_val_header_t* mobj = (metadb_val_header_t *) mobj_val.value;

    mobj->objname_len = objname_len;
    mobj->objname = (char*) mobj + sizeof(metadb_val_header_t);
    strncpy(mobj->objname, objname, objname_len);
    mobj->objname[objname_len] = '\0';

    mobj->realpath_len = realpath_len;
    mobj->realpath = (char*) mobj + sizeof(metadb_val_header_t) + objname_len+1;
    strncpy(mobj->realpath, realpath, realpath_len);
    mobj->realpath[realpath_len] = '\0';

    mobj->statbuf = INIT_STATBUF;
    mobj->statbuf.st_ino = inode_id;
    mobj->statbuf.st_mode = (mobj->statbuf.st_mode & ~S_IFMT) | S_IFREG;
    mobj->statbuf.st_nlink = 1;

    time_t now = time(NULL);
    mobj->statbuf.st_atime = now;
    mobj->statbuf.st_mtime = now;
    mobj->statbuf.st_ctime = now;

    return mobj_val;
}

static
metadb_val_t init_dir_val(const metadb_inode_t inode_id,
                          const size_t objname_len,
                          const char* objname,
                          metadb_val_dir_t *dir_val)
{
    metadb_val_t mobj_val;
    size_t header_size = sizeof(metadb_val_header_t)
                       + objname_len + 2;
    mobj_val.size = header_size + sizeof(metadb_val_dir_t);
    mobj_val.value = (char*) malloc(mobj_val.size);

    metadb_val_header_t* mobj = (metadb_val_header_t *) mobj_val.value;

    mobj->objname_len = objname_len;
    mobj->objname = (char*) mobj + sizeof(metadb_val_header_t);
    if (objname_len > 0)
        strncpy(mobj->objname, objname, objname_len);
    mobj->objname[objname_len] = '\0';

    mobj->realpath_len = 0;

    mobj->statbuf = INIT_STATBUF;
    mobj->statbuf.st_ino = inode_id;
    mobj->statbuf.st_mode = (mobj->statbuf.st_mode & ~S_IFMT) | S_IFDIR;
    metadb_val_dir_t* mdir =
          (metadb_val_dir_t*) (mobj_val.value + header_size);
    memcpy(mdir, dir_val, sizeof(metadb_val_dir_t));
    mobj->statbuf.st_nlink = 2;

    time_t now = time(NULL);
    mobj->statbuf.st_atime = now;
    mobj->statbuf.st_mtime = now;
    mobj->statbuf.st_ctime = now;

    return mobj_val;
}

static
void free_metadb_val(metadb_val_t* mobj_val) {
    if (mobj_val->value != NULL) {
        free(mobj_val->value);
        mobj_val->size = 0;
        mobj_val->value = NULL;
    }
}

static void CmpDestroy(void* arg) { if (arg != NULL) {} }

static int CmpCompare(void* arg, const char* a, size_t alen,
                          const char* b, size_t blen) {
    if (arg == NULL) {
      int n = (alen < blen) ? alen : blen;
      int r = memcmp(a, b, n);
      if (r == 0) {
         if (alen < blen) r = -1;
         else if (alen > blen) r = +1;
      }
      return r;
    } else {
      return 0;
    }
}

static const char* CmpName(void* arg) {
    if (arg != NULL) {
      return "wrong";
    }
    return "foo";
}

int metric_thread_errors;

void* metric_thread(void *unused) {
    struct MetaDB* mdb = (struct MetaDB*) unused;

    struct timespec ts;
    ts.tv_sec = DEFAULT_METRIC_SAMPLING_INTERVAL;
    ts.tv_nsec = 0;

    struct timespec rem;
    time_t seconds, flush_seconds=0;
    char* prop;

    do {
        seconds = time(NULL);
        prop = leveldb_property_value(mdb->db, "leveldb.stats");
        if (prop != NULL) {
            fprintf(mdb->logfile, "%ld %s", seconds, prop);
        }
        free(prop);
        if (seconds - flush_seconds > 10) {
            fflush(mdb->logfile);
            flush_seconds = seconds;
        }
        int ret = nanosleep(&ts, &rem);
        if (ret == -1){
            if (errno == EINTR)
                nanosleep(&rem, NULL);
            else
                metric_thread_errors++;
        }
    } while (metric_thread_errors < 50);

    fclose(mdb->logfile);

    return NULL;
}

void metadb_log_init(struct MetaDB *mdb) {
    mdb->logfile = fopen(DEFAULT_METADB_LOG_FILE, "w");
    if (mdb->logfile != NULL) {
        pthread_t tid;
        int ret;
        if ((ret = pthread_create(&tid, NULL, metric_thread, mdb))) {
            logMessage(METADB_LOG, __func__,
                       "pthread_create() error: %d", ret);
            exit(1);
        }
        if ((ret = pthread_detach(tid))) {
            logMessage(METADB_LOG, __func__,
                       "pthread_detach() error: %d", ret);
            exit(1);
        }
    }
}

void metadb_log_destroy() {
    metric_thread_errors = 100;
}


// Returns "0" if a new LDB is created successfully, "1" if an existing LDB is
// opened successfully, and "-1" on error.
int metadb_init(struct MetaDB *mdb, const char *mdb_name)
{
    char* err = NULL;

    mdb->env = leveldb_create_default_env();
    mdb->cache = leveldb_cache_create_lru(DEFAULT_LEVELDB_CACHE_SIZE);
    mdb->cmp = leveldb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);

    mdb->options = leveldb_options_create();
    leveldb_options_set_comparator(mdb->options, mdb->cmp);
    leveldb_options_set_cache(mdb->options, mdb->cache);
    leveldb_options_set_env(mdb->options, mdb->env);
    leveldb_options_set_create_if_missing(mdb->options, 0);
    leveldb_options_set_info_log(mdb->options, NULL);
    leveldb_options_set_write_buffer_size(mdb->options,
                                          DEFAULT_WRITE_BUFFER_SIZE);
    leveldb_options_set_max_open_files(mdb->options, DEFAULT_MAX_OPEN_FILES);
    leveldb_options_set_block_size(mdb->options, 4096);
    leveldb_options_set_compression(mdb->options, leveldb_no_compression);

    mdb->lookup_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(mdb->lookup_options, 1);

    mdb->scan_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(mdb->scan_options, 1);

    mdb->insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(mdb->insert_options, 0);

    mdb->ext_insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(mdb->ext_insert_options, 0);

    mdb->extraction = (metadb_extract_t *) malloc(sizeof(metadb_extract_t));

    pthread_rwlock_init(&(mdb->rwlock_extract), NULL);
    pthread_mutex_init(&(mdb->mtx_bulkload), NULL);
    pthread_mutex_init(&(mdb->mtx_leveldb), NULL);
    pthread_mutex_init(&(mdb->mtx_extload), NULL);

    if (lstat("./", &(INIT_STATBUF)) < 0) {
       logMessage(METADB_LOG, __func__, "Getting init statbuf failed");
        return -1;
    }

    int ret = 0;

    mdb->db = leveldb_open(mdb->options, mdb_name, &err);
    if (err != NULL) {
        if (strstr(err, "(create_if_missing is false)") != NULL) {
            leveldb_options_set_create_if_missing(mdb->options, 1);
            free(err);
            err = NULL;
            mdb->db = leveldb_open(mdb->options, mdb_name, &err);
            if (err != NULL) {
                logMessage(METADB_LOG, __func__, "Init metadb %s", err);
                ret = -1;
            } else {
                ret = 1;
            }
        } else {
            ret = -1;
        }
    }

    metadb_log_init(mdb);

    return ret;
}

int metadb_create(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path,
                  const char *realpath)
{
    int ret = 0;
    metadb_key_t mobj_key;
    metadb_val_t mobj_val;
    char* err = NULL;

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    mobj_val = init_meta_val(0,
                             strlen(path), path,
                             strlen(realpath), realpath);

    logMessage(METADB_LOG, __func__, "create(%s) in (partition=%d,dirid=%d): (%d, %08x)",
               path, partition_id, dir_id, mobj_val.size, mobj_val.value);

    //ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), "metadb_create(%s)", path);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_create(%s)", path);

    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                mobj_val.value, mobj_val.size, &err);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_create(%s)", path);

    //RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_create(%s)", path);

    free_metadb_val(&mobj_val);

    if (err != NULL)
      ret = -1;

    return ret;
}

int metadb_create_dir(struct MetaDB mdb,
                      const metadb_inode_t dir_id, const int partition_id,
                      //const metadb_inode_t inode_id, 
                      const char *path,
                      metadb_val_dir_t* dir_mapping)
{
    int ret = 0;
    metadb_key_t mobj_key;
    metadb_val_t mobj_val;
    char* err = NULL;

    metadb_inode_t inode_id = dir_mapping->id; 
    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    if (path != NULL) {
        mobj_val = init_dir_val(inode_id,
                                strlen(path), path, dir_mapping);
    } else {
        mobj_val = init_dir_val(inode_id, 0, NULL, dir_mapping);
    }

    logMessage(METADB_LOG, __func__, "create_dir(%s) in (partition=%d,dirid=%d): (%d, %08x)",
               path, partition_id, dir_id, mobj_val.size, mobj_val.value);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_create_dir(%s)", path);

    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                mobj_val.value, mobj_val.size, &err);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_create_dir(%s)", path);

    free_metadb_val(&mobj_val);

    if (err != NULL)
      ret = -1;

    return ret;
}

static
metadb_val_t metadb_lookup_internal(struct MetaDB mdb,
                                    const metadb_inode_t dir_id,
                                    const int partition_id,
                                    const char *path) {
    metadb_key_t mobj_key;
    metadb_val_t mobj_val;
    char* err = NULL;

    logMessage(METADB_LOG, __func__,
               "lookup_internal(%s) in (partition=%d,dirid=%ld)",
               path, partition_id, dir_id);

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_lookup_internal(%s)", path);

    mobj_val.value = leveldb_get(mdb.db, mdb.lookup_options,
                             (const char*) &mobj_key, METADB_KEY_LEN, &mobj_val.size, &err);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_lookup_internal(%s)", path);

    if (err != NULL) {
        logMessage(METADB_LOG, __func__,
               "lookup_internal(%s) in (partition=%d,dirid=%ld) failed: (%s)",
               path, partition_id, dir_id, err);
        mobj_val.value = NULL;
        mobj_val.size = 0;
    } else {
        logMessage(METADB_LOG, __func__,
               "lookup_internal(%s) in (partition=%d,dirid=%ld) found entry: (%d, %08x)",
               path, partition_id, dir_id, mobj_val.size, mobj_val.value);
    }

    return mobj_val;
}

static
int metadb_update_internal(struct MetaDB mdb,
                           const metadb_inode_t dir_id,
                           const int partition_id,
                           const char *path,
                           update_func_t update_func,
                           void* arg1) {
    int ret;

    metadb_key_t mobj_key;
    metadb_val_t mobj_val;
    char* err = NULL;

    logMessage(METADB_LOG, __func__,
               "update_internal(%s) in (partition=%d,dirid=%ld)",
               path, partition_id, dir_id);

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_update_internal(%s)", path);

    mobj_val.value = leveldb_get(mdb.db, mdb.lookup_options,
                                (const char*) &mobj_key, METADB_KEY_LEN,
                                &mobj_val.size, &err);

    if ((err == NULL) & (mobj_val.size != 0)) {
        ret = update_func(&mobj_val, arg1);
        if (ret >= 0) {
            leveldb_put(mdb.db, mdb.insert_options,
                        (const char*) &mobj_key, METADB_KEY_LEN,
                        mobj_val.value, mobj_val.size, &err);
            if (err != NULL) {
                logMessage(METADB_LOG, __func__,
                           "update_internal (%s) failed (%s).", path, err);
                ret = -1;
            }
        }
    } else {
        mobj_val.value = NULL;
        mobj_val.size = 0;
        ret = ENOENT;
    }

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_update_internal(%s)", path);

    return ret;
}

int metadb_lookup(struct MetaDB mdb,
                  const metadb_inode_t dir_id, const int partition_id,
                  const char *path, struct stat *statbuf)
{
    int ret = 0;
    metadb_val_t mobj_val;

    mobj_val = metadb_lookup_internal(mdb, dir_id, partition_id, path);

    if (mobj_val.size != 0) {
        *statbuf = ((metadb_val_header_t *) mobj_val.value)->statbuf;
        logMessage(METADB_LOG, __func__, "lookup found entry(%s).", path);
    } else {
        logMessage(METADB_LOG, __func__, "entry(%s) not found.", path);
        ret = -ENOENT;
    }

    free_metadb_val(&mobj_val);
    return ret;
}

int metadb_read_bitmap(struct MetaDB mdb,
                       const metadb_inode_t dir_id,
                       const int partition_id,
                       const char* path,
                       struct giga_mapping_t* mapping) {
    int ret = 0;
    metadb_val_t mobj_val;

    mobj_val = metadb_lookup_internal(mdb, dir_id, partition_id, path);

    if (mobj_val.size != 0) {
        struct giga_mapping_t* mobj_mapping =
            (struct giga_mapping_t*) (mobj_val.value
                                    + metadb_header_size(&mobj_val));
        *mapping = *mobj_mapping;
        logMessage(METADB_LOG, __func__, "read_bitmap found entry(%s).", path);
    } else {
        logMessage(METADB_LOG, __func__, "entry(%s) not found.", path);
        ret = ENOENT;
    }

    free_metadb_val(&mobj_val);
    return ret;
}

int metadb_write_bitmap_handler(metadb_val_t* mobj_val, void* arg1) {
    struct giga_mapping_t* mobj_mapping =
        (struct giga_mapping_t*) ( mobj_val->value
                                 + metadb_header_size(mobj_val));
    *mobj_mapping = *((struct giga_mapping_t *) arg1);
    return 0;
}

int metadb_write_bitmap(struct MetaDB mdb,
                        const metadb_inode_t dir_id,
                        const int partition_id,
                        const char* path,
                        struct giga_mapping_t* mapping) {
    return metadb_update_internal(mdb, dir_id, partition_id, path,
                                  metadb_write_bitmap_handler,
                                  (void *) mapping);
}

int metadb_close(struct MetaDB mdb) {
    metadb_log_destroy();
    leveldb_close(mdb.db);
    leveldb_options_destroy(mdb.options);
    leveldb_cache_destroy(mdb.cache);
    leveldb_env_destroy(mdb.env);
    leveldb_readoptions_destroy(mdb.lookup_options);
    leveldb_readoptions_destroy(mdb.scan_options);
    leveldb_writeoptions_destroy(mdb.insert_options);
    leveldb_writeoptions_destroy(mdb.ext_insert_options);
    free(mdb.extraction);
    return 0;
}

int metadb_remove(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path) {
    metadb_key_t mobj_key;
    char* err = NULL;

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_remove(%s)", path);
    leveldb_delete(mdb.db, mdb.insert_options,
            (char *) &mobj_key, METADB_KEY_LEN,
            &err);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_remove(%s)", path);
    if (err == NULL) {
        return 0;
    } else {
        return -1;
    }
}

int readdir_filler(char* buf,
                   const size_t buf_len,
                   size_t* buf_offset,
                   metadb_val_t mval) {
    readdir_rec_len_t rec_len = (readdir_rec_len_t) metadb_header_size(&mval);
    if ((*buf_offset) + rec_len + sizeof(rec_len) <= buf_len) {
        buf += (*buf_offset);
        memcpy(buf, &rec_len, sizeof(rec_len));
        buf += sizeof(rec_len);
        memcpy(buf, (void *) mval.value, rec_len);
        *buf_offset = (*buf_offset) + rec_len + sizeof(rec_len);
        return 0;
    } else {
        return 1;
    }
}

metadb_readdir_iterator_t* metadb_create_readdir_iterator(const char* buf,
        size_t buf_len, size_t num_entries) {
    metadb_readdir_iterator_t* iter = (metadb_readdir_iterator_t *)
        malloc(sizeof(metadb_readdir_iterator_t));
    iter->buf = buf;
    iter->buf_len = buf_len;
    iter->num_ent = num_entries;
    iter->offset = 0;
    iter->cur_ent = 0;
    return iter;
}

void metadb_destroy_readdir_iterator(metadb_readdir_iterator_t *iter) {
    free(iter);
}

void metadb_readdir_iter_begin(metadb_readdir_iterator_t *iter) {
    iter->offset = 0;
    iter->cur_ent = 0;
}

int metadb_readdir_iter_valid(metadb_readdir_iterator_t *iter) {
    return (iter->offset < iter->buf_len && iter->cur_ent < iter->num_ent);
}

void metadb_readdir_iter_next(metadb_readdir_iterator_t *iter) {
    iter->cur_ent ++;
    if (iter->cur_ent < iter->num_ent) {
        readdir_rec_len_t rec_len =
            *((readdir_rec_len_t *) (iter->buf + iter->offset));
        iter->offset += rec_len + sizeof(rec_len);
    }
}

const char* metadb_readdir_iter_get_objname(metadb_readdir_iterator_t *iter,
                                      size_t *name_len) {
    if (iter->cur_ent < iter->num_ent) {
        metadb_val_header_t* header = (metadb_val_header_t *)
            (iter->buf + iter->offset + sizeof(readdir_rec_len_t));
        *name_len = header->objname_len;
        return (char *) header + sizeof(metadb_val_header_t);
    } else {
        return NULL;
    }
}

const char* metadb_readdir_iter_get_realpath(metadb_readdir_iterator_t *iter,
                                       size_t *path_len) {
    if (iter->cur_ent < iter->num_ent) {
        metadb_val_header_t* header = (metadb_val_header_t *)
            (iter->buf + iter->offset + sizeof(readdir_rec_len_t));
        *path_len = header->realpath_len;
        return (char *) header + sizeof(metadb_val_header_t)
                               + header->objname_len + 1;
    } else {
        return NULL;
    }
}

int metadb_readdir_iter_get_stat(metadb_readdir_iterator_t *iter,
                                 struct stat *statbuf) {
    if (iter->cur_ent < iter->num_ent) {
        memcpy(statbuf,
               iter->buf + iter->offset + sizeof(readdir_rec_len_t),
               sizeof(struct stat));
        return 0;
    } else {
        return -1;
    }
}

int metadb_readdir(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   int* partition_id,
                   const char* start_key,
                   char* buf,
                   const size_t buf_len,
                   int *num_entries,
                   char* end_key,
                   int* more_entries_flag) {
    int ret = 0;
    size_t buf_offset = 0;
    int entry_count = 0;
    *num_entries = 0;
    *more_entries_flag = 0;
    metadb_key_t mobj_key;
    if (*partition_id < 0) {
        init_meta_obj_seek_key(&mobj_key, dir_id, 0, NULL);
    } else {
        init_meta_obj_seek_key(&mobj_key, dir_id, *partition_id, start_key);
    }

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb),
                "metadb_readdir(p[%d])", *partition_id);

    leveldb_iterator_t* iter =
        leveldb_create_iterator(mdb.db, mdb.scan_options);
    leveldb_iter_seek(iter, (char *) &mobj_key, METADB_KEY_LEN);
    if (leveldb_iter_valid(iter)) {
        do {
            metadb_key_t* iter_key;
            metadb_val_t  iter_val;
            size_t klen;
            iter_key = (metadb_key_t*) leveldb_iter_key(iter, &klen);
            if (iter_key->parent_id == dir_id) {
                if (iter_key->partition_id >= 0) {
                    iter_val.value =
                        (char *) leveldb_iter_value(iter, &iter_val.size);
                    int fret = readdir_filler(buf, buf_len, &buf_offset, iter_val);
                    if (fret > 0) {
                        memcpy(end_key, iter_key->name_hash, HASH_LEN);
                        *more_entries_flag = 1;
                        *partition_id = iter_key->partition_id;
                        // Check if there is no more entries
                        /*
                        leveldb_iter_next(iter);
                        if (leveldb_iter_valid(iter)) {
                            iter_key =
                                (metadb_key_t*) leveldb_iter_key(iter, &klen);
                            if (iter_key->parent_id == dir_id) {
                                *more_entries_flag = 1;
                                *partition_id = iter_key->partition_id;
                            }
                        }
                        */
                        break;
                    } else {
                        entry_count += 1;
                    }
                }
            } else {
                break;
            }
            leveldb_iter_next(iter);
        } while (leveldb_iter_valid(iter));
    } else {
        //printf("metadb_readdir: Invalid Iterator.\n");
        ret = -ENOENT;
    }
    leveldb_iter_destroy(iter);

    RELEASE_MUTEX(&(mdb.mtx_leveldb),
                "metadb_readdir(p[%d])", *partition_id);
    *num_entries = entry_count;
    return ret;
}

static void build_sstable_filename(const char* dir_with_new_partition,
                                   int new_partition_id,
                                   int num_new_sstable,
                                   char* sstable_filename) {
  snprintf(sstable_filename, MAX_FILENAME_LEN,
           "%s/p%d-%08x.sst", dir_with_new_partition,
           new_partition_id, num_new_sstable);
}

static void construct_new_key(const char* old_key,
                              int key_len,
                              int new_partition_id,
                              char* new_key) {
    memcpy(new_key, old_key, key_len);
    metadb_key_t* user_key = (metadb_key_t*) new_key;
    user_key->partition_id = new_partition_id;
}

static uint64_t get_sequence_number(const char* key,
                                    int key_len) {
    uint64_t num;
    //FIXME: Current code only considers little-endian
    memcpy(&num, (key + key_len - 8), sizeof(num));
    return num >> 8;
}

static int directory_exists(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        return 1;
    } else {
        return 0;
    }
}

int metadb_extract_do(struct MetaDB mdb,
                      const metadb_inode_t dir_id,
                      const int old_partition_id,
                      const int new_partition_id,
                      const char* dir_with_new_partition,
                      uint64_t* min_sequence_number,
                      uint64_t* max_sequence_number)
{
    int ret = 0;
    char* err = NULL;

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_extract(p%d->p%d)",
                    old_partition_id, new_partition_id);

    ACQUIRE_MUTEX(&(mdb.mtx_extload), "metadb_extract(p%d->p%d)",
                    old_partition_id, new_partition_id);


    //ACQUIRE_RWLOCK_WRITE(&(mdb.rwlock_extract), "metadb_extract(p%d->p%d)",
    //                     old_partition_id, new_partition_id);

    metadb_extract_t* extraction = mdb.extraction;
    //mdb.extraction->in_extraction = 1;
    extraction->dir_id = dir_id;
    extraction->old_partition_id = old_partition_id;
    extraction->new_partition_id = new_partition_id;
    strcpy(extraction->dir_with_new_partition, dir_with_new_partition);
    if (!directory_exists(dir_with_new_partition)) {
        mkdir(dir_with_new_partition, DEFAULT_MODE);
    }

    if (ret < 0) {
        //RELEASE_RWLOCK(&(mdb.rwlock_extract),  "metadb_extract(p%d->p%d)", old_partition_id, new_partition_id);

        RELEASE_MUTEX(&(mdb.mtx_extload), "metadb_extract(p%d->p%d)",
                    old_partition_id, new_partition_id);

        RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_extract(p%d->p%d)",
                  old_partition_id, new_partition_id);

        return ret;
    }

    metadb_key_t mobj_key;
    init_meta_obj_seek_key(&mobj_key, dir_id, old_partition_id, NULL);

    int num_new_sstable = 0;
    int num_migrated_entries = 0;
    char sstable_filename[MAX_FILENAME_LEN];
    char new_internal_key[METADB_INTERNAL_KEY_LEN];
    build_sstable_filename(dir_with_new_partition,
                           new_partition_id, num_new_sstable,
                           sstable_filename);
    leveldb_tablebuilder_t* builder = leveldb_tablebuilder_create(
        mdb.options, sstable_filename, mdb.env, &err);
    metadb_error("create new builder", err);

    leveldb_iterator_t* iter =
      leveldb_create_iterator(mdb.db, mdb.scan_options);
    leveldb_writebatch_t* batch = leveldb_writebatch_create();

    if (!leveldb_iter_valid(iter)) {

        uint64_t min_seq = 0;
        uint64_t max_seq = 0;
        leveldb_iter_seek(iter, (char *) &mobj_key, METADB_KEY_LEN);

        while (leveldb_iter_valid(iter)) {
            size_t klen;
            const char* iter_ori_key = leveldb_iter_key(iter, &klen);
            metadb_key_t* iter_key = (metadb_key_t*) iter_ori_key;


            if (iter_key->parent_id == dir_id &&
                iter_key->partition_id == old_partition_id) {

                size_t vlen;
                const char* iter_ori_val = leveldb_iter_value(iter, &vlen);
                if (giga_file_migration_status_with_hash(iter_key->name_hash,
                                                         new_partition_id)) {

                    leveldb_writebatch_delete(batch, iter_ori_key, klen);

                    size_t iklen;
                    const char* iter_internal_key =
                        leveldb_iter_internalkey(iter, &iklen);
                    construct_new_key(iter_internal_key, iklen,
                        new_partition_id, new_internal_key);
                    leveldb_tablebuilder_put(builder,
                        new_internal_key, iklen, iter_ori_val, vlen);

                    uint64_t sequence_number =
                        get_sequence_number(iter_internal_key, iklen);
                    if (!num_migrated_entries) {
                        min_seq = sequence_number;
                        max_seq = sequence_number;
                    } else {
                        if (sequence_number < min_seq) {
                            min_seq = sequence_number;
                        } else if (sequence_number > max_seq) {
                            max_seq = sequence_number;
                        }
                    }

                    num_migrated_entries++;
                }

                if (leveldb_tablebuilder_size(builder) >= DEFAULT_SSTABLE_SIZE)
                {
                    // flush sstable file
                    leveldb_tablebuilder_destroy(builder);
                    // create new sstable builder
                    ++num_new_sstable;
                    build_sstable_filename(dir_with_new_partition,
                        new_partition_id, num_new_sstable,
                        sstable_filename);
                    builder = leveldb_tablebuilder_create(
                    mdb.options, sstable_filename, mdb.env, &err);
                    metadb_error("create new builder", err);
                    // delete moved entries
                    leveldb_write(mdb.db, mdb.insert_options, batch, &err);
                    metadb_error("delete moved entreis", err);
                    leveldb_writebatch_clear(batch);
                }
            } else {
                break;
            }
            leveldb_iter_next(iter);
        }
        if (leveldb_tablebuilder_size(builder) > 0) {
            leveldb_write(mdb.db, mdb.insert_options, batch, &err);
            metadb_error("delete moved entreis", err);
        }

        *min_sequence_number = min_seq;
        *max_sequence_number = max_seq;
        ret = num_migrated_entries;
    } else {
        ret = -ENOENT;
    }

    leveldb_writebatch_destroy(batch);
    leveldb_tablebuilder_destroy(builder);
    leveldb_iter_destroy(iter);

    if (ret < 0) {
        // commented by SVP:
        // FIXME?? what if this condition is false?? where do you release this lock 
        //
 //       RELEASE_RWLOCK(&(mdb.rwlock_extract),  "metadb_extract(p%d->p%d)", old_partition_id, new_partition_id);
        RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_extract(p%d->p%d)",
                      old_partition_id, new_partition_id);
    }


    RELEASE_MUTEX(&(mdb.mtx_extload), "metadb_extract(p%d->p%d)",
                    old_partition_id, new_partition_id);

    return ret;
}

/*
int metadb_extract_do(struct MetaDB mdb,
                      const metadb_inode_t dir_id,
                      const int old_partition_id,
                      const int new_partition_id,
                      const char* dir_with_new_partition,
                      uint64_t *min_sequence_number,
                      uint64_t *max_sequence_number)
{

    int ret = 0;
    char* err = NULL;

    ACQUIRE_RWLOCK_WRITE(&(mdb.rwlock_extract), "rwlock_extract in metadb_extract");


    metadb_extract_t* extraction = mdb.extraction;
    //mdb.extraction->in_extraction = 1;
    extraction->dir_id = dir_id;
    extraction->old_partition_id = old_partition_id;
    extraction->new_partition_id = new_partition_id;
    strcpy(extraction->dir_with_new_partition, dir_with_new_partition);
    if (!directory_exists(dir_with_new_partition)) {
        ret = mkdir(dir_with_new_partition, DEFAULT_MODE);
    }

    if (ret < 0) {
        RELEASE_RWLOCK(&(mdb.rwlock_extract), "rwlock_extract in metadb_extract");
        return ret;
    }

    metadb_key_t mobj_key;
    extraction->extract_db = leveldb_open(mdb.options,
                extraction->dir_with_new_partition, &err);
    metadb_error("create new extract_db:", err);
    if (err != NULL) {
        RELEASE_RWLOCK(&(mdb.rwlock_extract), "rwlock_extract in metadb_extract");
        return -1;
    }

    init_meta_obj_seek_key(&mobj_key,
                           extraction->dir_id,
                           extraction->old_partition_id,
                           NULL);

    int num_migrated_entries = 0;
    int num_inprogress_entries = 0;
    char new_key[METADB_KEY_LEN];

    leveldb_iterator_t* iter =
      leveldb_create_iterator(mdb.db, mdb.scan_options);
    leveldb_writebatch_t* del_batch = leveldb_writebatch_create();
    leveldb_writebatch_t* add_batch = leveldb_writebatch_create();

    if (!leveldb_iter_valid(iter)) {
        leveldb_iter_seek(iter, (char *) &mobj_key, METADB_KEY_LEN);

        while (leveldb_iter_valid(iter)) {
            size_t klen;
            const char* iter_ori_key = leveldb_iter_key(iter, &klen);
            metadb_key_t* iter_key = (metadb_key_t*) iter_ori_key;

            if (iter_key->parent_id == extraction->dir_id &&
                iter_key->partition_id == extraction->old_partition_id) {

                if (giga_file_migration_status_with_hash(iter_key->name_hash,
                                                extraction->new_partition_id)) {
                    size_t vlen;
                    const char* iter_ori_val = leveldb_iter_value(iter, &vlen);

                    leveldb_writebatch_delete(del_batch, iter_ori_key, klen);
                    construct_new_key(iter_ori_key, klen,
                            extraction->new_partition_id, new_key);
                    leveldb_writebatch_put(add_batch,
                                           new_key, klen,
                                           iter_ori_val, vlen);

                    num_inprogress_entries++;
                }

                if (num_inprogress_entries >= DEFAULT_MAX_BATCH_SIZE)
                {
                    leveldb_write(extraction->extract_db,
                                  mdb.ext_insert_options, add_batch, &err);
                    metadb_error("insert moved entreis", err);
                    leveldb_write(mdb.db, mdb.insert_options, del_batch, &err);
                    metadb_error("delete moved entreis", err);
                    num_migrated_entries += num_inprogress_entries;
                    num_inprogress_entries = 0;
                    leveldb_writebatch_clear(add_batch);
                    leveldb_writebatch_clear(del_batch);
                }
            } else {
                break;
            }
            leveldb_iter_next(iter);
        }
        if (num_inprogress_entries > 0) {
            leveldb_write(extraction->extract_db,
                          mdb.ext_insert_options, add_batch, &err);
            metadb_error("insert moved entreis", err);
            leveldb_write(mdb.db, mdb.insert_options, del_batch, &err);
            metadb_error("delete moved entries", err);
            num_migrated_entries += num_inprogress_entries;
            num_inprogress_entries = 0;
        }
        *min_sequence_number = 0;
        *max_sequence_number = num_migrated_entries;
        ret = num_migrated_entries;
    } else {
        ret = -ENOENT;
    }

    //TODO: Really need to compact? How to make sure all logs become sst files?
    if (ret >= 0) {
        leveldb_compact_range(extraction->extract_db, NULL, 0, NULL, 0);
    }

    leveldb_writebatch_destroy(add_batch);
    leveldb_writebatch_destroy(del_batch);
    leveldb_iter_destroy(iter);
    leveldb_close(mdb.extraction->extract_db);

    if (ret < 0) {
        RELEASE_RWLOCK(&(mdb.rwlock_extract), "rwlock_extract in metadb_extract");
    }

    return ret;
}
*/

int metadb_extract_clean(struct MetaDB mdb) {
    int ret = 0;
    //Remove directories
    //Close extractdb
    //Remove extractdb


    // char* err = NULL;
    //mdb.extraction->in_extraction = 0;
    //leveldb_destroy_db(mdb.options,
    //                   mdb.extraction->dir_with_new_partition,
    //                   &err);

    if (rmdir(mdb.extraction->dir_with_new_partition) < 0) {
        if (errno == ENOTEMPTY) {
            DIR* dp = opendir(mdb.extraction->dir_with_new_partition);
            char fullpath[MAX_FILENAME_LEN];
            snprintf(fullpath, MAX_FILENAME_LEN, "%s/",
                     mdb.extraction->dir_with_new_partition);
            size_t prefix_len = strlen(mdb.extraction->dir_with_new_partition);
            if (dp != NULL) {
                struct dirent *de;
                while ((de = readdir(dp)) != NULL) {
                  if (strcmp(de->d_name,".")!=0 && strcmp(de->d_name,"..")!=0) {
                    sprintf(fullpath+prefix_len+1, "%s", de->d_name);
                    unlink(fullpath);
                  }
                }
                closedir(dp);
            }
          ret = -rmdir(mdb.extraction->dir_with_new_partition);
        }
    }

    //RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_extract(ret=%d)", ret);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_extract_clean", "");

    return ret;
}

int strendswith(const char* srcstr, const char* pattern) {
    size_t srcstr_len = strlen(srcstr);
    size_t pattern_len = strlen(pattern);
    if (pattern_len <= srcstr_len) {
        if (strcmp(srcstr+srcstr_len-pattern_len, pattern) == 0) {
            return 1;
        }
    }
    return 0;
}

int metadb_bulkinsert(struct MetaDB mdb,
                      const char* dir_with_new_partition,
                      uint64_t min_sequence_number,
                      uint64_t max_sequence_number) {

    int ret = 0;
    char sstable_filename[MAX_FILENAME_LEN];
    char* err = NULL;

    //ACQUIRE_RWLOCK_WRITE(&(mdb.rwlock_extract), "metadb_bulkinsert(%s)", dir_with_new_partition);

    ACQUIRE_MUTEX(&(mdb.mtx_leveldb), "metadb_bulkinsert(%s)",
                    dir_with_new_partition);

    DIR* dp = opendir(dir_with_new_partition);
    if (dp != NULL) {
        struct dirent *de;
        while ((de = readdir(dp)) != NULL) {
          if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0 &&
              strendswith(de->d_name, ".sst")) {
            snprintf(sstable_filename, MAX_FILENAME_LEN,
                     "%s/%s", dir_with_new_partition, de->d_name);
            leveldb_bulkinsert(mdb.db, mdb.insert_options,
                               sstable_filename,
                               min_sequence_number,
                               max_sequence_number,
                               &err);
            metadb_error("bulkinsert", err);
          }
        }
        closedir(dp);
    }

    //RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_bulkinsert(%s)", dir_with_new_partition);

    RELEASE_MUTEX(&(mdb.mtx_leveldb), "metadb_bulkinsert(%s)",
                    dir_with_new_partition);

    return ret;
}
