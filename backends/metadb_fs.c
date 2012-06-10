#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"

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
#define DEFAULT_WRITE_BUFFER_SIZE  100000
#define DEFAULT_MAX_OPEN_FILES     128
#define DEFAULT_MAX_BATCH_SIZE     1024
#define DEFAULT_SSTABLE_SIZE       (2 << 20)
#define MAX_FILENAME_LEN 1024
#define METADB_KEY_LEN (sizeof(metadb_key_t))
#define METADB_INTERNAL_KEY_LEN (sizeof(metadb_key_t)+8)

#define metadb_error(phase, cond)                                        \
  if (cond != NULL) {                                                    \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, cond); \
    abort();                                                             \
  }

static
void safe_free(char** ptr)
{
    if (*ptr) {
        free(*ptr);
        *ptr = NULL;
    }
}

static
void init_meta_obj_key(metadb_key_t *mkey,
                       metadb_inode_t dir_id, int partition_id, const char* path)
{
    mkey->parent_id = dir_id;
    mkey->partition_id = (uint64_t) partition_id;
    memset(mkey->name_hash, 0, sizeof(mkey->name_hash));
    giga_hash_name(path, mkey->name_hash);
}

static
void print_meta_obj_key(metadb_key_t *mkey) {
//    printf("%ld, %ld, %s\n", mkey->parent_id, mkey->partition_id, mkey->name_hash);
    printf("%s\n",  mkey->name_hash);
}

static
void init_meta_obj_seek_key(metadb_key_t *mkey,
                            metadb_inode_t dir_id, int partition_id)
{
    mkey->parent_id = dir_id;
    mkey->partition_id = partition_id;
    memset(mkey->name_hash, 0, sizeof(mkey->name_hash));
}

static
metadb_obj_t* create_metadb_obj(const char* objname, const size_t objname_len,
                                const char* realpath, const size_t realpath_len)
{
    size_t allocation = sizeof(metadb_obj_t) + realpath_len + objname_len + 2;
    metadb_obj_t *mobj = (metadb_obj_t*) malloc(allocation);

    if (mobj != NULL) {
        mobj->objname_len = objname_len;
        mobj->realpath_len = realpath_len;

        mobj->objname = (char*)mobj + sizeof(metadb_obj_t);
        strncpy(mobj->objname, objname, objname_len);
        mobj->objname[objname_len] = '\0';

        mobj->realpath = (char*)mobj + sizeof(metadb_obj_t) + objname_len + 1;
        strncpy(mobj->realpath, realpath, realpath_len);
        mobj->realpath[realpath_len] = '\0';
    }
    return mobj;
}

static
size_t metadb_obj_size(const metadb_obj_t* mobj) {
    size_t mobj_size = sizeof(metadb_obj_t) +
                       (mobj->objname_len)  +
                       (mobj->realpath_len) + 2;
    return mobj_size;
}

static
void init_meta_obj_statbuf(metadb_obj_t* mobj,
     const metadb_inode_t inode_id, metadb_obj_type_t obj_type)
{

    mobj->obj_type = obj_type;

    //FIXME: Initialize Inode

    lstat("./", &(mobj->statbuf));
    mobj->statbuf.st_ino = inode_id;
    if (obj_type == OBJ_DIR) {
        mobj->statbuf.st_mode = (mobj->statbuf.st_mode & ~S_IFMT) | S_IFDIR;
    } else {
        mobj->statbuf.st_mode = (mobj->statbuf.st_mode & ~S_IFMT) | S_IFREG;
    }

    logMessage(METADB_LOG, __func__, "init_meta_obj: %16lx",
               mobj->statbuf.st_mode);

    /*
    mobj->statbuf.st_ino  = inode_id;
    mobj->statbuf.st_mode = DEFAULT_MODE;
    if (obj_type == OBJ_DIR)
        mobj->statbuf.st_nlink   = 2;
    else
        mobj->statbuf.st_nlink   = 1;

    mobj->statbuf.st_uid  = 1000;
    mobj->statbuf.st_gid  = 1000;
    mobj->statbuf.st_size = 4096;
    */

    time_t now = time(NULL);
    mobj->statbuf.st_atime = now;
    mobj->statbuf.st_mtime = now;
    mobj->statbuf.st_ctime = now;
}

static void CmpDestroy(void* arg) { assert(arg==NULL); }

static int CmpCompare(void* arg, const char* a, size_t alen,
                      const char* b, size_t blen) {
  assert(arg==NULL);
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

static const char* CmpName(void* arg) {
  assert(arg==NULL);
  return "foo";
}

int metadb_init(struct MetaDB *mdb, const char *mdb_name)
{
    char* err = NULL;

    mdb->env = leveldb_create_default_env();
    mdb->cache = leveldb_cache_create_lru(DEFAULT_LEVELDB_CACHE_SIZE);
    mdb->cmp = leveldb_comparator_create(NULL, CmpDestroy, CmpCompare, CmpName);

    mdb->options = leveldb_options_create();
    leveldb_options_set_cache(mdb->options, mdb->cache);
    leveldb_options_set_comparator(mdb->options, mdb->cmp);
    leveldb_options_set_env(mdb->options, mdb->env);
    leveldb_options_set_create_if_missing(mdb->options, 1);
    leveldb_options_set_info_log(mdb->options, NULL);
    leveldb_options_set_write_buffer_size(mdb->options,
                                          DEFAULT_WRITE_BUFFER_SIZE);
    leveldb_options_set_max_open_files(mdb->options, DEFAULT_MAX_OPEN_FILES);
    leveldb_options_set_block_size(mdb->options, 1024);
    leveldb_options_set_compression(mdb->options, leveldb_no_compression);

    mdb->lookup_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(mdb->lookup_options, 1);

    mdb->scan_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(mdb->scan_options, 1);

    mdb->insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(mdb->insert_options, 0);

    mdb->ext_insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(mdb->ext_insert_options, 1);

    mdb->extraction = (metadb_extract_t *) malloc(sizeof(metadb_extract_t));

    pthread_rwlock_init(&(mdb->rwlock_extract), NULL);
    pthread_mutex_init(&(mdb->mtx_bulkload), NULL);

    mdb->db = leveldb_open(mdb->options, mdb_name, &err);
    metadb_error("leveldb_init", err);

    logMessage(METADB_LOG, __func__, "LevelDB table(%s) created.", mdb_name);

    return 0;
}


int metadb_create(struct MetaDB mdb,
                  const metadb_inode_t dir_id, const int partition_id,
                  metadb_obj_type_t entry_type,
                  const metadb_inode_t inode_id, const char *path,
                  const char *realpath)
{
    int ret = 0;
    metadb_key_t mobj_key;
    metadb_obj_t* mobj;
    char* err = NULL;

    logMessage(METADB_LOG, __func__, "create(%s) in (partition=%d,dirid=%d)",
               path, partition_id, dir_id);

    //TODO: check if the ibject exists: return error if it does

    //TODO: how do we treat different "entry_type" differently?

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);
    mobj = create_metadb_obj(path, strlen(path),
                             realpath, strlen(realpath));
    init_meta_obj_statbuf(mobj, inode_id, entry_type);
    size_t msize = metadb_obj_size(mobj);

    ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), "metadb_create(%s)", path);

    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                (const char*) mobj, msize, &err);

    RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_create(%s)", path);

    safe_free((char **) (&mobj));
    if (err != NULL)
      ret = -1;

    return ret;
}

int metadb_lookup(struct MetaDB mdb,
                  const metadb_inode_t dir_id, const int partition_id,
                  const char *path, struct stat *stbuf)
{
    int ret = 0;
    metadb_key_t mobj_key;
    metadb_obj_t* mobj;
    char* err = NULL;

    char* val;
    size_t val_len;

    logMessage(METADB_LOG, __func__, "lookup(%s) in (partition=%d,dirid=%ld)",
               path, partition_id, dir_id);

    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), "metadb_lookup(%s)", path);

    val = leveldb_get(mdb.db, mdb.lookup_options,
                      (const char*) &mobj_key, METADB_KEY_LEN, &val_len, &err);

    RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_lookup(%s)", path);

    if ((err == NULL) && (val_len != 0)) {
        mobj = (metadb_obj_t*)val;
        *stbuf = mobj->statbuf;
        logMessage(METADB_LOG, __func__, "lookup found entry(%s).", path);
        safe_free(&val);
    } else {
        /*
        if (mdb.extraction->in_extraction) {

            if (mdb.extraction->in_extraction) {
                val = leveldb_get(mdb.extraction->extract_db,mdb.lookup_options,
                      (const char*) &mobj_key, METADB_KEY_LEN, &val_len, &err);
            }

            if ((err == NULL) && (val_len != 0)) {
                mobj = (metadb_obj_t*)val;
                *stbuf = mobj->statbuf;
                logMessage(METADB_LOG, __func__, "lookup found entry(%s).", path);
                safe_free(&val);
                return ret;
            }
        }
        */
        logMessage(METADB_LOG, __func__, "entry(%s) not found.", path);
        ret = ENOENT;
    }

    return ret;
}

int metadb_close(struct MetaDB mdb) {
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

    ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), "metadb_remove(%s)", path);
    leveldb_delete(mdb.db, mdb.insert_options,
            (char *) &mobj_key, METADB_KEY_LEN,
            &err);
    RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_remove(%s)", path);

    if (err == NULL) {
        return 0;
    } else {
        return -1;
    }
}

int metadb_readdir(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   void *buf, fill_dir_t filler) {
  int ret = 0;
  metadb_key_t mobj_key;

  init_meta_obj_seek_key(&mobj_key, dir_id, partition_id);

  ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), 
                      "metadb_readdir(p[%d])", partition_id);

  leveldb_iterator_t* iter = leveldb_create_iterator(mdb.db, mdb.scan_options);
  leveldb_iter_seek(iter, (char *) &mobj_key, METADB_KEY_LEN);
  if (leveldb_iter_valid(iter)) {
    do {
      metadb_key_t* iter_key;
      metadb_obj_t* iter_obj;
      size_t len;
      iter_key = (metadb_key_t*) leveldb_iter_key(iter, &len);
      if (iter_key->parent_id == dir_id &&
          iter_key->partition_id == partition_id) {
        iter_obj = (metadb_obj_t*) leveldb_iter_value(iter, &len);
        filler(buf, iter_key, iter_obj);
      } else {
        break;
      }
      leveldb_iter_next(iter);
    } while (leveldb_iter_valid(iter));
  } else {
    printf("metadb_readdir: Invalid Iterator.\n");
    ret = ENOENT;
  }
  leveldb_iter_destroy(iter);

  RELEASE_RWLOCK(&(mdb.rwlock_extract), 
                 "metadb_readdir(p[%d])", partition_id);

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

    ACQUIRE_RWLOCK_WRITE(&(mdb.rwlock_extract), "metadb_extract(p%d->p%d)", 
                         old_partition_id, new_partition_id);

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
        RELEASE_RWLOCK(&(mdb.rwlock_extract),  "metadb_extract(p%d->p%d)", old_partition_id, new_partition_id);
        return ret;
    }

    metadb_key_t mobj_key;
    init_meta_obj_seek_key(&mobj_key, dir_id, old_partition_id);

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

        uint64_t min_seq, max_seq;
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
        RELEASE_RWLOCK(&(mdb.rwlock_extract),  "metadb_extract(p%d->p%d)", old_partition_id, new_partition_id);
    }

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
                           extraction->old_partition_id);

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

    ret = rmdir(mdb.extraction->dir_with_new_partition);
    RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_extract(ret=%d)", ret);
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

    ACQUIRE_RWLOCK_READ(&(mdb.rwlock_extract), "metadb_bulkinsert(%s)", dir_with_new_partition);

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

    RELEASE_RWLOCK(&(mdb.rwlock_extract), "metadb_bulkinsert(%s)", dir_with_new_partition);
    return ret;
}

void metadb_test_put_and_get(struct MetaDB mdb,
                             const metadb_inode_t dir_id,
                             const int partition_id,
                             const char *path) {
    char key[] = "test1";
    char val[] = "value1";
    char* err = NULL;

    leveldb_put(mdb.db, mdb.insert_options,
                key, strlen(key),
                val, strlen(val), &err);

    size_t new_val_len;
    char* new_val = leveldb_get(mdb.db, mdb.lookup_options,
                                key, strlen(key), &new_val_len, &err);
    printf("%s\n", new_val);

    int new_key = 12346;

    leveldb_put(mdb.db, mdb.insert_options,
                (char *) &new_key, sizeof(new_key),
                val, strlen(val), &err);

    new_val = leveldb_get(mdb.db, mdb.lookup_options,
                          (char *) &new_key, sizeof(new_key),
                          &new_val_len, &err);
    printf("%s\n", new_val);

    metadb_key_t mobj_key;
    init_meta_obj_key(&mobj_key, dir_id, partition_id, path);

    print_meta_obj_key(&mobj_key);


    char testval[] = "value2";
    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                testval, strlen(testval), &err);

    char* shit = leveldb_get(mdb.db, mdb.lookup_options,
                          (const char*) &mobj_key, METADB_KEY_LEN,
                          &new_val_len, &err);

    metadb_obj_t mobj;
    mobj.objname_len = 0;
    mobj.realpath_len = 0;
    init_meta_obj_statbuf(&mobj, 0, 0);

    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                (const char*) &mobj, metadb_obj_size(&mobj), &err);

    shit = leveldb_get(mdb.db, mdb.lookup_options,
                          (const char*) &mobj_key, METADB_KEY_LEN,
                          &new_val_len, &err);
    metadb_obj_t* shitobj = (metadb_obj_t *) shit;

    metadb_obj_t* nmobj;
    nmobj = create_metadb_obj(path, strlen(path), path, strlen(path));
    init_meta_obj_statbuf(nmobj, 0, 0);

    leveldb_put(mdb.db, mdb.insert_options,
                (const char*) &mobj_key, METADB_KEY_LEN,
                (const char*) nmobj, metadb_obj_size(nmobj), &err);

    shit = leveldb_get(mdb.db, mdb.lookup_options,
                       (const char*) &mobj_key, METADB_KEY_LEN,
                       &new_val_len, &err);
    shitobj = (metadb_obj_t *) shit;
}
