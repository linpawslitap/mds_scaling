#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"

#include "operations.h"

#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define DEFAULT_LEVELDB_CACHE_SIZE 100000
#define DEFAULT_WRITE_BUFFER_SIZE  100000
#define DEFAULT_MAX_OPEN_FILES     128

#define METADB_KEY_LEN (sizeof(metadb_key_t))

#define metadb_error(phase, cond)                                        \
  if (cond != NULL) {                                                         \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, cond);\
    abort();                                                             \
  }

int metadb_init(struct MetaDB *mdb, const char *mdb_name) {
  char* err = NULL;

  mdb->env = leveldb_create_default_env();
  mdb->cache = leveldb_cache_create_lru(DEFAULT_LEVELDB_CACHE_SIZE);

  mdb->options = leveldb_options_create();
  leveldb_options_set_cache(mdb->options, mdb->cache);
  leveldb_options_set_env(mdb->options, mdb->env);
  leveldb_options_set_create_if_missing(mdb->options, 1);
  leveldb_options_set_info_log(mdb->options, NULL);
  leveldb_options_set_write_buffer_size(mdb->options, DEFAULT_WRITE_BUFFER_SIZE);
  leveldb_options_set_max_open_files(mdb->options, DEFAULT_MAX_OPEN_FILES);
  leveldb_options_set_block_size(mdb->options, 1024);
  leveldb_options_set_compression(mdb->options, leveldb_no_compression);

  mdb->lookup_options = leveldb_readoptions_create();
  leveldb_readoptions_set_fill_cache(mdb->lookup_options, 1);

  mdb->scan_options = leveldb_readoptions_create();
  leveldb_readoptions_set_fill_cache(mdb->scan_options, 1);

  mdb->insert_options = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(mdb->insert_options, 1);

  mdb->db = leveldb_open(mdb->options, mdb_name, &err);
  metadb_error("leveldb_init", err);

  return 0;
}

int metadb_close(struct MetaDB mdb) {
  leveldb_close(mdb.db);
  leveldb_options_destroy(mdb.options);
  leveldb_cache_destroy(mdb.cache);
  leveldb_env_destroy(mdb.env);
  leveldb_readoptions_destroy(mdb.lookup_options);
  leveldb_readoptions_destroy(mdb.scan_options);
  leveldb_writeoptions_destroy(mdb.insert_options);
  return 0;
}

static metadb_obj_t* create_metadb_obj(const char* objname,
                                       const size_t objname_len,
                                       const char* realpath,
                                       const size_t realpath_len) {
  metadb_obj_t *mobj;
  mobj = (metadb_obj_t *) malloc(sizeof(metadb_obj_t)
                                 + realpath_len + 1
                                 + objname_len + 1);
  if (mobj != NULL) {
    mobj->objname_len = objname_len;
    mobj->objname = (char *) mobj + sizeof(metadb_obj_t);
    strncpy(mobj->objname, objname, objname_len);
    mobj->realpath = (char *) mobj+sizeof(metadb_obj_t)+objname_len+1;
    strncpy(mobj->realpath, realpath, realpath_len);
    mobj->objname[objname_len] = '\0';
  }
  return mobj;
}

static void init_meta_obj(metadb_obj_t* mobj,
                          const int inode_id,
                          mode_t mode,
                          metadb_obj_type_t obj_type) {
  mobj->obj_type = obj_type;
  mobj->statbuf.st_ino  = inode_id;
  mobj->statbuf.st_mode = mode;
  mobj->statbuf.st_uid  = 1000;
  mobj->statbuf.st_gid  = 1000;
  mobj->statbuf.st_size = 0;
  if (obj_type == OBJ_DIR) {
    mobj->statbuf.st_nlink   = 2;
  } else {
    mobj->statbuf.st_nlink   = 1;
  }
  time_t now = time(NULL);
  mobj->statbuf.st_atime = now;
  mobj->statbuf.st_mtime = now;
  mobj->statbuf.st_ctime = now;
}

#define meta_obj_size(mobj) ((mobj.objname_len+1)+(mobj.realpath_len+1)+sizeof(metadb_obj_t))

static void init_meta_obj_key(metadb_key_t *mkey,
                              int dir_id,
                              int partition_id,
                              const char* path) {
  mkey->parent_id = dir_id;
  mkey->partition_id = partition_id;
  giga_hash_name(path, mkey->name_hash);
}

static void safe_free(char** ptr) {
  if (*ptr) {
    free(*ptr);
    *ptr = NULL;
  }
}

int metadb_create(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  metadb_obj_type_t entry_type,
                  const metadb_inode_t inode_id,
                  const char *path,
                  const char *realpath) {
  metadb_key_t mobj_key;
  metadb_obj_t* mobj;
  char* err = NULL;

  init_meta_obj_key(&mobj_key, dir_id, partition_id, path);
  mobj = create_metadb_obj(path, strlen(path),
                           realpath, strlen(realpath));
  init_meta_obj(mobj, inode_id, 0660, entry_type);

  leveldb_put(mdb.db, mdb.insert_options,
              (char *) &mobj_key, sizeof(metadb_key_t),
              (char *) &mobj, METADB_KEY_LEN,
              &err);

  safe_free((char **) (&mobj));

  if (err != NULL)
      return -1;

  return 0;
}

int metadb_lookup(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path,
                  struct stat *stbuf) {
  metadb_key_t mobj_key;
  metadb_obj_t* mobj;
  char* err = NULL;
  size_t val_len;
  char* val;

  init_meta_obj_key(&mobj_key, dir_id, partition_id, path);
  val = leveldb_get(mdb.db, mdb.lookup_options,
                    (char *) &mobj_key, METADB_KEY_LEN,
                    &val_len, &err);

  if (err == NULL && val != NULL) {
    mobj = (metadb_obj_t *) val;
    *stbuf = mobj->statbuf;
    safe_free(&val);
    return 0;
  } else {
    return ENOENT;
  }
}

int metadb_remove(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path) {
  metadb_key_t mobj_key;
  char* err = NULL;

  init_meta_obj_key(&mobj_key, dir_id, partition_id, path);
  leveldb_delete(mdb.db, mdb.insert_options,
                (char *) &mobj_key, METADB_KEY_LEN,
                &err);
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
  metadb_key_t mobj_key;

  init_meta_obj_key(&mobj_key, dir_id, partition_id, 0);
  leveldb_iterator_t* iter = leveldb_create_iterator(mdb.db, mdb.scan_options);
  if (leveldb_iter_valid(iter)) {
    leveldb_iter_seek(iter, (char *) &mobj_key, METADB_KEY_LEN);
    while (leveldb_iter_valid(iter)) {
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
    }
    return 0;
  } else {
    return ENOENT;
  }
}

/*
int metadb_extract(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   iden_part_t idenf,
                   const char* result_dir) {
  metadb_key_t mobj_key;
  init_meta_obj_key(&mobj_key, dir_id, partition_id, 0);

  return 0;
}
*/
