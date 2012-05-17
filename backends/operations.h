#ifndef OPERATIONS_H
#define OPERATIONS_H   

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "./leveldb/include/leveldb/c.h"

/*
 * Operations for local file system as the backend.
 */
int local_getattr(const char *path, struct stat *stbuf);
int local_symlink(const char *path, const char *link);
int local_readlink(const char *path, char *link, size_t size);
int local_open(const char *path, int flags, int *fd_to_return);
int local_mknod(const char *path, mode_t mode, dev_t dev);
int local_mkdir(const char *path, mode_t mode);

/*
 * Operations for sending RPC to backends.
 */
int rpc_init();
int rpc_getattr(int dir_id, const char *path, struct stat *statbuf);
int rpc_mkdir(int dir_id, const char *path, mode_t mode);

/*
 * LevelDB specific definitions
 */
struct MetaDB {
    leveldb_t* db;              // DB instance
    leveldb_comparator_t* cmp;  // Compartor object that allows user-defined
                                // object comparions functions.
    leveldb_cache_t* cache;     // Cache object: If enabled, this enables caching of
                                // individual blocks (of levelDB files) using LRU.
    leveldb_env_t* env;
    leveldb_options_t* options;
    leveldb_readoptions_t*  lookup_options;
    leveldb_readoptions_t*  scan_options;
    leveldb_writeoptions_t* insert_options;
};

typedef enum MetaDB_obj_type {
    OBJ_FILE,
    OBJ_DIR,
    OBJ_SLINK,
    OBJ_HLINK
} metadb_obj_type_t;

typedef uint64_t metadb_inode_t;
typedef uint64_t name_hash_t;

typedef struct MetaDB_key {
  metadb_inode_t parent_id;
  int partition_id;
  name_hash_t name_hash;
} metadb_key_t;

typedef struct MetaDB_obj {
  struct stat statbuf;
  metadb_obj_type_t obj_type;
  int objname_len;
  char* objname;
  int realpath_len;
  char* realpath;
} metadb_obj_t;

typedef void (*fill_dir_t)(void* buf, metadb_key_t* iter_key, metadb_obj_t* iter_obj);

typedef void (*iden_part_t)(const char* entry, const int partition_id);

struct MetaDB ldb_mds;

int metadb_init(struct MetaDB mdb, const char *mdb_name);

int metadb_destroy(struct MetaDB mdb, const char *mdb_name);

int metadb_create(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  metadb_obj_type_t entry_type,
                  const metadb_inode_t inode_id,
                  const char *path,
                  const char *realpath);

int metadb_remove(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path);

int metadb_lookup(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  const char *path,
                  struct stat *stbuf);

int metadb_readdir(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   void *buf, fill_dir_t filler);

/*
int metadb_extract(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   const int partition_id,
                   iden_part_t idenf,
                   const char* result_dir);
*/
#endif /* OPERATIONS_H */
