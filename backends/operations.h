#ifndef OPERATIONS_H
#define OPERATIONS_H

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "./leveldb/include/leveldb/c.h"
#include "common/giga_index.h"

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
int rpc_mknod(int dir_ID, const char *path, mode_t mode, dev_t dev);

/*
 * LevelDB specific definitions
 */
struct MetaDB {
    leveldb_t* db;              // DB instance
    leveldb_comparator_t* cmp;  // Compartor object that allows user-defined
                                // object comparions functions.
    leveldb_cache_t* cache;     // Cache object: If set, individual blocks 
                                // (of levelDB files) are cached using LRU.
    leveldb_env_t* env;
    leveldb_options_t* options;
    leveldb_readoptions_t*  lookup_options;
    leveldb_readoptions_t*  scan_options;
    leveldb_writeoptions_t* insert_options;
};

typedef enum MetaDB_obj_type {
    OBJ_DIR,
    OBJ_FILE,
    OBJ_MKNOD,
    OBJ_SLINK,
    OBJ_HLINK
} metadb_obj_type_t;

typedef uint64_t metadb_inode_t;

typedef struct MetaDB_key {
    metadb_inode_t parent_id;
    long int partition_id;
    char name_hash[HASH_LEN];
} metadb_key_t;

typedef struct MetaDB_obj {
    int magic_number;
    metadb_obj_type_t obj_type;
    struct stat statbuf;
    size_t objname_len;
    char* objname;
    size_t realpath_len;
    char* realpath;
} metadb_obj_t;

typedef void (*fill_dir_t)(void* buf, metadb_key_t* iter_key, metadb_obj_t* iter_obj);

typedef void (*iden_part_t)(const char* entry, const int partition_id);


int metadb_init(struct MetaDB *mdb, const char *mdb_name);

int metadb_close(struct MetaDB mdb);

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

int metadb_extract(struct MetaDB mdb,
                   const metadb_inode_t dir_id,
                   const int old_partition_id,
                   const int new_partition_id,
                   const char* dir_with_new_partition,
                   uint64_t *min_sequence_number,
                   uint64_t *max_sequence_number);

int metadb_bulkinsert(struct MetaDB mdb,
                      const char* dir_with_new_partition,
                      uint64_t min_sequence_number,
                      uint64_t max_sequence_number);

void metadb_test_put_and_get(struct MetaDB mdb,
                             const metadb_inode_t dir_id,
                             const int partition_id,
                             const char *path);

#endif /* OPERATIONS_H */
