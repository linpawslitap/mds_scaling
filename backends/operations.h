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

struct LevelDB {
    leveldb_t* db;              // DB instance
    leveldb_comparator_t* cmp;  // Compartor object that allows user-defined 
                                // object comparions functions.
    leveldb_cache_t* cache;     // Cache object: If set, it enables caching of
                                // individual blocks (of LDB files) using LRU.
    leveldb_env_t* env;
    leveldb_options_t* options;
    leveldb_readoptions_t* roptions;
    leveldb_writeoptions_t* woptions;
};

typedef enum LevelDB_obj_type {
    OBJ_FILE,
    OBJ_DIR,
    OBJ_SLINK,
    OBJ_HLINK
} ldb_obj_type_t;

struct LevelDB ldb_mds;

int leveldb_init(struct LevelDB level_db, const char *ldb_name);
int leveldb_lookup(struct LevelDB level_db, 
                   const int parent_dir_id, const int partition_id, 
                   const char *obj_name, struct stat *stbuf);
int leveldb_create(struct LevelDB ldb, 
                   const int parent_dir_id, const int partition_id,
                   ldb_obj_type_t obj_type, const int obj_id, 
                   const char *obj_name, const char *real_path);

/*
void leveldb_mkdir(struct LevelDB level_db, int if_exists_flag);
int leveldb_create(struct LevelDB level_db, const char *path, mode_t mode);
*/

#endif /* OPERATIONS_H */
