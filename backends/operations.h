
#ifndef OPERATIONS_H
#define OPERATIONS_H

#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "leveldb/include/leveldb/c.h"
#include "common/giga_index.h"
#include "common/rpc_giga.h"

/*
 * File/Directory permission bits
 * */
#define DEFAULT_MODE    (S_IRWXU | S_IRWXG | S_IRWXO )

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0

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
int rpc_create(int dir_id, const char *path, mode_t mode);
scan_list_t rpc_readdir(int dir_id, const char *path);
int rpc_opendir(int dir_id, const char *path);
int rpc_releasedir(int dir_id, const char *path);

typedef enum MetaDB_obj_type {
    OBJ_DIR,
    OBJ_FILE,
    OBJ_MKNOD,
    OBJ_SLINK,
    OBJ_HLINK
} metadb_obj_type_t;

typedef uint64_t metadb_inode_t;
typedef uint64_t mdb_seq_num_t;
typedef uint32_t readdir_rec_len_t;

typedef struct MetaDB_key {
    metadb_inode_t parent_id;
    long int partition_id;
    char name_hash[HASH_LEN];
} metadb_key_t;

typedef struct {
    struct stat statbuf;
    size_t objname_len;
    char* objname;
    size_t realpath_len;
    char* realpath;
} metadb_val_header_t;

typedef struct {
    char data;
} metadb_val_file_t;

typedef struct giga_mapping_t metadb_val_dir_t;

typedef struct {
    size_t size;
    char* value;
} metadb_val_t;

typedef struct Extract {
    metadb_inode_t dir_id;
    int old_partition_id;
    int new_partition_id;
    char dir_with_new_partition[PATH_MAX];

    leveldb_t* extract_db;
    int in_extraction;
} metadb_extract_t;

typedef struct {
    const char* buf;
    size_t buf_len;
    size_t num_ent;
    size_t offset;
    size_t cur_ent;
} metadb_readdir_iterator_t;

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
    leveldb_writeoptions_t* ext_insert_options;

    metadb_extract_t*  extraction;

    pthread_rwlock_t    rwlock_extract;
    pthread_mutex_t     mtx_bulkload;
    pthread_mutex_t     mtx_extload;
    pthread_mutex_t     mtx_leveldb;

    FILE* logfile;
};

typedef int (*update_func_t)(metadb_val_t* mval, void* arg1);

metadb_readdir_iterator_t* metadb_create_readdir_iterator(const char* buf,
        size_t buf_len, size_t num_entries);
void metadb_destroy_readdir_iterator(metadb_readdir_iterator_t *iter);
void metadb_readdir_iter_begin(metadb_readdir_iterator_t *iter);
int metadb_readdir_iter_valid(metadb_readdir_iterator_t *iter);
void metadb_readdir_iter_next(metadb_readdir_iterator_t *iter);
const char* metadb_readdir_iter_get_objname(metadb_readdir_iterator_t *iter,
                                      size_t *name_len);
const char* metadb_readdir_iter_get_realpath(metadb_readdir_iterator_t *iter,
                                       size_t *path_len);
int metadb_readdir_iter_get_stat(metadb_readdir_iterator_t *iter,
                                 struct stat *statbuf);

int metadb_init(struct MetaDB *mdb, const char *mdb_name);

int metadb_close(struct MetaDB mdb);

int metadb_create(struct MetaDB mdb,
                  const metadb_inode_t dir_id,
                  const int partition_id,
                  metadb_obj_type_t entry_type,
                  const metadb_inode_t inode_id,
                  const char *path,
                  const char *realpath);

int metadb_create_dir(struct MetaDB mdb,
                      const metadb_inode_t dir_id,
                      const int partition_id,
                      const metadb_inode_t inode_id,
                      const char *path,
                      metadb_val_dir_t* dir_mapping);

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
                   const char* start_key,
                   char* buf,
                   const size_t buf_len,
                   size_t *num_entries,
                   char* end_key,
                   int* more_entries_flag);

int metadb_extract_do(struct MetaDB mdb,
                      const metadb_inode_t dir_id,
                      const int old_partition_id,
                      const int new_partition_id,
                      const char* dir_with_new_partition,
                      uint64_t *min_sequence_number,
                      uint64_t *max_sequence_number);

int metadb_extract_clean(struct MetaDB mdb);


int metadb_bulkinsert(struct MetaDB mdb,
                      const char* dir_with_new_partition,
                      uint64_t min_sequence_number,
                      uint64_t max_sequence_number);

int metadb_read_bitmap(struct MetaDB mdb,
                       const metadb_inode_t dir_id,
                       const int partition_id,
                       const char* path,
                       struct giga_mapping_t* map_val);

int metadb_write_bitmap(struct MetaDB mdb,
                        const metadb_inode_t dir_id,
                        const int partition_id,
                        const char* path,
                        struct giga_mapping_t* map_val);

/* SVP: trying new leveldb code as is ... */
/******************************************/

struct LevelDB {
    leveldb_t* db;              // DB instance
    leveldb_comparator_t* cmp;  // Compartor object that allows user-defined
                                // object comparions functions.
    leveldb_cache_t* cache;     // Cache object: If set, individual blocks 
                                // (of levelDB files) are cached using LRU.
    leveldb_env_t* env;

    leveldb_options_t* options;

    leveldb_readoptions_t*  roptions;
    leveldb_writeoptions_t* woptions;
};

struct LevelDB ldb;

struct LevelDB_key {
    int dir_id;
    int partition_id;
    char obj_name[PATH_MAX];
};

struct LevelDB_val {
    struct stat statbuf;
    char realpath[PATH_MAX];
};


int leveldb_init(const char* ldb_name, struct LevelDB level_db);

int leveldb_create(struct LevelDB ldb,
                   const int dir_id, const int partition_id,
                   const char *obj_name, const char *link_path);

int leveldb_lookup(struct LevelDB ldb,
                   const int dir_id, const int partition_id,
                   const char *obj_name, struct stat *stbuf);


/*
void leveldb_mkdir(struct LevelDB level_db, 
                   int if_exists_flag);
int leveldb_create(struct LevelDB level_db, 
                   const char *path, mode_t mode);
int leveldb_lookup(struct LevelDB level_db, 
                   const char *path, struct stat *stbuf);
*/


#endif /* OPERATIONS_H */
