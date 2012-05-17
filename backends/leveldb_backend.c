
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"

#include "operations.h"

//#include "./leveldb/include/leveldb/c.h"

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>
#include <unistd.h>

const char* phase = "";

#define CheckNoError(err)                                               \
  if ((err) != NULL) {                                                  \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, (err)); \
    abort();                                                            \
  }

#define CheckCondition(cond)                                            \
  if (!(cond)) {                                                        \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, phase, #cond); \
    abort();                                                            \
  }

static void Free(char** ptr) {
  if (*ptr) {
    free(*ptr);
    *ptr = NULL;
  }
}

int leveldb_init(struct LevelDB ldb, const char *ldb_name)
{
    int ret_val = 0;
    char *err = NULL;

    ldb.env = leveldb_create_default_env();
    ldb.cache = leveldb_cache_create_lru(100000);

    // Create and initialize the "options" object for a levelDB table
    ldb.options = leveldb_options_create();
    //leveldb_options_set_comparator(ldb.options, cmp);     //XXX: need it?
    leveldb_options_set_error_if_exists(ldb.options, 1);
    leveldb_options_set_cache(ldb.options, ldb.cache);
    leveldb_options_set_env(ldb.options, ldb.env);
    leveldb_options_set_info_log(ldb.options, NULL);
    leveldb_options_set_write_buffer_size(ldb.options, 4000000);
    leveldb_options_set_paranoid_checks(ldb.options, 1);
    leveldb_options_set_max_open_files(ldb.options, 1000);
    leveldb_options_set_block_size(ldb.options, 4096);
    leveldb_options_set_block_restart_interval(ldb.options, 16);
    leveldb_options_set_compression(ldb.options, leveldb_no_compression);

    // Create and initialize options that control real operations
    ldb.roptions = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(ldb.roptions, 0);
    leveldb_readoptions_set_fill_cache(ldb.roptions, 0);

    // Create and initialize options that control write operations
    ldb.woptions = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(ldb.woptions, 0);
    
    leveldb_options_set_create_if_missing(ldb.options, 1);
    ldb.db = leveldb_open(ldb.options, ldb_name, &err);
    if (err == NULL) {
        fprintf(stdout, "%s:%d: ERROR=[%s]\n", __FILE__, __LINE__, err); \
        ret_val = -1;
    }
    Free(&err);

    return ret_val;
}

/*
 * entry_type = {file, dir}
 */
int leveldb_create(struct LevelDB ldb, 
                   const int parent_dir_id, const int partition_id,
                   ldb_obj_type_t obj_type, 
                   const int obj_id, const char *obj_name, const char *real_path)
{
    int ret_val = 0;
    char *err = NULL;

    char key[MAX_LEN] = {0};
    char val[MAX_SIZE] = {0}; 
    size_t key_len, val_len;

    //FIXME: replace "obj_name" with hash(obj_name)
    snprintf(key, strlen(key), 
             "%d:%d:%s", parent_dir_id, partition_id, obj_name);
    key_len = strlen(key);

    switch (obj_type) {
        case OBJ_DIR:
            // FIXME: check for duplicates???
            assert(obj_id != -1);   // only dirs have an object id.
            // FIXME: what is the correct "val"? statbuf? giga+?
            snprintf(val, strlen(val), "%d:%s", obj_id, real_path);
            val_len = strlen(val);
            break;
        case OBJ_FILE:
            assert(obj_id == -1);
            snprintf(val, strlen(val), "%s", real_path);
            val_len = strlen(val);
            break;
        default:
            break;
    }
    
    leveldb_put(ldb.db, ldb.woptions, key, key_len, val, val_len, &err);
    CheckNoError(err);
    
    //XXX: create the file in the underlying file system using "val"

    return ret_val;
}


int leveldb_lookup(struct LevelDB ldb, 
                   const int parent_dir_id, const int partition_id, 
                   const char *obj_name, struct stat *stbuf)
{
    int ret_val = 0;
    char *err = NULL;

    char key[MAX_LEN] = {0};
    void *val; 
    size_t key_len, val_len;

    //FIXME: replace "obj_name" with hash(obj_name)
    snprintf(key, strlen(key), 
             "%d:%d:%s", parent_dir_id, partition_id, obj_name);
    key_len = strlen(key);

    if ((val = (void *)malloc(MAX_SIZE)) == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }
    val = leveldb_get(ldb.db, ldb.roptions, key, key_len, &val_len, &err);
    CheckNoError(err);

    //TODO: put "val" in statbuf
    stbuf = (struct stat*) val;
    
    Free(val);

    return ret_val;

}

/*
void leveldb_mkdir(struct LevelDB ldb, int if_exists_flag)
{
    char *err = NULL;
    
    char dbname[200];
    snprintf(dbname, sizeof(dbname), "/tmp/%s", 
            LEVELDB_PREFIX);
    
    leveldb_options_set_create_if_missing(ldb.options, if_exists_flag);
    ldb.db = leveldb_open(ldb.options, dbname, &err);
    CheckCondition(err != NULL);
    Free(&err);
}
*/

