/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-08-19 00:27:24
* File Name: ./ldb_hdfs_test.c
* Description:
 ************************************************************************/

#include "../backends/leveldb/include/leveldb/c.h"
#include <stdio.h>

struct LDB {
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
    leveldb_writeoptions_t* sync_insert_options;
};


void init(struct LDB *ldb,
          const char* ip, int port,
          const char* dbname) {
    char* err = NULL;

    ldb->options = leveldb_options_create_with_hdfs_env(ip, port);
    leveldb_options_set_create_if_missing(ldb->options, 0);
    leveldb_options_set_info_log(ldb->options, NULL);
    leveldb_options_set_block_size(ldb->options, 1024*64);
    leveldb_options_set_compression(ldb->options, leveldb_no_compression);
    /*
    leveldb_options_set_filter_policy(ldb->options,
                        leveldb_filterpolicy_create_bloom(14));
    */
    ldb->lookup_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(ldb->lookup_options, 1);

    ldb->scan_options = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(ldb->scan_options, 1);

    ldb->insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(ldb->insert_options, 0);

    ldb->ext_insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(ldb->ext_insert_options, 0);

    ldb->sync_insert_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(ldb->sync_insert_options, 1);

    ldb->db = leveldb_open(ldb->options, dbname, &err);

    if (err != NULL) {
      printf("Error: %s\n", err);
    } else {
      printf("Initialized successfully!\n");
    }
}

void test() {
  struct LDB ldb;
  init(&ldb, "localhost", 8020, "/l0/giga_ldb/l0/");
}

int main() {
  test();
  return 0;
}
