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


void init_db(struct LDB *ldb,
             const char* ip, int port,
             const char* dbname) {
    char* err = NULL;

    ldb->options = leveldb_options_create_with_hdfs_env(ip, port);
//    ldb->options = leveldb_options_create();

    leveldb_options_set_create_if_missing(ldb->options, 1);
    leveldb_options_set_info_log(ldb->options, NULL);
    leveldb_options_set_block_size(ldb->options, 1024*64);
    leveldb_options_set_compression(ldb->options, leveldb_no_compression);

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

void close_db(struct LDB *ldb) {
  leveldb_close(ldb->db);
  leveldb_options_destroy(ldb->options);
  leveldb_readoptions_destroy(ldb->lookup_options);
  leveldb_readoptions_destroy(ldb->scan_options);
  leveldb_writeoptions_destroy(ldb->insert_options);
  leveldb_writeoptions_destroy(ldb->ext_insert_options);
}

void test() {
  struct LDB ldb;
  init_db(&ldb, "localhost", 8020, "/l0/giga_ldb/l0/");

  char key[20];
  char value[20];
  sprintf(value, "hello world");
  char* err = NULL;
  int i;
  int n = 1000;

  int err_cnt = 0;
  for (i = 0; i < n; ++i) {
    sprintf(key, "%010d", i);
    size_t vallen;
    char* return_value = leveldb_get(ldb.db,
                                     ldb.lookup_options,
                                     key, 20,
                                     &vallen,
                                     &err);
    if (err != NULL) {
      err_cnt ++;
    } else
    if (strncmp(return_value, value, vallen) != 0 ||
        vallen == 0) {
      printf("key=%s\n", key);
      err_cnt ++;
    }
  }
  printf("Error count = %d\n", err_cnt);

  for (i = 0; i < n; ++i) {
    sprintf(key, "%010d", i);
    leveldb_put(ldb.db, ldb.insert_options,
                key, 20,
                value, 20,
                &err);
    if (err != NULL) {
      printf("Error: %s\n", err);
    }
  }

  err_cnt = 0;
  for (i = 0; i < n; ++i) {
    sprintf(key, "%010d", i);
    size_t vallen;
    char* return_value = leveldb_get(ldb.db,
                                     ldb.lookup_options,
                                     key, 20,
                                     &vallen,
                                     &err);
    if (err != NULL) {
      err_cnt ++;
    } else
    if (strncmp(return_value, value, vallen) != 0 ||
        vallen == 0) {
      err_cnt ++;
    }
  }
  printf("Error count = %d\n", err_cnt);

  close_db(&ldb);
}

int main() {
  test();
  return 0;
}
