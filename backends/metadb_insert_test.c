/*************************************************************************
* Author: Kai Ren
* Created Time: 2012-05-30 15:03:58
* File Name: ./metadb_insert_test.c
* Description:
 ************************************************************************/
#include "operations.h"
#include <time.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 10000
#define FILE_FORMAT "fd%d"
#define DIR_FORMAT "d%d"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stdout, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

int num_small_entries;
int num_test;

static
void myreaddir(struct MetaDB mdb,
               metadb_inode_t dir_id) {
    char* buf = (char *) malloc(MAX_BUF_SIZE);
    char end_key[128];
    char* start_key = NULL;
    int num_ent = 0;
    int more_entries_flag = 0;
    int real_partition_id = -1;
    do {
        int ret = metadb_readdir(mdb, dir_id, &real_partition_id,
                              start_key, buf, MAX_BUF_SIZE,
                              &num_ent, end_key, &more_entries_flag);
        ASSERT(ret == 0);
        metadb_readdir_iterator_t* iter =
            metadb_create_readdir_iterator(buf, MAX_BUF_SIZE, num_ent);
        metadb_readdir_iter_begin(iter);
        while (metadb_readdir_iter_valid(iter)) {
            size_t len;
            const char* objname =
                metadb_readdir_iter_get_objname(iter, &len);
            const char* realpath =
                metadb_readdir_iter_get_realpath(iter, &len);
            struct stat statbuf;
                metadb_readdir_iter_get_stat(iter, &statbuf);
            ASSERT(memcmp(objname, realpath, len) == 0);
            metadb_readdir_iter_next(iter);
        }
        metadb_destroy_readdir_iterator(iter);
        start_key = end_key;
    } while (more_entries_flag != 0);
    free(buf);
}

int drop_buffer_cache() {
    ASSERT(system("sudo su - -c 'sync'") == 0);
    ASSERT(system("sudo su - -c 'echo 3 > /proc/sys/vm/drop_caches'") == 0);
    return 0;
}

int clean_db(char* dbname) {
    (void) dbname;
    ASSERT(system("rm -rf /l0/giga_ldb") == 0);
    ASSERT(system("mkdir /l0/giga_ldb") == 0);
    return 0;
}

int64_t timespecDiff(struct timespec *timeA_p, struct timespec *timeB_p)
{
    return ((timeA_p->tv_sec * 1000000000) + timeA_p->tv_nsec) -
           ((timeB_p->tv_sec * 1000000000) + timeB_p->tv_nsec);
}

void create_dir_with_entries(struct MetaDB mdb, int dir_id, int num_entries) {
    char filename[MAX_FILENAME_LEN];

    metadb_val_dir_t dir_map;
    snprintf(filename, MAX_FILENAME_LEN, DIR_FORMAT, dir_id);
    ASSERT(metadb_create_dir(mdb, 0, 0, filename, &dir_map) == 0);

    int i;
    struct stat statbuf;
    int partition_id = 0;
    for (i = 0; i < num_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, (int) i);

        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        ASSERT(metadb_create(mdb, dir_id, partition_id,
                             filename, filename) == 0);
        ASSERT(metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf) == 0);
    }
}

void get_metric(struct MetaDB mdb) {
  char* compact = metadb_get_metric(mdb);
  if (compact != NULL) {
    printf("%s\n", compact);
    free(compact);
  }

  FILE* f=fopen("/proc/diskstats", "r");
  if (f == NULL) {
    return;
  }
  char device[20];
  long metric[11];
  while (!feof(f)) {
    if (fscanf(f, "%s %s %s", device, device, device) < 3) {
      break;
    }
    if (strcmp(device, "sda4") == 0) {
      if (fscanf(f, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",
                 &metric[0], &metric[1], &metric[2], &metric[3],
                 &metric[4], &metric[5], &metric[6], &metric[7],
                 &metric[8], &metric[9], &metric[10]) < 11) {
          break;
      }
    } else {
      char c;
      do {
        c = getc(f);
      } while (c != '\n' && c != EOF);
    }
  }
  fclose(f);
  printf("sda4: %ld", time(NULL));
  int i;
  for (i = 0; i < 11; ++i) {
    printf(" %ld", metric[i]);
  }
  printf("\n");
}

void run_test_mix(int nargs, char* args[], int num_small, int num_big, int small_size)
{
    if (nargs < 2) {
        return;
    }

    printf("Test %d %d Start: %ld\n", num_small, num_big, time(NULL));
    ++num_test;
    printf("Test %d:\n", num_test);

    struct timespec start, end;
    char* dbname = args[1];

    clean_db(dbname);
    drop_buffer_cache();

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);

    clock_gettime(CLOCK_MONOTONIC, &start);
    int i;
    const int num_big_entries = 1000000;
    for (i = 0; i < num_big; ++i) {
      create_dir_with_entries(mdb, i, num_big_entries);
    }
    for (i = 0; i < num_small; ++i) {
      create_dir_with_entries(mdb, i + num_big, small_size);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t timeElapsed = timespecDiff(&end, &start);
    printf("Insertion %d: %06lf s\n", num_test, (double) timeElapsed / 1e9);

    metadb_close(mdb);
    drop_buffer_cache();

    metadb_init(&mdb, dbname);
    clock_gettime(CLOCK_MONOTONIC, &start);
    get_metric(mdb);
    myreaddir(mdb, 0 + num_big);
    clock_gettime(CLOCK_MONOTONIC, &end);
    get_metric(mdb);
    metadb_close(mdb);
    timeElapsed = timespecDiff(&end, &start);

    printf("Scan %d: %06lf s\n", num_test, (double) timeElapsed / 1e9);
    drop_buffer_cache();
    printf("Test %d %d End: %ld\n", num_small, num_big, time(NULL));
}

int main(int nargs, char* args[]) {
    if (nargs < 2) {
      return 1;
    }

    num_test = 0;

    run_test_mix(nargs, args, 1, 0, 100);
    run_test_mix(nargs, args, 40, 1, 100);
    run_test_mix(nargs, args, 4000, 1, 100);
    run_test_mix(nargs, args, 4000, 10, 100);
    run_test_mix(nargs, args, 40000, 10, 100);

    run_test_mix(nargs, args, 1, 0, 100);
    run_test_mix(nargs, args, 8, 1, 500);
    run_test_mix(nargs, args, 80, 1, 500);
    run_test_mix(nargs, args, 80, 10, 500);
    run_test_mix(nargs, args, 8000, 10, 500);

    run_test_mix(nargs, args, 1, 0, 4000);
    run_test_mix(nargs, args, 1, 1, 4000);
    run_test_mix(nargs, args, 100, 1, 4000);
    run_test_mix(nargs, args, 100, 10, 4000);
    run_test_mix(nargs, args, 1000, 10, 4000);

    run_test_mix(nargs, args, 1, 0, 16000);
    run_test_mix(nargs, args, 1, 1, 16000);
    run_test_mix(nargs, args, 25, 1, 16000);
    run_test_mix(nargs, args, 25, 10, 16000);
    run_test_mix(nargs, args, 250, 10, 16000);

    return 0;
}
