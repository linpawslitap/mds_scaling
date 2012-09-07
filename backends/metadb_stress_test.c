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
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 10000
#define FILE_FORMAT "fd%d-%d"
#define DIR_FORMAT "d%d"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

#define DEFAULT_BLOCK_SIZE (128 << 20)

int num_small_entries;
int num_test;

/*
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
} */

int drop_buffer_cache() {
    ASSERT(system("sudo su - -c 'sync'") == 0);
    ASSERT(system("sudo su - -c 'echo 3 > /proc/sys/vm/drop_caches'") == 0);
    return 0;
}

int clean_db(char* dbname) {
    (void) dbname;
    char cmd[100];
    sprintf(cmd, "rm -rf %s", dbname);
//    ASSERT(system("rm -rf /tmp/giga_ldb") == 0);
    ASSERT(system(cmd) == 0);
    sprintf(cmd, "mkdir %s", dbname);
//    ASSERT(system("mkdir /tmp/giga_ldb") == 0);
    ASSERT(system(cmd) == 0);
    return 0;
}

int64_t timespecDiff(struct timespec *timeA_p, struct timespec *timeB_p)
{
    return ((timeA_p->tv_sec * 1000000000) + timeA_p->tv_nsec) -
           ((timeB_p->tv_sec * 1000000000) + timeB_p->tv_nsec);
}


const int sampling = 1;

volatile int count_files = 0;
int errors;

void *timer_thread(void *unused)
{
    struct MetaDB* mdb = (struct MetaDB*) unused;
    int ret;

    struct timespec ts;
    ts.tv_sec = sampling;
    ts.tv_nsec = 0;

    struct timespec rem;

top:
    printf("%d\n", count_files);
    if (metadb_valid(*mdb) > 0) {
      char* result = metadb_get_metric(*mdb);
      printf("%s\n", result);
      free(result);
    }

    ret = nanosleep(&ts, &rem);
    if (ret == -1){
        if (errno == EINTR)
            nanosleep(&rem, NULL);
        else
            errors++;
    }

    //if ((errors > 50) || (curr = num_files-1))
    if (errors > 50)
        return NULL;
    else
        goto top;
}

typedef struct Locker {
  char** blocks;
  size_t num_blocks;
  size_t current_block;
  size_t remaining_bytes;
} locker_t;

void locker_lock_memory(locker_t *locker, size_t lock_size) {
  size_t i;
  if (lock_size > 0) {
    locker->num_blocks = (lock_size*1024*1024 - 1) / DEFAULT_BLOCK_SIZE + 1;
    locker->blocks = (char**) malloc(sizeof(char*) * (locker->num_blocks));
    for (i = 0; i < locker->num_blocks; ++i) {
      void* page_start;
      int ret;
      if ((ret = posix_memalign(&page_start,sysconf(_SC_PAGESIZE),
                                DEFAULT_BLOCK_SIZE)) != 0) {
        printf("memory allocation failed %.0f MB\n",
               lock_size/1.0);
        exit(1);
      }
      if (mlock(page_start, DEFAULT_BLOCK_SIZE) != 0) {
        perror("memory locking failed");
        exit(1);
      }
      locker->blocks[i] = (char*) page_start;
    }
  } else {
    locker->blocks = NULL;
    locker->num_blocks = 0;
  }
  locker->current_block = 0;
  locker->remaining_bytes = DEFAULT_BLOCK_SIZE;
}

void locker_release_lock_memory(locker_t *locker) {
  size_t i;
  for (i = 0; i < locker->num_blocks; ++i) {
    free(locker->blocks[i]);
  }
}

void* locker_allocate(locker_t *locker, size_t bytes) {
  if (bytes > DEFAULT_BLOCK_SIZE) {
    fprintf(stderr, "ask for too much memory\n");
    exit(1);
  }
  if (locker->remaining_bytes < bytes) {
    if (locker->current_block + 1 < locker->num_blocks) {
      locker->current_block ++;
      locker->remaining_bytes = DEFAULT_BLOCK_SIZE;
    } else {
      fprintf(stderr, "fail to allocate memory\n");
      exit(1);
    }
  }
  char* alloc_addr = locker->blocks[locker->current_block]+
                     (DEFAULT_BLOCK_SIZE - locker->remaining_bytes);
  locker->remaining_bytes -= bytes;
  return (void *) alloc_addr;
}

typedef struct TraceLoader {
  int num_paths;
  int num_file_paths;
  int num_dir_paths;
  locker_t locker;
  char mem_lock_flag;
  char** paths;
} trace_loader_t;

void load_trace(trace_loader_t *loader, const char* filename, int mem_lock_size) {
  if (mem_lock_size > 0) {
    locker_lock_memory(&(loader->locker), mem_lock_size);
    loader->mem_lock_flag = 1;
  } else {
    loader->mem_lock_flag = 0;
  }
  FILE* file = fopen(filename, "r");
  if (file == NULL) {
      fprintf(stderr, "fail to open trace file\n");
      exit(1);
  }
  if (fscanf(file, "%d %d", &loader->num_paths, &loader->num_file_paths) < 2) {
      fprintf(stderr, "trace loader file format error\n");
      exit(1);
  }
  loader->num_dir_paths = loader->num_paths - loader->num_file_paths;
  if (loader->mem_lock_flag) {
    loader->paths = (char**) locker_allocate(&(loader->locker), loader->num_paths * sizeof(char*));
  } else {
    loader->paths = (char**) malloc(loader->num_paths * sizeof(char*));
  }
  char pathname[4096];
  int i = 0, j;
  for (j = 0; j < loader->num_paths; ++j) {
    char type;
    if (fscanf(file, "%s %c", pathname, &type) < 1) {
      fprintf(stderr, "trace loader file format error\n");
      exit(1);
    }
    if (loader->mem_lock_flag) {
      loader->paths[i] = (char*) locker_allocate(&(loader->locker), strlen(pathname)+1);
    } else {
      loader->paths[i] = (char*) malloc(strlen(pathname)+1);
    }
    strncpy(loader->paths[i], pathname, strlen(pathname)+1);
    ++i;
  }
}

void destroy_trace_loader(trace_loader_t *loader) {
  if (loader->mem_lock_flag) {
     locker_release_lock_memory(&(loader->locker));
  } else {
    int j;
    for (j = 0; j < loader->num_paths; ++j) {
      free(loader->paths[j]);
    }
    free(loader->paths);
  }
}

void run_create(struct MetaDB mdb, trace_loader_t* loader, int num_entries) {
    int i;
    struct stat statbuf;
    int dir_id = 0;
    int partition_id = 0;
    printf("Run create\n");
    for (i = 0; i < num_entries; ++i) {
        ++count_files;
        char* filename = loader->paths[i];
        ASSERT(metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf) != 0);
        ASSERT(metadb_create(mdb, dir_id, partition_id,
                             filename, filename) == 0);
        ASSERT(metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf) == 0);
    }
}

void run_query(struct MetaDB mdb, trace_loader_t* loader, int num_queries, int read_ratio, int range) {
    int i;
    struct stat statbuf;
    int dir_id = 0;
    int partition_id = 0;
    printf("Run query\n");
    for (i = 0; i < num_queries; ++i) {
        ++count_files;
        char* filename = loader->paths[rand() % range];
        int qi = rand() % 100;
        if (qi < read_ratio) {
          ASSERT(metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf) == 0);
        } else {
          mode_t new_val = 2;
          ASSERT(metadb_chmod(mdb, dir_id, partition_id, filename, new_val) == 0);
        }
    }

}

void enable_monitor_thread(struct MetaDB* mdb) {
    pthread_t tid;
    int ret;
    if ((ret = pthread_create(&tid, NULL, timer_thread, mdb))){
        fprintf(stderr, "pthread_create() error: %d\n",
                ret);
        exit(1);
    }
    if ((ret = pthread_detach(tid))){
        fprintf(stderr, "pthread_detach() error: %d\n",
                ret);
        exit(1);
    }
}

void disable_monitor_thread() {
    errors = 100;
}

void run_test(int nargs, char* args[]) {
    if (nargs < 2) {
        return;
    }
    struct timespec start, end;
    char* dbname = args[1];
    drop_buffer_cache();

    trace_loader_t loader;
    load_trace(&loader, args[2], atoi(args[3]));

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);

    enable_monitor_thread(&mdb);
    clock_gettime(CLOCK_MONOTONIC, &start);

    if (strcmp(args[4], "create") == 0) {
      run_create(mdb, &loader, atoi(args[5]));
    } else {
      run_query(mdb, &loader, atoi(args[5]), atoi(args[6]), atoi(args[7]));
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    disable_monitor_thread();
    metadb_close(mdb);

    uint64_t timeElapsed = timespecDiff(&end, &start);
    timeElapsed = timespecDiff(&end, &start);
<<<<<<< HEAD
    printf("%s %d: %lld ns\n", args[4], num_test, timeElapsed);
=======
    printf("%s %d: %.1f ns\n", args[4], num_test, timeElapsed/1.0);
>>>>>>> e2f7e661e3caa64e298284d917e720e7ad0d5177

    destroy_trace_loader(&loader);
    drop_buffer_cache();
}


int main(int nargs, char* args[]) {
    if (nargs < 4) {
      return 1;
    }

    run_test(nargs, args);

    return 0;
}
