/*************************************************************************
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
#include <errno.h>

#define DEFAULT_MEASURE_INTERVAL 1
#define MAX_FILENAME_LEN 1024
#define NUM_BIG_ENTRIES 1000000
#define FILE_FORMAT "fd%d"
#define DIR_FORMAT "d%d"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }
char* split_dir_path;

int64_t timespecDiff(struct timespec *timeA_p, struct timespec *timeB_p)
{
    return ((timeA_p->tv_sec * 1000000000) + timeA_p->tv_nsec) -
           ((timeB_p->tv_sec * 1000000000) + timeB_p->tv_nsec);
}

uint32_t randpick(uint32_t *s) {
  uint32_t seed_ = *s;
  uint32_t M = 2147483647L;
  uint64_t A = 16807;
  uint64_t product = seed_ * A;
  seed_ = (uint32_t) ((product >> 31) + (product & M));
  if (seed_ > M) {
    seed_ -= M;
  }
  *s = seed_;
  return seed_;
}

typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cv;
  struct MetaDB mdb;
  int start_threshold;
  int dir_id;
  int partition_id;
  int split_threshold;
  int num_done;
  int num_entries;
  int num_initialized;
  int start;
  int finish;
} shared_state_t;

typedef struct {
  pthread_t ptid;
  int tid;
} thread_state_t;

typedef void (*operation_ptr)(thread_state_t*, shared_state_t*);

typedef struct {
  thread_state_t* thread;
  shared_state_t* shared;
  operation_ptr ops_func;
} thread_arg_t;

void* thread_body(void * v) {
  thread_arg_t* arg = (thread_arg_t *) v;
  thread_state_t* thread = arg->thread;
  shared_state_t* shared = arg->shared;
  {
    pthread_mutex_lock(&(shared->mutex));
    shared->num_initialized++;
    if (shared->num_initialized >= shared->start_threshold) {
      pthread_cond_broadcast(&(shared->cv));
    }
    while (shared->start == 0) {
      pthread_cond_wait(&(shared->cv), &(shared->mutex));
    }
    pthread_mutex_unlock(&(shared->mutex));
  }

  (*arg->ops_func)(thread, shared);

  {
    pthread_mutex_lock(&(shared->mutex));
    shared->num_done ++;
    if (shared->num_done == shared->start_threshold) {
      pthread_cond_broadcast(&(shared->cv));
    }
    pthread_mutex_unlock(&(shared->mutex));
  }

  return NULL;
}

void create_entry(thread_state_t* thread, shared_state_t* shared) {
    int i;
    char filename[MAX_FILENAME_LEN];

    int parent_index = 0;
    int child_index = 1;
    mdb_seq_num_t min, max = 0;
    (void) thread->tid;

    for (i=0; i < shared->num_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, (int) i);

        ASSERT(metadb_create(shared->mdb,
                             shared->dir_id,
                             shared->partition_id,
                             filename, filename) == 0);

        if (i % shared->split_threshold == 0) {
            metadb_extract_do(shared->mdb,
                              shared->dir_id,
                              parent_index,
                              child_index,
                              split_dir_path,
                              &min, &max);
            printf("extracting %s\n", split_dir_path);
            metadb_extract_clean(shared->mdb);
            child_index = child_index * 2;
        }
    }
    pthread_mutex_lock(&(shared->mutex));
    shared->finish = 1;
    pthread_mutex_unlock(&(shared->mutex));
}

void lookup_entry(thread_state_t* thread, shared_state_t* shared) {
  char filename[MAX_FILENAME_LEN];
  struct stat statbuf;
  int j, finish;
  (void) thread;
  do {
    memset(filename, 0, sizeof(filename));
    j = rand() % shared->num_entries;
    snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, (int) j);
    metadb_lookup(shared->mdb,
                  shared->dir_id,
                  0,
                  filename,
                  &statbuf);
    pthread_mutex_lock(&(shared->mutex));
    finish = shared->finish;
    pthread_mutex_unlock(&(shared->mutex));
  } while (finish == 0);
}

void run_mix(struct MetaDB mdb, int num_entries, int n) {
  shared_state_t shared;
  shared.num_entries = num_entries;
  shared.dir_id = 0;
  shared.partition_id = 0;
  shared.split_threshold = 8000;
  shared.num_initialized = 0;
  shared.start_threshold = n+1;
  shared.start = 0;
  shared.finish = 0;
  shared.num_done = 0;
  shared.mdb = mdb;
  pthread_mutex_init(&(shared.mutex), NULL);
  pthread_cond_init(&(shared.cv), NULL);

  thread_arg_t* arg = (thread_arg_t*) malloc(sizeof(thread_arg_t) * (n+1));
  int i;
  for (i = 0; i <= n; i++) {
    arg[i].shared = &shared;
    arg[i].thread = (thread_state_t*) malloc(sizeof(thread_state_t));
    arg[i].thread->tid = i;
    if (i < n-1) {
      arg[i].ops_func = lookup_entry;
    } else {
      arg[i].ops_func = create_entry;
    }
    pthread_create(&(arg[i].thread->ptid), NULL, thread_body, &arg[i]);
    ASSERT(pthread_detach(arg[i].thread->ptid) == 0);
  }

  {
    pthread_mutex_lock(&(shared.mutex));
    while (shared.num_initialized <= n) {
      pthread_cond_wait(&(shared.cv), &(shared.mutex));
    }
    shared.start = 1;
    pthread_cond_broadcast(&(shared.cv));
    ASSERT(pthread_mutex_unlock(&(shared.mutex)) == 0);
  }

  pthread_mutex_lock(&(shared.mutex));
  while (shared.num_done <= n) {
    pthread_cond_wait(&(shared.cv), &(shared.mutex));
  }
  ASSERT(pthread_mutex_unlock(&(shared.mutex)) == 0);

  for (i = 0; i <= n; i++) {
    free(arg[i].thread);
  }
  free(arg);
  pthread_mutex_destroy(&(shared.mutex));
  pthread_cond_destroy(&(shared.cv));
}

void run_test_mix(int nargs, char* args[]) {
    if (nargs < 2)
      return;
    char* dbname = args[1];
    split_dir_path = args[2];

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);
    run_mix(mdb, 56000, 8);
    metadb_close(mdb);
}

int main(int nargs, char* args[]) {
    if (nargs < 2) {
      return 1;
    }

    run_test_mix(nargs, args);
    return 0;
}
