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

#define DEFAULT_MEASURE_INTERVAL 30
#define MAX_FILENAME_LEN 1024
#define NUM_BIG_ENTRIES 1000000
#define FILE_FORMAT "fd%d"
#define DIR_FORMAT "d%d"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

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

void get_metric() {
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

typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cv;
  struct MetaDB mdb;
  int start_threshold;
  int num_initialized;
  int num_done;
  int nscan;
  int ndir;
  int start;
  int finish;
} shared_state_t;

typedef struct ThreadState thread_state_t;

struct ThreadState {
  pthread_t ptid;
  int tid;
  int range;
  uint32_t readratio;
};

typedef struct {
  thread_state_t* thread;
  shared_state_t* shared;
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

  uint32_t randseed = thread->tid;
  int base = NUM_BIG_ENTRIES;
  char filename[MAX_FILENAME_LEN];
  int finish = 0;
  do {
    int i;
    for (i = 0; i < 100; ++i) {
      uint32_t pi = randpick(&randseed);
      uint32_t di = randpick(&randseed) % thread->range;
      uint32_t fid = randpick(&randseed) % base;
      if (pi < thread->readratio) {
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, thread->tid * base + fid);
        metadb_chmod(shared->mdb, di, 0, filename, 717);
      } else {
        struct stat statbuf;
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, fid);
        metadb_lookup(shared->mdb, di, 0, filename, &statbuf);
      }
    }
    pthread_mutex_lock(&(shared->mutex));
    finish = shared->finish;
    pthread_mutex_unlock(&(shared->mutex));
  } while (finish == 0);

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

void* scan_body(void* v) {
  thread_arg_t* arg = (thread_arg_t *) v;
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

  struct timespec start, end;
  uint32_t randseed = shared->start_threshold;
  int i;
  for (i = 0; i < shared->nscan; ++i) {
    clock_gettime(CLOCK_MONOTONIC, &start);
    get_metric();
    uint32_t ri = randpick(&randseed) % shared->nscan;
    myreaddir(shared->mdb, ri + shared->ndir - shared->nscan);
    clock_gettime(CLOCK_MONOTONIC, &end);
    get_metric();
    uint64_t timeElapsed = timespecDiff(&end, &start);
    printf("Time %ld Scan: %ld\n", time(NULL), timeElapsed);

    struct timespec ts;
    ts.tv_sec = DEFAULT_MEASURE_INTERVAL;
    ts.tv_nsec = 0;
    struct timespec rem;
    int ret = nanosleep(&ts, &rem);
    if (ret == -1 && errno == EINTR) {
       printf("nanosleep ret:%d\n", ret);
       nanosleep(&rem, NULL);
    }
  }

  pthread_mutex_lock(&(shared->mutex));
  shared->finish = 1;
  shared->num_done ++;
  pthread_mutex_unlock(&(shared->mutex));

  return NULL;
}

void create_entries(char* dbname, int num_small, int num_big, int small_size)
{
    clean_db(dbname);
    drop_buffer_cache();

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);

    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    int i;
    const int num_big_entries = NUM_BIG_ENTRIES;
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
}

void run_mix(struct MetaDB mdb, int num_big, int num_small, int n, int nscan) {
  shared_state_t shared;
  shared.start_threshold = n + 1;
  shared.num_initialized = 0;
  shared.num_done = 0;
  shared.nscan = nscan;
  shared.ndir = num_big + num_small;
  shared.start = 0;
  shared.finish = 0;
  shared.mdb = mdb;
  pthread_mutex_init(&(shared.mutex), NULL);
  pthread_cond_init(&(shared.cv), NULL);

  thread_arg_t* arg = (thread_arg_t*) malloc(sizeof(thread_arg_t) * (n+1));
  int i;
  for (i = 0; i <= n; i++) {
    arg[i].shared = &shared;
    arg[i].thread = (thread_state_t*) malloc(sizeof(thread_state_t));
    arg[i].thread->tid = i;
    arg[i].thread->range = num_big + num_small - nscan;
    if (i < n) {
      pthread_create(&(arg[i].thread->ptid), NULL, thread_body, &arg[i]);
    } else {
      pthread_create(&(arg[i].thread->ptid), NULL, scan_body, &arg[i]);
    }
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

void run_test_mix(int nargs, char* args[], int num_small, int num_big, int small_size) {
    if (nargs < 2) {
        return;
    }

    printf("Test %d %d Start: %ld\n", num_small, num_big, time(NULL));
    ++num_test;

    char* dbname = args[1];

    create_entries(dbname, num_small, num_big, small_size);

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);
    run_mix(mdb, num_small, num_big, 4, 10);
    metadb_close(mdb);

    drop_buffer_cache();
    printf("Test %d %d End: %ld\n", num_small, num_big, time(NULL));
}

int main(int nargs, char* args[]) {
    if (nargs < 2) {
      return 1;
    }

    num_test = 0;

    run_test_mix(nargs, args, 320, 1, 100);
    run_test_mix(nargs, args, 4000, 1, 100);
    run_test_mix(nargs, args, 4000, 10, 100);
    run_test_mix(nargs, args, 40000, 10, 100);

    run_test_mix(nargs, args, 64, 1, 500);
    run_test_mix(nargs, args, 800, 1, 500);
    run_test_mix(nargs, args, 800, 10, 500);
    run_test_mix(nargs, args, 8000, 10, 500);

    run_test_mix(nargs, args, 40, 1, 4000);
    run_test_mix(nargs, args, 100, 1, 4000);
    run_test_mix(nargs, args, 100, 10, 4000);
    run_test_mix(nargs, args, 1000, 10, 4000);

    run_test_mix(nargs, args, 10, 1, 16000);
    run_test_mix(nargs, args, 25, 1, 16000);
    run_test_mix(nargs, args, 25, 10, 16000);
    run_test_mix(nargs, args, 250, 10, 16000);

    return 0;
}
