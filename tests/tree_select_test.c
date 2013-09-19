#include "client/libclient.h"
#include "common/measure.h"
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)

static char hostname[64];
static int pid;

typedef struct {
  int dirno;
  int min;
  int max;
} op_t;

volatile int curr;
volatile int lastfile;
int errors;
time_t start_exp, end_exp;
const int sampling = 1;
int num_dirs, num_files, num_ops;
char** dirnames;
op_t* oplist;
int seed;

void *timer_thread(void *unused)
{
    (void)unused;
    int now, ret;

    struct timespec ts;
    ts.tv_sec = sampling;
    ts.tv_nsec = 0;

    struct timespec rem;

top:
    now = curr;
    printf("%d\n", now - lastfile);
    lastfile = now;

    ret = nanosleep(&ts, &rem);
    if (ret == -1){
        if (errno == EINTR)
            nanosleep(&rem, NULL);
        else
            errors++;
    }

    if (errors > 50)
        return NULL;
    else
        goto top;
}

void read_dir_list(const char *filename) {
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
      return;
    }
    fscanf(file, "%d", &num_dirs);
    dirnames = (char **) malloc(sizeof(char*) * num_dirs);
    int i = 0;
    char path[1024];
    for (i = 0; i < num_dirs; ++i) {
      fscanf(file, "%s", path);
      dirnames[i] = (char *) malloc(sizeof(char) * strlen(path) + 1);
      strcpy(dirnames[i], path);
    }
    fclose(file);
}

void read_op_list(const char *filename) {
    FILE* file = fopen(filename, "r");
    if (file == NULL)
      return;
    fscanf(file, "%d", &num_ops);
    oplist = (op_t *) malloc(sizeof(op_t) * num_ops);
    int i = 0;
    for (i=0; i < num_ops; ++i) {
      oplist[i].dirno = i;
      fscanf(file,"%d %d", &(oplist[i].min), &(oplist[i].max));
    }
    fclose(file);
}

void pick_file(char* p) {
    int op_id = rand() % num_ops;
    sprintf(p, "%s/%d\0",
             dirnames[oplist[op_id].dirno], oplist[op_id].min);
    oplist[op_id].min ++;
    if (oplist[op_id].min == oplist[op_id].max) {
      oplist[op_id] = oplist[num_ops-1];
      --num_ops;
    }
}

static void mknod_from_list() {
    char p[512] = {0};
    start_measure();
    for (curr = 0; curr< num_files; ++curr) {
        pick_file(p);
        start_op();
        if (gigaMknod(p, CREATE_MODE) < 0) {
              printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
              return;
        }
        finish_op();
    }
    finish_measure();
}

static void getattr_from_list() {
    char p[512] = {0};
    start_measure();
    int errcount = 0;
    for (curr = 0; curr< num_files; ++curr) {
        pick_file(p);
        start_op();
        struct stat buf;
        if (gigaGetAttr(p, &buf) < 0) {
          errcount++;
        }
        finish_op();
    }
    finish_measure();
    printf("errcount:%d\n", errcount);
}
/*
static void chmod_from_list(const char *filename) {
    char p[512] = {0};
    start_measure();
    for (curr = 0; curr< num_files; ++curr) {
        pick_file(p);
        start_op();
        struct stat buf;
        if (gigaGetAttr(p, &buf) < 0) {
              printf("ERROR during getattr(%s): %s\n",
                      p, strerror(errno));
              return;
        }
        finish_op();
    }
    finish_measure();
}
*/
void launch_timer_thread() {
    pthread_t tid;
    int ret;
    if ((ret = pthread_create(&tid, NULL, timer_thread, NULL))){
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

int main(int argc, char **argv)
{
    if (argc != 6) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <type> <dirfilename> <oplist> <num_files> <seed>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    setvbuf(stdout,NULL,_IONBF,0);
    pid = (int)getpid();
    num_files = atoi(argv[4]);
    read_dir_list(argv[2]);
    read_op_list(argv[3]);
    seed = atoi(argv[5]);
    srand(seed);

    if (num_dirs <= 0) {
      printf("Not directory names listed in %s\n", argv[1]);
      return -1;
    }

    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
    }

    if (gigaInit() != 0) {
        printf("ERROR during gigaInit().");
        return -1;
    }

    launch_timer_thread();

    start_exp = time(NULL);
    if (strcmp(argv[1], "mknod") == 0)
        mknod_from_list();
    if (strcmp(argv[1], "getattr") == 0)
        getattr_from_list();
    end_exp = time(NULL);

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    gigaDestroy();

    return 0;
}

