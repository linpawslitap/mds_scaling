#include "client/libclient.h"
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

#include <signal.h>
#include <time.h>
#include <pthread.h>

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)

static char hostname[64];
static int pid;

volatile int curr;
volatile int lastfile;
int errors;
time_t start_exp, end_exp;
const int sampling = 1;
int num_dirs, num_files;
char** dirnames;

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
    dir_names = (char *) malloc(sizeof(char*) * n);
    int i = 0;
    char path[1024];
    for (i = 0; i < num_dirs; ++i) {
      fscanf(file, path);
      dir_names[i] = (char *) malloc(sizeof(char) * strlen(path) + 1);
      strcpy(dir_names[i], path);
    }
}

static void mkdir_from_list(const char *filename) {
    for (curr = 0; curr< num_files; ++curr) {
        int dir_id = rand() % num_dirs;
        char p[512] = {0};
        snprintf(p, sizeof(p), "%s%s_f%d",
                 dir_names[dir_id], hostname, curr);
        if (gigaMknod(p, CREATE_MODE) < 0) {
              printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
              return;
        }
    }
}

static void getattr_from_list(const char *filename) {
    for (curr = 0; curr< num_files; ++curr) {
        int dir_id = rand() % num_dirs;
        char p[512] = {0};
        snprintf(p, sizeof(p), "%s%s_p%d_f%d",
                 dir_names[dir_id], hostname, pid, curr);
        struct stat buf;
        if (gigaGetAttr(p, &buf) < 0) {
              printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
              return;
        }
    }
}

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
    if (argc != 2) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <filename>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    pid = (int)getpid();

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
    mkdir_from_list(argv[1]);
    end_exp = time(NULL);

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    gigaDestroy();

    return 0;
}

