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

#define DEFAULT_MODE    (S_IRWXU | S_IRWXG | S_IRWXO )

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0

static char hostname[64];
static int num_files;
static int pid;

const int sampling = 1;

volatile int curr;
volatile int lastfile;
int errors;
int num_bytes;
time_t start_exp, end_exp;

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

    //if ((errors > 50) || (curr = num_files-1))
    if (errors > 50)
        return NULL;
    else
        goto top;
}

static void write_files(const char *dir)
{
    printf("Creating %d files from test_%d ... \n", num_files, pid);
    mode_t m = DEFAULT_MODE;
    dev_t d = CREATE_RDEV;

    for (curr=0; curr<num_files; curr++) {
        char p[512] = {0};
        snprintf(p, sizeof(p), "%s%s_p%d_f%d", dir, hostname, pid, curr);
        if (gigaMknod(p, m, d) < 0) {
            printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
            return;
        } else {
            int f = gigaOpen(p, O_WRONLY);
            const int buf_size = num_bytes;
            char buf[buf_size];
            int i = 0;
            for (i = 0; i < 1; ++i) {
                gigaWrite(f, buf, buf_size);
            }
            gigaClose(f);
        }
    }
}

static void ls_files(const char *dir)
{
    printf("Scanning files from %s ... \n", dir);
    DIR* dp;
    struct dirent *de;

    struct timeval begin;
    struct timeval end;

    if ((dp = opendir(dir)) == NULL) {
        printf("[%s] ERR_opendir: %s\n",__FILE__, strerror(errno));
        exit(1);
    }

    gettimeofday(&begin, NULL);

    int num_ent = 0;
    while (1) {
        errno = 0; // to distinguish error from End of Directory

        if ((de = readdir(dp)) == NULL) {
            //printf("err=%s\n", strerror(errno));
            break;
        }
        if ((strcmp(de->d_name, ".") == 0) ||
            (strcmp(de->d_name, "..") == 0))
            continue;

        //printf("entry=%s\n", de->d_name);
        num_ent += 1;
    }

    gettimeofday(&end, NULL);

    if (errno != 0) {
        printf("[%s] ERR_readdir: %s\n",__FILE__, strerror(errno));
        exit(1);
    }

    int microsec = (end.tv_sec - begin.tv_sec)*1000000 +
                   ((int)end.tv_usec - (int)begin.tv_usec);
    int millisec = microsec/1000;

    printf("readdir_ret=%d in %d\n", num_ent, millisec);
    closedir(dp);
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <dir_name> <num_bytes> <num_files>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    setvbuf(stdout,NULL,_IONBF,0);
    num_bytes = atoi(argv[2]) * 1024;
    num_files = atoi(argv[3]);
    pid = (int)getpid();

    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
    }

    if (gigaInit() != 0) {
        printf("ERROR during gigaInit().");
    }

    if (num_files != -1) {
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
        start_exp = time(NULL);
        write_files(argv[1]);
        end_exp = time(NULL);
    }
    else {
        ls_files(argv[1]);
    }

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));
    gigaDestroy();

    return 0;
}

