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
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0

static char hostname[64];
static int num_files;
static int num_stats;
static int pid;
static int num_threads;

#if 0
void* create_thread(void *arg)
{
    (void)arg;

    return 0;
}

void spawn_threads(int n)
{
    pthread_t thd[32];

    int i = 0;

    for (i=0; i<n; i++) {
        if (pthread_create(&thd[i], 0, create_thread, NULL) < 0) 
            printf("ERR_pthread_create(%d): %s", split_tid, strerror(errno));
    }

    for (i=0; i<n; i++) {
        if (pthread_detach(thd[i]) < 0)
            printf("ERR_pthread_detach(%d): %s", split_tid, strerror(errno));
    }

    return;
}
#endif

const int sampling = 1;

volatile int curr;
volatile int lastfile;
int errors;

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

static void mknod_and_stat_files(const char *dir)
{
    printf("Creating %d files from test_%d ... \n", num_files, pid);
    mode_t m = CREATE_MODE;
    dev_t d = CREATE_RDEV;
    int num_ops = num_files + num_stats;
    int num_created_files = 0;
    for (curr=0; curr<num_ops; curr++) {
        char p[512] = {0};
        if (rand()%num_ops < num_files) {
          num_created_files++;
          snprintf(p, sizeof(p), "%s/%s_p%d_f%d",
                   dir, hostname, pid, num_created_files);
          if (mknod(p, m, d) < 0) {
              printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
              return;
          }
        } else {
          int file_id = rand()%(num_created_files+1);
          snprintf(p, sizeof(p), "%s/%s_p%d_f%d",
                   dir, hostname, pid, file_id);
          struct stat statbuf;
          stat(p, &statbuf);
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <dir_name> <num_files> <num_stats>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    setvbuf(stdout,NULL,_IONBF,0);
    num_files = atoi(argv[2]);
    num_stats = atoi(argv[3]);
    pid = (int)getpid();

    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
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

        mknod_and_stat_files(argv[1]);
    }
    errors = 100;

    return 0;
}

