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

volatile int curr;
volatile int lastfile;
int errors;
time_t start_exp, end_exp;
const int sampling = 1;

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


static void deldir_from_list(const char *filename)
{
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
      return;
    }
    char path[1024];
    int n;
    fscanf(file, "%d", &n);
    char** paths = (char**) malloc(n * sizeof(char *));
    for (curr = 0; curr< n; ++curr) {
      fscanf(file, "%s", path);
      paths[curr] = (char*) malloc(strlen(path)+1);
      strcpy(paths[curr], path);
    }
    int count = 0;
    for (curr = n-1; curr >= 0; --curr) {
      gigaUnlink(paths[curr]);
      free(paths[curr]);
      count ++;
      if (count % 100 == 0)
        printf("%d\n", count);
    }
    free(paths);
    fclose(file);
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

    if (gigaInit() != 0) {
        printf("ERROR during gigaInit().");
        return -1;
    }

//    launch_timer_thread();

    start_exp = time(NULL);
    deldir_from_list(argv[1]);
    end_exp = time(NULL);

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    gigaDestroy();

    return 0;
}

