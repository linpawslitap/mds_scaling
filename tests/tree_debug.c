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
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include "common/cache.h"

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

    struct sockaddr_in recv_addr;
    bzero(&recv_addr, sizeof(recv_addr));
    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(10601);
    int fd = -1;
    if (inet_aton("127.0.0.1", &recv_addr.sin_addr) != 0) {
      fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    }
    char message[256];

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

    if (fd > 0) {
      time_t now_time = time(NULL);
      snprintf(message, 256,
              "test_ops %ld %d\n",
              now_time, now);
      if (sendto(fd, message, strlen(message), 0, &recv_addr, sizeof(recv_addr))
          < 0) {
        printf("Sending messages failed\n");
      } else {
        printf("Sending message successfully\n");
      }
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

/*
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
    struct stat buf;
    char p[1024] = "/bmdkugyjxwaf/ibkmlegrgqgv/yuespiycvmkj/qffuuayktyzq/rydvdlgoarhr/rtwmeourvwhh/pbbpszcmfpnb/dpmtvbzgucqf/2334";
    if (gigaGetAttr(p, &buf) < 0) {
          printf("ERROR during getattr(%s): %s\n",
                  p, strerror(errno));
          return;
    }
}
*/

static void test() {
    struct timespec ts;
    ts.tv_sec = sampling;
    ts.tv_nsec = 0;

    struct timespec rem;

    for (curr = 0; curr < 1000000; ++curr) {
      if (curr % 1000 == 0) {
          nanosleep(&ts, &rem);
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

    printf("Size: %d\n", sizeof(struct giga_directory));
    printf("MAX_NUM: %d\n", MAX_NUM);
    printf("pthread_mutex_t: %d\n", sizeof(pthread_mutex_t));

    return 0;
    setvbuf(stdout,NULL,_IONBF,0);
    pid = (int)getpid();
    num_files = atoi(argv[1]);

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
    test();
    end_exp = time(NULL);

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    gigaDestroy();

    return 0;
}

