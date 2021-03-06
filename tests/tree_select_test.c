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
int num_dirs, num_files, num_ops, num_clients;
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
    recv_addr.sin_port = htons(10600);
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
              "client_ops %ld %d pid=%d\n",
              now_time, now, pid);
      sendto(fd, message, strlen(message), 0, (struct sockaddr *) &recv_addr,
             sizeof(recv_addr));
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
    int type;
    fscanf(file, "%d %d", &num_ops, &type);
    printf("%d %d\n", num_ops, type);
    oplist = (op_t *) malloc(sizeof(op_t) * num_ops);
    int i = 0;
    for (i=0; i < num_ops; ++i) {
      if (type == 1) {
        oplist[i].dirno = i;
        fscanf(file,"%d %d", &(oplist[i].min), &(oplist[i].max));
      } else {
        fscanf(file,"%d %d %d",
               &(oplist[i].dirno), &(oplist[i].min), &(oplist[i].max));
      }
    }
    fclose(file);
}

int pick_file(char* p) {
    if (num_ops == 0) {
      return -1;
    }
    int op_id = rand() % num_ops;
    //int op_id = 0;
    sprintf(p, "%s/f%d\0",
             dirnames[oplist[op_id].dirno],
             oplist[op_id].min*1000+seed);
    oplist[op_id].min ++;
    if (oplist[op_id].min == oplist[op_id].max) {
      oplist[op_id] = oplist[num_ops-1];
      --num_ops;
    }
    return 0;
}

static void mknod_from_list() {
    char p[512] = {0};
    start_measure();
    for (curr = 0; curr< num_files; ++curr) {
        if (pick_file(p) != 0)
          break;
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
        if (pick_file(p) != 0)
          break;
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

static void write_files(int buf_size)
{
    printf("Creating %d files from test_%d ... \n", num_files, pid);
    start_measure();
    char buf[buf_size];
    char p[512] = {0};
    for (curr = 0; curr< num_files; ++curr) {
        if (pick_file(p) != 0)
          break;
        start_op();
        if (gigaMknod(p, CREATE_MODE) < 0) {
            printf ("ERROR during mknod(%s): %s\n", p, strerror(errno));
            return;
        } else {
            int f = gigaOpen(p, O_WRONLY);
            gigaWrite(f, buf, buf_size);
            gigaClose(f);
        }
        finish_op();
    }
    finish_measure();
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
static void readdir_from_list() {
    char p[512] = {0};
    start_measure();
    int errcount = 0;
    for (curr = 0; curr < num_files; ++curr) {
      start_op();
      int num_entries = 0;
      int dirid = rand() % num_dirs;
      gigaListStatus(dirnames[dirid], &num_entries);
      if (num_entries <= 0) {
         errcount++;
      }
      finish_op();
    }
    finish_measure();
    printf("errcount:%d\n", errcount);
}

static void circreate_from_list(int tot, int id, int numop) {
    char p[512] = {0};
    start_measure();
    int errcount = 0;
    int i, j;
    for (i = 0; i< num_dirs; ++i) {
      if (i % tot == id) {
        for (j = 0; j < numop; ++j) {
          ++curr;
          sprintf(p, "%s/%d", dirnames[i], j);
          start_op();
          gigaMknod(p, CREATE_MODE);
          finish_op();
        }
      }
    }
    finish_measure();
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
    if (argc < 7) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <type> <dirfilename> <oplist> <num_files> <seed> <tid>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    setvbuf(stdout,NULL,_IONBF,0);
    pid = (int)getpid();
    num_files = atoi(argv[4]);
    read_dir_list(argv[2]);
    if (strcmp(argv[1], "circreate") != 0)
      read_op_list(argv[3]);
    seed = atoi(argv[5])*100+atoi(argv[6]);
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
    if (strcmp(argv[1], "write") == 0)
        write_files(atoi(argv[6]));
    if (strcmp(argv[1], "readdir") == 0)
        readdir_from_list();
    if (strcmp(argv[1], "circreate") == 0) {
        num_clients = atoi(argv[6]);
        circreate_from_list(num_clients, seed, num_files);
    }
    end_exp = time(NULL);

    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    gigaDestroy();

    return 0;
}

