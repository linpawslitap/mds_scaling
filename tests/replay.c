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
  char op[10];
  char* path;
  char* target;
} op_t;

volatile int tot_curr;
volatile int lastfile;
int errors;
time_t start_exp, end_exp;
const int sampling = 1;
int num_ops, num_threads;
op_t* oplist;

pthread_mutex_t mutex_pick_file = PTHREAD_MUTEX_INITIALIZER;

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
    now = tot_curr;
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

void read_op_list(const char *filename) {
    FILE* file = fopen(filename, "r");
    if (file == NULL)
      return;
    char path[4096];
    oplist = (op_t *) malloc(sizeof(op_t) * num_ops);
    int i = 0;
    for (i=0; i < num_ops; ++i) {
        fscanf(file, "%s %s", oplist[i].op, path);
        oplist[i].path = (char *) malloc(strlen(path)*sizeof(char)+1);
        strcpy(oplist[i].path, path);
        if (strcmp(oplist[i].op, "rename") == 0) {
            fscanf(file, "%s", path);
            oplist[i].target = (char *) malloc(strlen(path)*sizeof(char)+1);
            strcpy(oplist[i].target, path);
//            printf("%s %s %s\n", oplist[i].op, oplist[i].path, oplist[i].target);
        } else {
            oplist[i].target = NULL;
//            printf("%s %s\n", oplist[i].op, oplist[i].path);
        }
    }
    fclose(file);
}

void cleanup_op_list() {
    int i = 0;
    for (i = 0; i < num_ops; ++i) {
        if (oplist[i].path != NULL)
            free(oplist[i].path);
        if (oplist[i].target != NULL)
            free(oplist[i].target);
    }
    free(oplist);
}

int pick_file() {
    if (tot_curr >= num_ops) {
      return -1;
    }

    int ret = 0;
    pthread_mutex_lock(&mutex_pick_file);
    ret = tot_curr++;
    pthread_mutex_unlock(&mutex_pick_file);

    return ret;
}

static void* replay(void* arg) {
    printf("created one replay thread: %d ops\n", num_ops);
    histogram_t* hist = (histogram_t *) arg;
    int curr;
    struct stat buf;
    for (curr = 0; curr < num_ops; ++curr) {
        int op_no;
        if ((op_no = pick_file()) < 0)
          break;
        uint64_t start_time = now_micros();
        if (strcmp(oplist[op_no].op, "open") == 0) {
            gigaGetAttr(oplist[op_no].path, &buf);
        } /* else
        if (strcmp(oplist[op_no].op, "create") == 0) {
            gigaMknod(oplist[op_no].path, CREATE_MODE);
        } else
        if (strcmp(oplist[op_no].op, "mkdirs") == 0) {
            gigaMkdir(oplist[op_no].path, CREATE_MODE);
        } */else
        if (strcmp(oplist[op_no].op, "setPermission") == 0) {
            gigaChmod(oplist[op_no].path, CREATE_MODE);
        } else {
            gigaGetAttr(oplist[op_no].path, &buf);
        }
        uint64_t end_time = now_micros();
        histogram_add(hist, end_time - start_time);
    }
    return NULL;
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
    if (argc < 4) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <oplist> <num_ops> <ntid>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    setvbuf(stdout,NULL,_IONBF,0);
    pid = (int)getpid();
    num_threads = atoi(argv[3]);
    num_ops = atoi(argv[2]);
    read_op_list(argv[1]);

    if (num_ops <= 0) {
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

    histogram_t* hists=
        (histogram_t*) malloc(sizeof(histogram_t) * num_threads);
    int histi;
    for (histi = 0; histi < num_threads; ++histi)
        histogram_clear(&hists[histi]);

    pthread_t tid[num_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    tot_curr = 0;

    int i;
    for (i = 0; i < num_threads; ++i)
        if (pthread_create(&tid[i], &attr, replay, (void *)&hists[i])) {
            fprintf(stderr, "pthread_create() error: scan on s[%d]\n", i);
            exit(1);
        }

    void* status;
    pthread_attr_destroy(&attr);
    for (i = 0; i < num_threads; ++i) {
        int ret = pthread_join(tid[i], &status);
        if (ret) {
            fprintf(stderr, "pthread_join(s[%d]) error: %d\n", i, ret);
            exit(1);
        }
    }

    end_exp = time(NULL);

    histogram_t summary;
    for (histi = 0; histi < num_threads; ++histi)
        histogram_merge(&summary, &hists[histi]);
//    histogram_print(&summary);
    free(hists);
    errors = 100;
    printf("tot_time %d\n", (int)(end_exp-start_exp));

    cleanup_op_list();
    gigaDestroy();

    return 0;
}
