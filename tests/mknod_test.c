#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0
    
static char hostname[64];
static int num_files;
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

static void mknod_files(const char *dir)
{
    printf("Creating %d files from test_%d ... \n", num_files, pid);
    int i = 0;
    for (i=0; i<num_files; i++) {
        char path[512] = {0};
        snprintf(path, sizeof(path), "%s/%s_p%d_f%d", dir, hostname, pid, i);
        if (mknod(path, m, d) < 0) {
            printf ("mknod(%s): %s\n", path, strerror(errno));
            return;
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <dir_name> <num_files>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    mode_t m = CREATE_MODE;
    dev_t d = CREATE_RDEV;
    
    num_files = atoi(argv[2]);
    pid = (int)getpid();
   
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
    }


    mknod_files(argv[1]);

    return 0;
}

