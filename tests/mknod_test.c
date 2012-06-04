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
    
    int num_files = atoi(argv[2]);
   
    char hostname[64] = {0};
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
    }

    int pid = (int)getpid();

    printf("Creating %d files from test_%d ... \n", num_files, pid);
    int i = 0;
    for (i=0; i<num_files; i++) {
        char path[512] = {0};
        snprintf(path, sizeof(path), "%s/%s_p%d_f%d", 
                 argv[1], hostname, pid, i);
        if (mknod(path, m, d) < 0) {
            printf ("ERR_mknod(%s): %s\n", path, strerror(errno));
            return -1;
        }
    }
    
    return 0;
}

