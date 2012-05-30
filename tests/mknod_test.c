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
    int i = 0;
    int num_files = atoi(argv[2]);
    mode_t m = CREATE_MODE;
    dev_t d = CREATE_RDEV;

    if (argc != 3) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <dir_name> <num_files>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    printf("Creating %d files ... \n", num_files);
    for (i=0; i<num_files; i++) {
        char path[512] = {0};
        snprintf(path, sizeof(path), "%s/fd-%d", argv[1], i);
        if (mknod(path, m, d) < 0) {
            printf ("ERROR! %s\n", strerror(errno));
        }
    }
    
    return 0;
}
