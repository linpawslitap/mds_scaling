#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stdout, "*** ERROR: insufficient parameters ... \n\n");
        fprintf(stdout, "USAGE: %s <dir_name> <num_files>\n", argv[0]);
        fprintf(stdout, "\n");
        return -1;
    }

    struct stat statbuf;
    int num_files = atoi(argv[2]);
   
    char hostname[64] = {0};
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        printf("ERROR during gethostname(): %s", strerror(errno));
        return -1;
    }

    int pid = (int) 0;

    printf("Lookup %d files from test_%d ... \n", num_files, pid);
    int i = 0;
    for (i=0; i<num_files; i++) {
        char path[512] = {0};
        snprintf(path, sizeof(path), "%s/%s_p%d_f%d", 
                 argv[1], hostname, pid, i);
        if (stat(path, &statbuf) < 0) {
            printf ("ERR_stat(%s): %s\n", path, strerror(errno));
        }
    }
    
    return 0;
}

