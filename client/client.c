
#include "client.h"
#include "FUSE_operations.h"

#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/rpc_giga.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <fuse.h>
#include <fuse_opt.h>
#include <rpc/rpc.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>


/** macro to define options */
#define GIGA_OPT_KEY(t, p, v) { t, offsetof(struct giga_options, p), v }

static struct fuse_opt giga_opts[] = {
    GIGA_OPT_KEY("pvfs=%s", backend_type, 0),  // FIXME: for panfs

    FUSE_OPT_END
};

/** This tells FUSE how to do every operation */
static struct fuse_operations giga_oper = {
    .init       = GIGAinit,
    .destroy    = GIGAdestroy,
    .getattr    = GIGAgetattr,
    .mkdir      = GIGAmkdir,
    .mknod      = GIGAmknod,
    .open       = GIGAopen,
    .readlink   = GIGAreadlink,
    .symlink    = GIGAsymlink,
    .rename     = GIGArename,
    .rmdir      = GIGArmdir,
    .unlink     = GIGAunlink,
    .link       = GIGAlink,
    .chmod      = GIGAchmod,
    .chown      = GIGAchown,
    .truncate   = GIGAtruncate,
    .utime      = GIGAutime,
    .read       = GIGAread,
    .write      = GIGAwrite,
    .statfs     = GIGAstatfs,
    .flush      = GIGAflush,
    .release    = GIGArelease,
    .fsync      = GIGAfsync,
    .setxattr   = NULL,
    .getxattr   = NULL,
    .listxattr  = NULL,
    .removexattr= NULL,
    .opendir    = GIGAopendir,
    .readdir    = GIGAreaddir,
    .releasedir = GIGAreleasedir,
    .fsyncdir   = GIGAfsyncdir,
    .access     = GIGAaccess,
    .create     = NULL,
    .ftruncate  = GIGAftruncate
};

int main(int argc, char *argv[])
{
    int ret = 0;

    /*
    if (argc != 2) {
        fprintf(stdout, "***ERROR*** insufficient number of arguments.\n");
        fprintf(stdout, "USAGE:: %s mount_point \n", argv[0]);
        exit(1);
    }
    */

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    // initialize logging
    char log_file[PATH_MAX] = {0};
    snprintf(log_file, sizeof(log_file),
             "%s.c.%d", DEFAULT_LOG_FILE_PATH, (int)getpid());
    if ((ret = logOpen(log_file, DEFAULT_LOG_LEVEL)) < 0) {
        fprintf(stdout, "***ERROR*** during opening log(%s) : [%s]\n",
                log_file, strerror(ret));
        return ret;
    }

    memset(&giga_options_t, 0, sizeof(struct giga_options));
    //initGIGAsetting(GIGA_CLIENT, argv[1], CONFIG_FILE);
    initGIGAsetting(GIGA_CLIENT, DEFAULT_MNT, CONFIG_FILE);

    if (fuse_opt_parse(&args, &giga_options_t, giga_opts, NULL) == -1)
        return -1;

    fuse_opt_insert_arg(&args, 1, "-odirect_io");
    fuse_opt_insert_arg(&args, 1, "-oattr_timeout=0");
    fuse_opt_insert_arg(&args, 1, "-omax_write=524288");
    if ( getpid() == 0 )
        fuse_opt_insert_arg( &args, 1, "-oallow_other" );
    fuse_opt_insert_arg(&args, 1, "-s");

    ret = fuse_main(args.argc, args.argv, &giga_oper, NULL);

    fuse_opt_free_args(&args);

    return ret;
}
