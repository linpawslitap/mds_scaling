#include "libclient.h"
#include "client.h"
#include "FUSE_operations.h"
#include "common/debugging.h"
#include "common/options.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include "common/uthash.h"

//TODO: fd may not be unique
/* Open files info */
typedef struct {
    int fd; //Key for hashmap
    int offset;
    struct fuse_file_info fi;
    char path[PATH_MAX];
    UT_hash_handle hh;
} giga_file_info_t;

giga_file_info_t *_open_files;

void add_giga_fi(const struct fuse_file_info *fi, const char* path) {
    giga_file_info_t *gi = (giga_file_info_t *) malloc(sizeof(giga_file_info_t));
    memset(gi, 0, sizeof(giga_file_info_t));
    gi->fd = fi->fh;
    gi->fi = *fi;
    strncpy(gi->path, path, strlen(path));
    HASH_ADD_INT(_open_files, fd, gi);
}

giga_file_info_t* get_giga_fi(int fd)
{
    giga_file_info_t *info;
    HASH_FIND_INT(_open_files, &fd, info);
    return info;
}

void del_giga_fi(int fd)
{
    giga_file_info_t *info;
    HASH_FIND_INT(_open_files, &fd, info);
    if(info != NULL)
        HASH_DEL(_open_files, info);
}

/* Begin File Operations */

/* Initialize library
 * return non-zero value on error
 */
int gigaInit()
{
  //Note: Code copied from client/client.c

  int ret = 0;

  // initialize logging
  char log_file[PATH_MAX] = {0};
  snprintf(log_file, sizeof(log_file),
       "%s.c.%d", DEFAULT_LOG_FILE_PATH, (int)getpid());
  if ((ret = logOpen(log_file, DEFAULT_LOG_LEVEL)) < 0)
  {
    fprintf(stdout, "***ERROR*** during opening log(%s) : [%s]\n", log_file, strerror(ret));
    return ret;
  }

  memset(&giga_options_t, 0, sizeof(struct giga_options));
  initGIGAsetting(GIGA_CLIENT, DEFAULT_MNT, CONFIG_FILE);
  GIGAinit(NULL);

  return ret;
}

void gigaDestroy()
{
  void *unused = NULL;
  GIGAdestroy(unused);
}

int gigaMknod(const char *path, mode_t mode, dev_t dev)
{
  return GIGAmknod(path, mode, dev);
}

int gigaMkdir(const char *path, mode_t mode)
{
  return GIGAmkdir(path, mode);
}

int gigaRmdir(const char *path)
{
  return GIGArmdir(path);
}

int gigaOpen(const char *path, int flags)
{
    struct fuse_file_info fi;
    fi.fh = 0;
    fi.flags = flags;
    GIGAopen(path, &fi);

    if (fi.fh != 0)
        add_giga_fi(&fi, path);

    return fi.fh;
}

int gigaCreat(const char *path, mode_t mode)
{
    (void) mode;
    return gigaOpen(path, O_CREAT | O_TRUNC | O_WRONLY);
}

int gigaRead(int fd, void *buf, size_t size)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int read = GIGAread(gi->path, buf, size, gi->offset, &(gi->fi));
    if (read >= 0)
        gi->offset += size;

    return read;
}

int gigaWrite(int fd, const void *buf, size_t size)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int written = GIGAwrite(gi->path, buf, size, gi->offset, &(gi->fi));
    if (written >= 0)
        gi->offset += size;
    return written;
}

int gigaStat(const char *path, struct statvfs *buf)
{
    return GIGAstatfs(path, buf);
}

int gigaFsync(int fd)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    return GIGAflush(gi->path, &(gi->fi));
}

int gigaClose(int fd)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int ret = GIGArelease(gi->path, &(gi->fi));
    del_giga_fi(fd);
    return ret;
}

int gigaAccess(const char *path, int mask)
{
    return GIGAaccess(path, mask);
}

int gigaUnlink(const char *path)
{
    return GIGAunlink(path);
}
