#include "libclient.h"
#include "client.h"
#include "FUSE_operations.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/uthash.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

//TODO: fd might be used up
/* Open files info */
typedef struct {
    int fd; //Key for hashmap
    size_t offset;
    struct fuse_file_info fi;
    UT_hash_handle hh;
} giga_file_info_t;

giga_file_info_t *_open_files;
static int fd_count;

int add_giga_fi(const struct fuse_file_info *fi) {
    giga_file_info_t *gi = (giga_file_info_t *) malloc(sizeof(giga_file_info_t));
    memset(gi, 0, sizeof(giga_file_info_t));
    fd_count += 1;
    gi->fd = fd_count;
    gi->fi = *fi;
    HASH_ADD_INT(_open_files, fd, gi);
    return fd_count;
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
  fd_count = 0;

  return ret;
}

void gigaDestroy()
{
  void *unused = NULL;
  GIGAdestroy(unused);
}

int gigaMknod(const char *path, mode_t mode)
{
  return GIGAmknod(path, mode, 0);
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

    if (fi.fh != 0) {
        return add_giga_fi(&fi);
    } else {
        return -1;
    }
}

int gigaFetch(const char *path, mode_t mode,
              char* buf, struct fetch_reply* reply) {
    return GIGAfetch(path, mode, &(reply->state), buf, &(reply->buf_len));
}

int gigaReadall(int fd, char *buf,
                struct fetch_reply* reply)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    return GIGAreadall(&(gi->fi), buf, &(reply->buf_len));
}

int gigaWritelink(int fd, const char* link)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    return GIGAwritelink(&(gi->fi), link);
}

int gigaUpdatelink(const char *path, const char *link) {
    return GIGAupdatelink(path, link);
}

int gigaCreate(const char *path, mode_t mode) {
    struct fuse_file_info fi;
    fi.fh = 0;
    fi.flags = O_WRONLY | O_CREAT;
    GIGAcreate(path, mode, &fi);

    if (fi.fh != 0) {
        return add_giga_fi(&fi);
    } else {
        return -1;
    }
}

int gigaRead(int fd, char *buf, size_t size)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int read = GIGAread(NULL, buf, size, gi->offset, &(gi->fi));
    if (read >= 0)
        gi->offset += size;

    return read;
}

int gigaWrite(int fd, const char *buf, size_t size)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int written = GIGAwrite(NULL, buf, size, gi->offset, &(gi->fi));
    if (written >= 0)
        gi->offset += size;
    return written;
}


int gigaPread(int fd, void *buf, size_t size, size_t offset)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int read = GIGAread(NULL, buf, size, offset, &(gi->fi));

    return read;
}

int gigaPwrite(int fd, const void *buf, size_t size, size_t offset)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int written = GIGAwrite(NULL, buf, size, offset, &(gi->fi));
    return written;
}

int gigaGetattr(const char *path, struct stat *buf)
{
    return GIGAgetattr(path, buf);
}

int gigaGetInfo(const char *path, struct info *buf)
{
    struct stat statbuf;
    int ret = GIGAgetattr(path, &statbuf);
    if (ret == 0) {
      buf->permission = statbuf.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
      buf->is_dir = S_ISDIR(statbuf.st_mode);
      buf->size = statbuf.st_size;
      buf->uid = statbuf.st_uid;
      buf->gid = statbuf.st_gid;
      buf->atime = statbuf.st_atime;
      buf->ctime = statbuf.st_ctime;
    }
    return ret;
}


int gigaFsync(int fd)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    return GIGAflush(NULL, &(gi->fi));
}

int gigaClose(int fd)
{
    giga_file_info_t *gi = get_giga_fi(fd);
    int ret = GIGArelease(NULL, &(gi->fi));
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
