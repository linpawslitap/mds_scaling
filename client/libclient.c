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
struct giga_file_info
{
    int fd; //Key for hashmap
    int flags;
    off_t offset;
    char path[PATH_MAX];
    UT_hash_handle hh;
};

struct giga_file_info *_open_files;

void createGigaFileInfo(int p_fd, int p_flags, const char *p_path)
{
    struct giga_file_info *new_info = (struct giga_file_info *) malloc(sizeof(struct giga_file_info));
    new_info->fd = p_fd;
    new_info->offset = 0;
    new_info->flags = p_flags;
    memcpy(new_info->path, p_path, PATH_MAX*sizeof(char));

    HASH_ADD_INT(_open_files, fd, new_info);
}

void getGigaFileInfo(int fd, struct giga_file_info *info)
{
    HASH_FIND_INT(_open_files, &fd, info);
}

void deleteGigaFileInfo(int fd)
{
    struct giga_file_info *info;
    HASH_FIND_INT(_open_files, &fd, info);
    if(info != NULL)
        HASH_DEL(_open_files, info);
}

void updateGigaOffset(int fd, off_t update_offset)
{
    struct giga_file_info *info;
    HASH_FIND_INT(_open_files, &fd, info);

    if(info != NULL)
    {
        info->offset += update_offset;
    }
    else {
        //TODO: ?
    }
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
    fi.flags = flags;
    fi.fh = -1;

    GIGAopen(path, &fi);

    if (fi.fh > 0)
        createGigaFileInfo(fi.fh, flags, path);

    return fi.fh;
}

int gigaCreat(const char *path, mode_t mode)
{
    //TODO: mode ignored
    (void) mode;

    return gigaOpen(path, O_CREAT | O_TRUNC | O_WRONLY);
}

int gigaRead(int fd, void *buf, size_t size)
{
    struct giga_file_info gi;
    getGigaFileInfo(fd, &gi);

    off_t offset = gi.offset;

    struct fuse_file_info fi;
    fi.flags = gi.flags;
    fi.fh = fd;

    int read = GIGAread(gi.path, buf, size, offset, &fi);
    if (read == offset)
        updateGigaOffset(fd, offset);

    return read;
}

int gigaWrite(int fd, const void *buf, size_t size)
{
    struct giga_file_info gi;
    getGigaFileInfo(fd, &gi);

    off_t offset = gi.offset;

    struct fuse_file_info fi;
    fi.flags = gi.flags;
    fi.fh = fd;

    int written = GIGAwrite(gi.path, buf, size, offset, &fi);
    if (written == offset)
        updateGigaOffset(fd, offset);

    return written;
}

int gigaStat(const char *path, struct statvfs *buf)
{
    return GIGAstatfs(path, buf);
}

int gigaFsync(int fd)
{
    struct giga_file_info gi;
    getGigaFileInfo(fd, &gi);

    struct fuse_file_info fi;
    fi.fh = fd;
    fi.flags = gi.flags;

    return GIGAflush(gi.path, &fi);
}

int gigaClose(int fd)
{
    struct giga_file_info gi;
    getGigaFileInfo(fd, &gi);

    struct fuse_file_info fi;
    fi.flags = gi.flags;
    fi.fh = fd;

    int ret = GIGArelease(gi.path, &fi);
    deleteGigaFileInfo(fd);
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
