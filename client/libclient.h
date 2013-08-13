/* C Library for GIGA+TableFS Client
 * Wrapper over FUSE operations
 * Author : Adit Madan (aditm)
 */
#ifndef LIBCLIENT_GIGA_H
#define LIBCLIENT_GIGA_H

#include <sys/stat.h>
#include <stdlib.h>

int gigaInit();
void gigaDestroy();

struct info {
    int mode;
    int uid, gid;
    int size;
    int atime, mtime, ctime;
};

int gigaMknod(const char *path, mode_t mode, dev_t dev);
int gigaMkdir(const char *path, mode_t mode);
int gigaRmdir(const char *path);
//TODO: gigaOpen with mode not implemented
int gigaOpen(const char *path, int flags);
int gigaRead(int fd, void *buf, size_t size);
int gigaWrite(int fd, const void *buf, size_t size);
int gigaPread(int fd, void *buf, size_t size, size_t offset);
int gigaPwrite(int fd, const void *buf, size_t size, size_t offset);
int gigaGetattr(const char *path, struct stat *buf);
int gigaGetinfo(const char *path, struct info *buf);
int gigaFsync(int fd);
int gigaClose(int fd);
int gigaAccess(const char *path, int mask);
int gigaUnlink(const char *path);
int gigaCreat(const char *path, mode_t mode);

#endif /*LIBCLIENT_GIGA_H*/
