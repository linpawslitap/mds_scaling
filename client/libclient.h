/* C Library for GIGA+TableFS Client
 * Wrapper over FUSE operations
 * Author : Adit Madan (aditm)
 */
#ifndef LIBCLIENT_GIGA_H
#define LIBCLIENT_GIGA_H

#include <sys/stat.h>
#include <stdlib.h>
#include <sys/statvfs.h>

int gigaInit();
void gigaDestroy();

int gigaMknod(const char *path, mode_t mode, dev_t dev);
int gigaMkdir(const char *path, mode_t mode);
int gigaRmdir(const char *path);

//TODO: gigaOpen with mode not implemented
int gigaOpen(const char *path, int flags);
int gigaRead(int fd, void *buf, size_t size);
int gigaWrite(int fd, const void *buf, size_t size);
int gigaStat(const char *path, struct statvfs *buf);
int gigaFsync(int fd);
int gigaClose(int fd);
int gigaAccess(const char *path, int mask);
int gigaUnlink(const char *path);
int gigaCreat(const char *path, mode_t mode);

#endif /*LIBCLIENT_GIGA_H*/
