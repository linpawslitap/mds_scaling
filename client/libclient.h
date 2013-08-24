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
    int permission;
    int is_dir;
    int uid, gid;
    int size;
    int atime;
    int ctime;
};

struct fetch_reply {
    int state;
    int buf_len;
};

int gigaMknod(const char *path, mode_t mode);
int gigaMkdir(const char *path, mode_t mode);
int gigaRmdir(const char *path);

int gigaOpen(const char *path, int flags);
int gigaCreate(const char *path, mode_t mode);

int gigaUpdatelink(const char *path, const char *updatelink);
int gigaFetch(const char *path, char* buf,
              struct fetch_reply* reply);
int gigaReadall(int fd, char* buf,
              struct fetch_reply* reply);
int gigaWritelink(int fd, const char *updatelink);
int gigaGetParentID(int fd);
int gigaRead(int fd, char *buf, size_t size);
int gigaWrite(int fd, const char *buf, size_t size);
int gigaPread(int fd, void *buf, size_t size, size_t offset);
int gigaPwrite(int fd, const void *buf, size_t size, size_t offset);
int gigaGetattr(const char *path, struct stat *buf);
int gigaGetinfo(const char *path, struct info *buf);
int gigaClose(int fd);
int gigaAccess(const char *path, int mask);
int gigaUnlink(const char *path);

#endif /*LIBCLIENT_GIGA_H*/
