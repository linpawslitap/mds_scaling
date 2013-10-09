/* C Library for GIGA+TableFS Client
 * Wrapper over FUSE operations
 * Author : Adit Madan (aditm)
 */
#ifndef LIBCLIENT_GIGA_H
#define LIBCLIENT_GIGA_H

#include <sys/stat.h>
#include <stdlib.h>
#include "common/rpc_giga.h"

int gigaInit();
void gigaDestroy();

struct fetch_reply {
    int state;
    int buf_len;
};

int gigaMknod(const char *path, mode_t mode);
int gigaMkdir(const char *path, mode_t mode);
int gigaRmdir(const char *path);

int gigaRecMknod(const char *path, mode_t mode);
int gigaRecMkdir(const char *path, mode_t mode);

scan_list_t gigaListStatus(const char *path, int* num_entries);
void gigaStatusInfo(scan_list_t ptr, struct info_t* buf);
char* gigaStatusName(scan_list_t ptr, int* name_len);
scan_list_t gigaNextStatus(scan_list_t ptr);
int gigaValidStatus(scan_list_t ptr);
void gigaCleanStatusList(scan_list_t ptr);

int gigaOpen(const char *path, int flags);
int gigaCreate(const char *path, mode_t mode);

int gigaUpdateLink(const char *path, const char *updatelink);
int gigaFetch(const char *path, char* buf,
              struct fetch_reply* reply);
int gigaReadAll(int fd, char* buf,
              struct fetch_reply* reply);
int gigaWriteLink(int fd, const char *updatelink);
int gigaGetParentID(int fd);
int gigaRead(int fd, char *buf, size_t size);
int gigaWrite(int fd, const char *buf, size_t size);
int gigaPread(int fd, void *buf, size_t size, size_t offset);
int gigaPwrite(int fd, const void *buf, size_t size, size_t offset);
int gigaGetAttr(const char *path, struct stat *buf);
int gigaGetInfo(const char *path, struct info_t *buf);
int gigaClose(int fd);
int gigaAccess(const char *path, int mask);
int gigaUnlink(const char *path);

#endif /*LIBCLIENT_GIGA_H*/
