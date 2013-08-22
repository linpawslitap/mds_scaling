#ifndef FUSE_OPERATIONS_H
#define FUSE_OPERATIONS_H

#include <fuse.h>

void* GIGAinit(struct fuse_conn_info *conn);
void GIGAdestroy(void *unused);

int GIGAgetattr(const char *path, struct stat *stbuf);

int GIGAsymlink(const char *path, const char *link);
int GIGAreadlink(const char *path, char *link, size_t size);

int GIGAopen(const char *path, struct fuse_file_info *fi);

int GIGAmknod(const char *path, mode_t mode, dev_t dev);
int GIGAmkdir(const char *path, mode_t mode);

int GIGAunlink(const char *path);
int GIGArmdir(const char *path);
int GIGArename(const char *path, const char *newpath);
int GIGAlink(const char *path, const char *newpath);
int GIGAchmod(const char *path, mode_t mode);
int GIGAchown(const char *path, uid_t uid, gid_t gid);
int GIGAtruncate(const char *path, off_t newsize);
int GIGAutime(const char *path, struct utimbuf *ubuf);

int GIGAupdatelink(const char *path, const char* link);
int GIGAfetch(const char *path, mode_t mode,
              int* state, char* buf, int* buf_len);
int GIGAreadall(struct fuse_file_info *fi,
                 char* buf, int* buf_len);
int GIGAwritelink(struct fuse_file_info *fi,
                 const char* link);
int GIGAread(const char *path, char *buf, size_t size, off_t offset,
             struct fuse_file_info *fi);
int GIGAwrite(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi);

int GIGAstatfs(const char *path, struct statvfs *statv);
int GIGAflush(const char *path, struct fuse_file_info *fi);
int GIGArelease(const char *path, struct fuse_file_info *fi);

int GIGAfsync(const char *path, int datasync, struct fuse_file_info *fi);

int GIGAopendir(const char *path, struct fuse_file_info *fi);

int GIGAreaddir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                struct fuse_file_info *fi);
int GIGAreleasedir(const char *path, struct fuse_file_info *fi);

int GIGAfsyncdir(const char *path, int datasync, struct fuse_file_info *fi);

int GIGAaccess(const char *path, int mask);

int GIGAcreate(const char *path, mode_t mode, struct fuse_file_info *fi);

int GIGAftruncate(const char *path, off_t offset, struct fuse_file_info *fi);
int GIGAfgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi);


#endif /* FUSE_OPERATIONS_H */
