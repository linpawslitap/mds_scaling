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

/*
int giga_create(const char *path, mode_t, struct fuse_file_info *);
int giga_getattr(const char *path, struct stat *stbuf);
int giga_mkdir(const char * path, mode_t mode);
//int giga_mknod();
int giga_open(const char *path, struct fuse_file_info *fi);
int giga_read(const char* path, char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi);
int giga_readdir(const char * path, 
                 void * buf, fuse_fill_dir_t filler, off_t offset,
                 struct fuse_file_info *fi);
int giga_rename(const char *src_path, const char *dst_path);
int giga_rmdir(const char *path);
int giga_unlink(const char *path);
int giga_utime(const char *path, struct utimbuf *time);
int giga_write(const char* path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi);
*/

#endif /* FUSE_OPERATIONS_H */
