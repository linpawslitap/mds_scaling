
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/rpc_giga.h"

#include "backends/operations.h"

#include "client.h"
#include "FUSE_operations.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <fuse.h>
#include <rpc/rpc.h>
#include <unistd.h>

#define _LEVEL_     LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(_LEVEL_, __func__, format, __VA_ARGS__);

static struct MetaDB ldb_mds;
//static uint64_t object_id = 0;

static int  parse_path_components(const char *path, char *file, char *dir);
static void get_full_path(char fpath[], const char *path);

#define FUSE_ERROR(x)   -(x)

typedef struct {
    int fd;
    int flags;
    int state;
    int dir_id;
    char file[PATH_MAX];
} rpc_leveldb_fh_t;

/*
 * Parse the full path name of the file to give the name of the file and the
 * name of the file's parent directory.
 *
 * -- Example: A path "/a/b/f.txt" yields "f.txt" as file and "b" as dir
 */
static int parse_path_components(const char *path, char *file, char *dir)
{
    const char *p = path;
    if (!p || !file)
    return -1;

    if (strcmp(path, "/") == 0) {
        strcpy(file, "/");
        if (dir)
            strcpy(dir, "/");
        return 0;
    }

    int pathlen = strlen(path);

    if (pathlen > PATH_MAX)
        return -ENAMETOOLONG;

    p += pathlen;
    while ( (*p) != '/' && p != path)
        p--; // Come back till '/'

    if (pathlen - (int)(p - path) > PATH_MAX)
        return -ENAMETOOLONG;

    // Copy after slash till end into filename
    strncpy(file, p+1, PATH_MAX);
    if (dir) {
        if (path == p)
            strncpy(dir, "/", 2);
        else {
            // Copy rest into dirpath
            strncpy(dir, path, (int)(p - path ));
            dir[(int)(p - path)] = '\0';
        }
    }

    LOG_MSG("parsed [%s] to {file=[%s],dir=[%s]}", path, file, dir);
    return 0;
}

/*
 * Appends a given path name with something else.
 *
 */
static void get_full_path(char fpath[PATH_MAX], const char *path)
{
    strncpy(fpath, DEFAULT_SRV_BACKEND, strlen(DEFAULT_SRV_BACKEND)+1);
            //giga_options_t.mountpoint, strlen(giga_options_t.mountpoint)+1);
    strncat(fpath, path, PATH_MAX);          //XXX: long path names with break!

    LOG_MSG("converted [%s] to [%s]", path, fpath);

    return;
}

void* GIGAinit(struct fuse_conn_info *conn)
{
    LOG_MSG(">>> FUSE_init(%d)", ROOT_DIR_ID);

    (void)conn;

    switch (giga_options_t.backend_type) {
        /*
        case BACKEND_LOCAL_LEVELDB:
            metadb_init(&ldb_mds, DEFAULT_LEVELDB_DIR);
            object_id = 0;
            if (metadb_create(ldb_mds,
                              ROOT_DIR_ID, 0,
                              OBJ_DIR,
                              object_id, "/", giga_options_t.mountpoint) < 0) {
                LOG_ERR("root entry creation error(%s)", ROOT_DIR_ID);
                exit(1);
            }
            break;
        */
        case BACKEND_RPC_LOCALFS:
            break;
        case BACKEND_RPC_LEVELDB:
            rpcInit();
            //rpcConnect();     //FIXME: I don't need this
            if (rpc_init() < 0) {
                LOG_ERR("RPC_init_err(%s)", ROOT_DIR_ID);
                exit(1);
            }
            break;
        default:
            break;
    }

    LOG_MSG("<<< FUSE_init(%d)", ROOT_DIR_ID);
    return NULL;
}

void GIGAdestroy(void * unused)
{
    (void)unused;

    logClose();

    // FIXME: check cleanup code.
    //rpc_disconnect();
}

int lookup_dir(const char* path) {
  int dir_id = fuse_cache_lookup((char*)path);
  if (dir_id >= 0) {
    return dir_id;
  }
  char pdir[PATH_MAX] = {0};
  char dirname[PATH_MAX] = {0};
  parse_path_components(path, dirname, pdir);
  dir_id = lookup_dir(pdir);
  struct stat statbuf;
  int ret = rpc_getattr(dir_id, dirname, &statbuf);
  if (ret < 0) {
    return -1;
  }
  if (S_ISDIR(statbuf.st_mode))
    fuse_cache_insert((char*)path, statbuf.st_ino);
  return statbuf.st_ino;
}

int lookup_parent_dir(const char* path, char* file, char* dir) {
    parse_path_components(path, file, dir);
    return lookup_dir(dir);
}

int GIGAgetattr(const char *path, struct stat *statbuf)
{
    LOG_MSG(">>> FUSE_getattr(%s): stat=[0x%016lx]", path, statbuf);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    char dir[PATH_MAX] = {0};
    char file[PATH_MAX] = {0};
    int dir_id = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_getattr(fpath, statbuf);
            ret = FUSE_ERROR(ret);
            //if ((ret = lstat(fpath, statbuf)) < 0)
            //    ret = FUSE_ERROR(ret);
            break;
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            ret  = metadb_lookup(ldb_mds, 0, 0, file, statbuf);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            dir_id = lookup_parent_dir(path, file, dir);
            ret = rpc_getattr(dir_id, file, statbuf);
            if (S_ISDIR(statbuf->st_mode))
                fuse_cache_insert((char*)path, statbuf->st_ino);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LOCALFS:
            parse_path_components(path, file, dir);
            //TODO: convert "dir" to "dir_id"
            ret = rpc_getattr(dir_id, file, statbuf);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }

    LOG_MSG("<<< FUSE_getattr(%s): size=[%ld] ret=[%d]",
            path, statbuf->st_size, ret);

    return ret;
}

int GIGAmkdir(const char *path, mode_t mode)
{
    LOG_MSG(">>> FUSE_mkdir(%s): mode=[%lo]", path, (unsigned long)mode);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    char dir[PATH_MAX] = {0};
    char file[PATH_MAX] = {0};
    int dir_id = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_mkdir(fpath, mode);
            ret = FUSE_ERROR(ret);
            break;
        /*
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            object_id += 1;
            ret = metadb_create(ldb_mds, 0, 0, OBJ_DIR, object_id, file, path);
            ret = FUSE_ERROR(ret);
            break;
        */
        case BACKEND_RPC_LEVELDB:
            dir_id = lookup_parent_dir(path, file, dir);
            ret = rpc_mkdir(dir_id, file, mode);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LOCALFS:
            parse_path_components(path, file, dir);
            ret = rpc_mkdir(dir_id, file, mode);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }

    LOG_MSG("<<< FUSE_mkdir(%s): status=[%d] ", path, ret);

    return ret;
}


int GIGAmknod(const char *path, mode_t mode, dev_t dev)
{
    LOG_MSG(">>> FUSE_mknod(%s): mode=[0%3o],dev=[%lld]", path, mode, dev);

    int ret = 0;
    char fpath[PATH_MAX];
    char dir[PATH_MAX] = {0};
    char file[PATH_MAX] = {0};
    int dir_id = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_mknod(fpath, mode, dev);
            ret = FUSE_ERROR(ret);
            break;
        /*
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            object_id += 1;
            ret = metadb_create(ldb_mds, 0, 0, OBJ_MKNOD, object_id, file, path);
            ret = FUSE_ERROR(ret);
            break;
        */
        case BACKEND_RPC_LOCALFS:
            ;
        case BACKEND_RPC_LEVELDB:
            dir_id = lookup_parent_dir(path, file, dir);
            ret = rpc_mknod(dir_id, file, mode, dev);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }

    LOG_MSG("<<< FUSE_mknod(%s): status=[%d] ", path, ret);
    return ret;
}

int GIGAopendir(const char *path, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_opendir(%s): fi=[0x%016lx])", path, fi);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    DIR *dp;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((dp = opendir(fpath)) == NULL)
                ret = errno;
            fi->fh = (intptr_t) dp;
            break;
        case BACKEND_RPC_LEVELDB:
            ret = 0;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_opendir(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAreaddir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_readdir(%s): offset=%lld", path, offset);

    int ret = 0;
    int dir_id = 0;
    struct dirent *de;
    DIR *dp = (DIR*) (uintptr_t) fi->fh;

    scan_list_t ls;
    scan_list_t ret_ls;
    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if ((de = readdir(dp)) == 0) {
                ret = errno;
                break;
            }
            do {
                if (filler(buf, de->d_name, NULL, 0) != 0)
                    ret = ENOMEM;
            } while ((de = readdir(dp)) != NULL);
            break;
        case BACKEND_RPC_LEVELDB:
            dir_id = lookup_dir(path);
            ret_ls = (scan_list_t)rpc_readdir(dir_id, path);
            for (ls = ret_ls; ls != NULL; ls = ls->next) {
                if (filler(buf, ls->entry_name, NULL, 0) != 0) {
                    ret = ENOMEM;
                    LOG_MSG("ERR_rpc_readdir(%s): [%s]", path, strerror(ret));
                    break;
                }
            }
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_readdir(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAreleasedir(const char *path, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_releasedir(%s): fi=[0x%016lx])", path, fi);

    int ret = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            closedir((DIR*)(uintptr_t)fi->fh);
            break;
        case BACKEND_RPC_LEVELDB:
            ret = 0;
            break;
        default:
            ret = ENOTSUP;
            break;
    }


    LOG_MSG("<<< FUSE_releasedir(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

// =========================================================

int GIGAunlink(const char *path)
{
    LOG_MSG(">>> FUSE_unlink(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = unlink(fpath)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_unlink(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGArmdir(const char *path)
{
    LOG_MSG(">>> FUSE_rmdir(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = rmdir(fpath)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_rmdir(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAsymlink(const char *path, const char *link)
{
    LOG_MSG(">>> FUSE_symlink(%s): link=[%s])", path, link);

    int ret = 0;
    char flink[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(flink, link);
            if ((ret = symlink(path, flink)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_symlink(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGArename(const char *path, const char *newpath)
{
    LOG_MSG(">>> FUSE_rename(%s): to [%s]", path, newpath);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    char fnewpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            get_full_path(fnewpath, newpath);
            if ((ret = rename(fpath, fnewpath)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_rename(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAlink(const char *path, const char *newpath)
{
    LOG_MSG(">>> FUSE_link(%s): to [%s]", path, newpath);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    char fnewpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            get_full_path(fnewpath, newpath);
            if ((ret = link(fpath, fnewpath)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_link(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAchmod(const char *path, mode_t mode)
{
    LOG_MSG(">>> FUSE_chmod(%s): mode=%3o ", path, mode);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = chmod(fpath, mode)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_chmod(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAchown(const char *path, uid_t uid, gid_t gid)
{
    LOG_MSG(">>> FUSE_chown(%s): uid=%d, gid=%d ", path, uid, gid);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = chown(fpath, uid, gid)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_chown(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAtruncate(const char *path, off_t newsize)
{
    LOG_MSG(">>> FUSE_truncate(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = truncate(fpath, newsize)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }
    LOG_MSG("<<< FUSE_truncate(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAutime(const char *path, struct utimbuf *ubuf)
{
    LOG_MSG(">>> FUSE_utime(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = utime(fpath, ubuf)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_utime(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAopen(const char *path, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_open(%s): fi=[0x%016lx])", path, fi);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    char dir[PATH_MAX] = {0};
    rpc_leveldb_fh_t *fh = NULL;
    int fd;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((fd = open(fpath, fi->flags)) < 0)
                ret = errno;
            fi->fh = (intptr_t)fd;
            break;
        case BACKEND_RPC_LEVELDB:
            fh = (rpc_leveldb_fh_t *) malloc(sizeof(rpc_leveldb_fh_t));
            fh->dir_id = lookup_parent_dir(path, fh->file, dir);
            fh->flags = fi->flags;
            ret = rpc_open(fh->dir_id, fh->file, fi->flags,
                           &fh->state, fpath);
            if (ret >= 0) {
                if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                  if ((fh->fd = open(fpath, fi->flags)) < 0)
                      ret = errno;
                }
                fi->fh = (intptr_t) fh;
            } else {
                LOG_MSG(" FUSE_open(%s): link cannot be read from server:"
                        "ret=[%d:%s])", path, ret, strerror(ret));
                free(fh);
                fi->fh = (intptr_t) NULL;
            }
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_open(%s): ret=[%d:%s] and fh=[0x%016lx])",
            path, ret, strerror(ret), fi->fh);

    return FUSE_ERROR(ret);
}

int GIGAread(const char *path, char *buf, size_t size, off_t offset,
             struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_read(%s): size=%ld,offset=%ld,and fi=[0x%016lx] ",
            path, size, offset, fi);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    rpc_leveldb_fh_t* fh = NULL;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if ((ret = pread(fi->fh, buf, size, offset)) < 0)
                ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            fh = (rpc_leveldb_fh_t*) (uintptr_t) fi->fh;
            if (fh == NULL)
                break;
            if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                if (fh->fd >= 0 && (ret = pread(fh->fd, buf, size, offset)) < 0)
                    ret = FUSE_ERROR(ret);
            } else {
                ret = rpc_read(fh->dir_id, fh->file, buf, size, offset,
                               &fh->state, fpath);
                if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                    if ((fh->fd = open(fpath, fi->flags)) < 0) {
                        ret = FUSE_ERROR(errno);
                        break;
                    }
                    if ((ret = pread(fh->fd, buf, size, offset)) < 0)
                        ret = FUSE_ERROR(ret);
                }
            }
            break;
        default:
            ret = FUSE_ERROR(ENOTSUP);
            break;
    }

    LOG_MSG("<<< FUSE_read(%s): ret=[%d]", path, ret);

    return ret;
}

int GIGAwrite(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_write(%s): size=%ld, offset=%ld and fi=[0x%016x]",
            path, size, offset, fi);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    rpc_leveldb_fh_t* fh = NULL;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if ((ret = pwrite(fi->fh, buf, size, offset)) < 0)
                ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            fh = (rpc_leveldb_fh_t*) (uintptr_t) fi->fh;
            if (fh == NULL)
                break;
            if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                if (fh->fd >= 0 && (ret = pwrite(fh->fd, buf, size, offset)) < 0)
                   ret = FUSE_ERROR(ret);
            } else {
                ret = rpc_write(fh->dir_id, fh->file, buf, size, offset,
                                &fh->state, fpath);
                if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                    LOG_MSG("open(%s): %s", path, fpath);
                    if ((fh->fd = open(fpath, O_RDWR)) <= 0) {
                        fh->fd = 0;
                        ret = FUSE_ERROR(errno);
                        LOG_MSG("write(%s): error fpath=[%s] ret=[%d]", path, fpath, ret);
                        break;
                    }
                    if ((ret = pwrite(fh->fd, buf, size, offset)) < 0)
                        ret = FUSE_ERROR(errno);
                    LOG_MSG("write(%s): fd=[%d] fpath=[%s] ret=[%d]", path, fh->fd, fpath, ret);
                }
            }
            break;
        default:
            ret = FUSE_ERROR(ENOTSUP);
            break;
    }

    LOG_MSG("<<< FUSE_write(%s): ret=[%d] %s", path, ret, strerror(ret));

    return ret;
}


int GIGAstatfs(const char *path, struct statvfs *statv)
{
    LOG_MSG(">>> FUSE_statfs(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = statvfs(fpath, statv)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_statfs(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAflush(const char *path, struct fuse_file_info *fi)
{
    int ret = 0;

    LOG_MSG(">>> FUSE_flush(%s): fi=[0x%016lx])", path, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path

    return ret;
}

int GIGArelease(const char *path, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_release(%s): fi=[0x%016lx] fh=[0x%016lx])",
            path, fi, fi->fh);

    int ret = 0;
    int ret_remote = 0;
    rpc_leveldb_fh_t* fh = NULL;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            ret = close(fi->fh);
            break;
        case BACKEND_RPC_LEVELDB:
            fh = (rpc_leveldb_fh_t*) (uintptr_t) fi->fh;
            if (fh == NULL)
                break;
            if (fh->state == RPC_LEVELDB_FILE_IN_FS) {
                ret = close(fh->fd);
            }
            ret_remote = rpc_close(fh->dir_id, fh->file);
            if (ret_remote < ret) {
              ret = ret_remote;
            }
            free(fh);
            break;
        default:
            break;
    }
    LOG_MSG("<<< FUSE_release(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAfsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_fsync(%s): datasync=%d and fi=[0x%016lx]", path, datasync, fi);

    int ret = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if (datasync)
                ret = fdatasync(fi->fh);
            else
                ret = fsync(fi->fh);
            if (ret < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_fsync(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}


int GIGAfsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_fsyncdir(%s): dsync=%d and fi=[0x%016lx]", path, datasync, fi);

    int ret = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_fsync(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAaccess(const char *path, int mask)
{
    LOG_MSG(">>> FUSE_access(%s): mask=[0%o]", path, mask);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = access(fpath, mask)) < 0)
                ret = errno;
            break;
        default:
            ret = 0;
            break;
    }

    LOG_MSG("<<< FUSE_access(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAcreate(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_create(%s): mode=[0%03o]", path, mode);

    int ret = 0;
    char fpath[PATH_MAX] = {0};
    /*
    char dir[PATH_MAX] = {0};
    char file[PATH_MAX] = {0};
    */
    int fd = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((fd = creat(fpath, mode)) < 0)
                ret = errno;
            fi->fh = fd;
            ret = FUSE_ERROR(ret);
            break;
        /*
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            object_id += 1;
            ret = metadb_create(ldb_mds, 0, 0, OBJ_MKNOD, object_id, file, path);
            ret = FUSE_ERROR(ret);
            break;
        */
        default:
            ret = ENOTSUP;
            ret = FUSE_ERROR(ret);
            break;
    }

    LOG_MSG("<<< FUSE_create(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return ret;
}

int GIGAftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_ftruncate(%s): offset=%lld", path, offset);

    int ret = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if ((ret = ftruncate(fi->fh, offset)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_truncate(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

int GIGAfgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    LOG_MSG(">>> FUSE_fgetattr(%s): statbuf=[0x%016lx] fi=[0x%016lx]",
            path, statbuf, fi);

    int ret = 0;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            if ((ret = fstat(fi->fh, statbuf)) < 0)
                ret = errno;
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_fgetattr(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}



int GIGAreadlink(const char *path, char *link, size_t size)
{
    LOG_MSG(">>> FUSE_readlink(%s): link=[%s],size[%d])", path, link, size);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = readlink(fpath, link, size-1)) < 0) {
                ret = errno;
            } else {
                link[ret] = '\0';
                ret = 0;
            }
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_readlink(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}

/*
{
    LOG_MSG(">>> FUSE_XXX(%s): ", path);

    int ret = 0;
    char fpath[PATH_MAX] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_XXX();
            break;
        default:
            ret = ENOTSUP;
            break;
    }

    LOG_MSG("<<< FUSE_XXX(%s): ret=[%d:%s]", path, ret, strerror(ret));

    return FUSE_ERROR(ret);
}
*/
