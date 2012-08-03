
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/giga_index.h"
#include "common/rpc_giga.h"

#include "operations.h"

#include <errno.h>
#include <fcntl.h>
#include <rpc/rpc.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define RPCFS_LOG   LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(RPCFS_LOG, __func__, format, __VA_ARGS__); 

static 
void update_client_mapping(struct giga_directory *dir, struct giga_mapping_t *map)
{
    giga_update_cache(&dir->mapping, map);
}

static 
int get_server_for_file(struct giga_directory *dir, const char *name)
{
    index_t index = giga_get_index_for_file(&(dir->mapping), name);
    //int server_id = (index + (&(dir->mapping))->zeroth_server) % 
    //                ((&(dir->mapping))->server_count);
    int server_id = giga_get_server_for_index(&dir->mapping, index);

    LOG_MSG("object[%s] goes to p[%d]-on-s[%d]", name, index, server_id);

    return server_id;

    //TODO: mask this call for better debugging: partition id and server id
    //return giga_get_server_for_file(&dir->mapping, name);
}

int rpc_init()
{
    int ret = 0;
    int server_id = 0;
    
    CLIENT *rpc_clnt = getConnection(server_id);
    giga_result_t rpc_reply;
    
    LOG_MSG(">>> RPC_init: s[%d]", server_id);

    if ((giga_rpc_init_1(giga_options_t.num_servers, &rpc_reply, rpc_clnt)) 
         != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_init(%s)", clnt_spcreateerror(""));
        exit(1);//TODO: retry again?
    }

    int errnum = rpc_reply.errnum;
    if (errnum == -EAGAIN) {
        int dir_id = 0; // update root server's bitmap
        struct giga_directory *dir = cache_fetch(&dir_id);
        if (dir == NULL) {
            LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
            ret = -EIO;
        }
        else {
            update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap); 
            ret = 0;
        }
    } else if (errnum < 0) {
        ret = errnum;
    }
    
    LOG_MSG("<<< RPC_init: s[%d]", server_id);

    return ret;
}

int rpc_getattr(int dir_id, const char *path, struct stat *stbuf)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_getattr_reply_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_getattr(%s): to s[%d]", path, server_id);

    if (giga_rpc_getattr_1(dir_id, (char*)path, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_getattr(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }
    
    // check return condition 
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap); 
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
        goto retry;
    }
    else if (ret < 0) {
        ;
    }
    else {
        *stbuf = rpc_reply.statbuf;
        if (stbuf == NULL)
            LOG_MSG("ERR_getattr(%s): statbuf is NULL!", path);
    }

    LOG_MSG("<<< RPC_getattr(%s): status=[%d]%s", path, ret, strerror(ret));
    
    return ret;
}

int rpc_mkdir(int dir_id, const char *path, mode_t mode)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERROR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_mkdir(%s): to s%d]", path, server_id);

    if (giga_rpc_mkdir_1(dir_id, (char*)path, mode, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_mkdir(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    // check return condition 
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
        goto retry;
    } else if (ret < 0) {
        ;
    } else {
        ret = 0;
    }

    LOG_MSG("<<< RPC_mkdir(%s): status=[%d]%s", path, ret, strerror(ret));
    
    return ret;
}

int rpc_mknod(int dir_id, const char *path, mode_t mode, dev_t dev)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_mknod(%s): to s%d]", path, server_id);

    if (giga_rpc_mknod_1(dir_id, (char*)path, mode, dev, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_mknod(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }
    
    // check return condition 
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
        goto retry;
    } else if (ret < 0) {
        ;
    } else {
        ret = 0;
    }


    LOG_MSG("<<< RPC_mknod(%s): status=[%d]%s", path, ret, strerror(ret));
    
    return ret;
}

scan_list_t rpc_readdir(int dir_id, const char *path)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    readdir_result_t rpc_reply;
    scan_list_t ls;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_readdir(%s): to s[%d]", path, server_id);

    int partition_id = 0;
    if (giga_rpc_readdir_1(dir_id, partition_id, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_readdir(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    
    // check return condition 
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.readdir_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
        goto retry;
    } else if (ret < 0) {
        ;
    } else {
        for (ls = rpc_reply.readdir_result_t_u.list; ls != NULL; ls = ls->next)
            LOG_MSG("readdir_result=[%s]", ls->entry_name);
        ret = 0;
    }


    LOG_MSG("<<< RPC_readdir(%s): status=[%d]%s", path, ret, strerror(ret));
   
    return rpc_reply.readdir_result_t_u.list;
    //return ret;
}

int rpc_releasedir(int dir_id, const char *path)
{
    int ret = 0;
    
    LOG_MSG(">>> RPC_releasedir(%s, d%d)", path, dir_id);
    LOG_MSG("<<< RPC_releasedir(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

int rpc_opendir(int dir_id, const char *path)
{
    int ret = 0;
    
    LOG_MSG(">>> RPC_opendir(%s, d%d)", path, dir_id);
    LOG_MSG("<<< RPC_opendir(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}


/*
int local_symlink(const char *path, const char *link)
{
    int ret = 0;

    if ((ret = symlink(path, link)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "symlink(%s,%s) failed: %s", path, link, strerror(errno));
        ret = errno;
    }
    
    return ret;
}

int local_readlink(const char *path, char *link, size_t size)
{
    int ret = 0;

    if ((ret = readlink(path, link, size-1)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "readlink(%s,%s) failed: %s", path, link, strerror(errno));
        ret = errno;
    }
    else  {
	    link[ret] = '\0';
        ret = 0;
    }
    
    return ret;
}

// NOTE: ret_fd is to return the fd from the open call (needed for fi->fh)
int local_open(const char *path, int flags, int *ret_fd)
{
    int ret = 0;
    int fd;
    
    if ((fd = open(path, flags)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "open(%s) failed: %s", path, strerror(errno));
        ret = errno;
    }

    *ret_fd = fd;

    return ret;
}

int local_mknod(const char *path, mode_t mode, dev_t dev)
{
    int ret = 0;

    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        if ((ret = open(path, O_CREAT | O_EXCL | O_WRONLY, mode) < 0)) {
            logMessage(LOG_FATAL, __func__,
                       "open(%s, CRT|EXCL|WR) failed: %s", path, strerror(errno));
            ret = (errno);
        }
        else {
            // close the opened file.
            if ((ret = close(ret)) < 0) {
                logMessage(LOG_FATAL, __func__,
                           "close(%d) failed: %s", ret, strerror(errno));
                ret = (errno);
            }
	    }
    } 
    else if (S_ISFIFO(mode)) {
	    if ((ret = mkfifo(path, mode)) < 0) {
            logMessage(LOG_FATAL, __func__,
                       "mkfifo(%s) failed: %s", path, strerror(errno));
            ret = (errno);
        }
	} else {
	    if ((ret = mknod(path, mode, dev)) < 0) {
            logMessage(LOG_FATAL, __func__,
                       "mknod(%s) failed: %s", path, strerror(errno));
            ret = (errno);
        }
	}

    return ret;
}
*/
