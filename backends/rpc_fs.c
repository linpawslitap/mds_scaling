
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"
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

#define RPCFS_LOG LOG_DEBUG

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

    logMessage(RPCFS_LOG, __func__, "{%s}={p%d->s%d}", name, index, server_id);

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
    
    logMessage(RPCFS_LOG, __func__, "RPC_init: start.");

    if ((giga_rpc_init_1(giga_options_t.num_servers, &rpc_reply, rpc_clnt)) 
         != RPC_SUCCESS) {
        logMessage(LOG_FATAL, __func__, "ERROR_rpc: rpc_init failed."); 
        clnt_perror(rpc_clnt,"(rpc_init failed)");
        exit(1);//TODO: retry again?
    }

    int errnum = rpc_reply.errnum;
    if (errnum == -EAGAIN) {
        int dir_id = 0; // update root server's bitmap
        struct giga_directory *dir = cache_fetch(&dir_id);
        if (dir == NULL) {
            logMessage(RPCFS_LOG, __func__, "Dir (id=%d) not in cache!", dir_id);
            ret = -EIO;
        }
        else {
            update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap); 
            ret = 0;
        }
    } else if (errnum < 0) {
        ret = errnum;
    }

    logMessage(RPCFS_LOG, __func__, "RPC_init: done.");

    return ret;
}

int rpc_getattr(int dir_id, const char *path, struct stat *stbuf)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(RPCFS_LOG, __func__, "ERROR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_getattr_reply_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    logMessage(RPCFS_LOG, __func__, 
               ">>> RPC_getattr(%s): to s[%d]", path, server_id);

    if (giga_rpc_getattr_1(dir_id, (char*)path, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        logMessage(LOG_FATAL, __func__, "ERROR_rpc: rpc_getattr failed."); 
        clnt_perror(rpc_clnt,"(rpc_getattr failed)");
        exit(1);//TODO: retry again?
    }
    
    // check return condition 
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap); 
        logMessage(RPCFS_LOG, __func__, "bitmap updated -- RETRY ..."); 
        goto retry;
    }
    else if (ret < 0) {
        ;
    }
    else {
        *stbuf = rpc_reply.statbuf;
        if (stbuf == NULL)
            logMessage(RPCFS_LOG, __func__, "ERROR_getattr: statbuf is NULL!");
    }

    /*
    int errnum = rpc_reply.result.errnum;
    if (errnum == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap); 
        goto retry;
    } else if (errnum < 0) {
        ret = errnum;
    } else {
        *stbuf = rpc_reply.statbuf;
        if (stbuf == NULL)
            logMessage(RPCFS_LOG, __func__, "getattr() stbuf is NULL!");
        ret = errnum;
    }
    */

    logMessage(RPCFS_LOG, __func__, 
               "<<< RPC_getattr(%s): status=[%d]%s", path, ret, strerror(ret));
    
    return ret;
}

int rpc_mkdir(int dir_id, const char *path, mode_t mode)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(RPCFS_LOG, __func__, "ERROR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    logMessage(RPCFS_LOG, __func__, 
               ">>> RPC_mkdir(%s): to s%d]", path, server_id);

    if (giga_rpc_mkdir_1(dir_id, (char*)path, mode, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        logMessage(LOG_FATAL, __func__, "ERROR_rpc: rpc_mkdir failed."); 
        clnt_perror(rpc_clnt,"(rpc_getattr failed)");
        exit(1);//TODO: retry again?
    }

    // check return condition 
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        logMessage(RPCFS_LOG, __func__, "bitmap updated -- RETRY ..."); 
        goto retry;
    } 
    
    /*
    int errnum = rpc_reply.errnum;
    if (errnum == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap); 
        goto retry;
    } else if (errnum < 0) {
        ret = errnum;
    } else {
        ret = 0;
    }
    */

    logMessage(RPCFS_LOG, __func__, 
               "<<< RPC_mkdir(%s): status=[%d]%s", path, ret, strerror(ret));
    
    return ret;
}

int rpc_mknod(int dir_id, const char *path, mode_t mode, dev_t dev)
{
    int ret = 0;
    
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(RPCFS_LOG, __func__, "ERROR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }
    
    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    logMessage(RPCFS_LOG, __func__, 
               ">>> RPC_mknod(%s): to s%d]", path, server_id);

    if (giga_rpc_mknod_1(dir_id, (char*)path, mode, dev, &rpc_reply, rpc_clnt) 
        != RPC_SUCCESS) {
        logMessage(LOG_FATAL, __func__, "ERROR_rpc: rpc_mknod failed."); 
        clnt_perror(rpc_clnt,"(rpc_mknod failed)");
        exit(1);//TODO: retry again?
    }
    
    // check return condition 
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        logMessage(RPCFS_LOG, __func__, "bitmap updated -- RETRY ..."); 
        goto retry;
    } 

    /*
    int errnum = rpc_reply.errnum;
    if (errnum == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        logMessage(RPCFS_LOG, __func__, "client mapping updated. retrying ...");
        goto retry;
    } else if (errnum < 0) {
        ret = errnum;
    } else {
        ret = 0;
    }
    */

    logMessage(RPCFS_LOG, __func__, 
               "<<< RPC_mknod(%s): status=[%d]%s", path, ret, strerror(ret));
    
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
