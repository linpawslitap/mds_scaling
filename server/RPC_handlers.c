
#include "common/cache.h"
#include "common/defaults.h"
#include "common/debugging.h"
#include "common/rpc_giga.h"
#include "common/options.h"

#include "backends/operations.h"

#include "server.h"

#include <assert.h>
#include <errno.h>
#include <stdbool.h>


bool_t giga_rpc_init_1_svc(int rpc_req, 
                           giga_result_t *rpc_reply, 
                           struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);

    logMessage(LOG_TRACE, __func__, "==> RPC_init_recv = %d", rpc_req);

    // send bitmap for the "root" directory.
    //
    int dir_id = 0;
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        logMessage(LOG_DEBUG, __func__, "Dir (id=%d) not in cache!", dir_id);
        return true;
    }
    rpc_reply->errnum = -EAGAIN;
    memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
           &dir->mapping, sizeof(dir->mapping));

    logMessage(LOG_TRACE, __func__, "RPC_init_reply(%d)", rpc_reply->errnum);

    return true;
}

int giga_rpc_prog_1_freeresult(SVCXPRT *transp, 
                               xdrproc_t xdr_result, caddr_t result)
{
    (void)transp;
    
    logMessage(LOG_TRACE, __func__, "RPC_freeresult_recv");

    xdr_free(xdr_result, result);

    /* TODO: add more cleanup code. */
    
    logMessage(LOG_TRACE, __func__, "RPC_freeresult_reply");

    return 1;
}

bool_t giga_rpc_getattr_1_svc(giga_dir_id dir_id, giga_pathname path, 
                              giga_getattr_reply_t *rpc_reply, 
                              struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(LOG_TRACE, __func__, 
               "==> RPC_getattr_recv(dir_id=%d,path=%s)", dir_id, path);

    bzero(rpc_reply, sizeof(giga_getattr_reply_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->result.errnum = -EIO;
        logMessage(LOG_DEBUG, __func__, "Dir (id=%d) not in cache!", dir_id);
        return true;
    }

    // (1): get the giga index/partition for operation
    int index = giga_get_index_for_file(&dir->mapping, (const char*)path);
    int server = giga_get_server_for_index(&dir->mapping, index);
    
    // (2): is this the correct server? NO --> (errnum=-EAGAIN) and return
    if (server != giga_options_t.serverID) {
        rpc_reply->result.errnum = -EAGAIN;
        memcpy(&(rpc_reply->result.giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        logMessage(LOG_TRACE, __func__, "req for server-%d reached server-%d.",
                   server, giga_options_t.serverID);
        return true;
    }

    char path_name[MAX_LEN];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            rpc_reply->result.errnum = local_getattr(path_name, 
                                                     &rpc_reply->statbuf);
            break;
        case BACKEND_RPC_LEVELDB:
            rpc_reply->result.errnum = leveldb_lookup(ldb_mds, 
                                                      dir_id, index, path, 
                                                      &rpc_reply->statbuf);
            break;  
        default:
            break;

    }

    logMessage(LOG_TRACE, __func__, "RPC_getattr_reply");
    return true;
}

bool_t giga_rpc_mkdir_1_svc(giga_dir_id dir_id, giga_pathname path, mode_t mode,
                            giga_result_t *rpc_reply, 
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(LOG_TRACE, __func__, 
               "==> RPC_mkdir_recv(path=%s,mode=0%3o)", path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        logMessage(LOG_DEBUG, __func__, "Dir (id=%d) not in cache!", dir_id);
        return true;
    }

    // (1): get the giga index/partition for operation
    int index = giga_get_index_for_file(&dir->mapping, (const char*)path);
    int server = giga_get_server_for_index(&dir->mapping, index);
    
    // (2): is this the correct server? NO --> (errnum=-EAGAIN) and return
    if (server != giga_options_t.serverID) {
        rpc_reply->errnum = -EAGAIN;
        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        logMessage(LOG_TRACE, __func__, "req for server-%d reached server-%d.",
                   server, giga_options_t.serverID);
        return true;
    }

    char path_name[MAX_LEN];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            rpc_reply->errnum = local_mkdir(path_name, mode);
            break;
        case BACKEND_RPC_LEVELDB:
            // create object in the underlying file system
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            rpc_reply->errnum = local_mkdir(path_name, mode); 
            
            // create object entry (metadata) in levelDB
            object_id += 1; 
            rpc_reply->errnum = leveldb_create(ldb_mds, dir_id, index,
                                               OBJ_DIR, 
                                               object_id, path, path_name);
            break;
        default:
            break;

    }

    logMessage(LOG_TRACE, __func__, 
               "RPC_mkdir_reply(status=%d)", rpc_reply->errnum);

    return true;
}



