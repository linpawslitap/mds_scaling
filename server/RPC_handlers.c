
#include "common/cache.h"
#include "common/connection.h"
#include "common/defaults.h"
#include "common/debugging.h"
#include "common/rpc_giga.h"
#include "common/options.h"

#include "backends/operations.h"

#include "server.h"
#include "split.h"

#include <assert.h>
#include <errno.h>
#include <stdbool.h>

#define HANDLER_LOG LOG_DEBUG

static
int check_split_eligibility(struct giga_directory *dir, int index)
{
    if (dir->partition_size[index] < giga_options_t.split_threshold)
        return false;

    return true;
}

// returns "index" on correct or "-1" on error
//
static 
int check_giga_addressing(struct giga_directory *dir, giga_pathname path, 
                          giga_result_t *rpc_reply,
                          giga_getattr_reply_t *stat_rpc_reply) 
{
    int index, server = 0;

    // (1): get the giga index/partition for operation
    index = giga_get_index_for_file(&dir->mapping, (const char*)path);
    server = giga_get_server_for_index(&dir->mapping, index);
        
    logMessage(HANDLER_LOG, __func__, "[%s]-->(p%d,s%d)", 
               path, index, server);
    
    // (2): is this the correct server? 
    // ---- NO: set rpc_reply (errnum=-EAGAIN and copy bitmap) and return
    if (server != giga_options_t.serverID) {
        if (rpc_reply != NULL) {
            assert (stat_rpc_reply == NULL);
            memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
                   &dir->mapping, sizeof(dir->mapping));
            rpc_reply->errnum = -EAGAIN;
        }
        else if (stat_rpc_reply != NULL) {
            assert (rpc_reply == NULL);
            memcpy(&(stat_rpc_reply->result.giga_result_t_u.bitmap), 
                   &dir->mapping, sizeof(dir->mapping));
            stat_rpc_reply->result.errnum = -EAGAIN;
        }
        
        logMessage(HANDLER_LOG, __func__, "ERR_redirect: to s%d, not me(s%d)",
                   server, giga_options_t.serverID);
        index = -1;
    }

    return index;
}

bool_t giga_rpc_init_1_svc(int rpc_req, 
                           giga_result_t *rpc_reply, 
                           struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);

    logMessage(HANDLER_LOG, __func__, ">>> RPC_init [req=%d]", rpc_req);

    // send bitmap for the "root" directory.
    //
    int dir_id = 0;
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        logMessage(HANDLER_LOG, __func__, "ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
    rpc_reply->errnum = -EAGAIN;
    memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
           &dir->mapping, sizeof(dir->mapping));

    logMessage(HANDLER_LOG, __func__, 
               ">>> RPC_init: [status=%d]", rpc_reply->errnum);
    return true;
}

int giga_rpc_prog_1_freeresult(SVCXPRT *transp, 
                               xdrproc_t xdr_result, caddr_t result)
{
    (void)transp;
    
    xdr_free(xdr_result, result);

    /* TODO: add more cleanup code. */
    
    //logMessage(HANDLER_LOG, __func__, ">>> RPC_freeresult <<<\n");
    return 1;
}

bool_t giga_rpc_getattr_1_svc(giga_dir_id dir_id, giga_pathname path, 
                              giga_getattr_reply_t *rpc_reply, 
                              struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(HANDLER_LOG, __func__, 
               ">>> RPC_getattr(d=%d,p=%s)", dir_id, path);

    bzero(rpc_reply, sizeof(giga_getattr_reply_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->result.errnum = -EIO;
        
        logMessage(HANDLER_LOG, __func__, "ERR_cache: dir(%d) missing", dir_id);
        return true;
    }

    /*
    // (1): get the giga index/partition for operation
    int index = giga_get_index_for_file(&dir->mapping, (const char*)path);
    int server = giga_get_server_for_index(&dir->mapping, index);
    
    // (2): is this the correct server? 
    // ---- NO: set rpc_reply (errnum=-EAGAIN and copy bitmap) and return
    if (server != giga_options_t.serverID) {
        memcpy(&(rpc_reply->result.giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->result.errnum = -EAGAIN;
        
        logMessage(HANDLER_LOG, __func__, "ERR_redirect: to s%d, not me(s%d)",
                   server, giga_options_t.serverID);
        return true;
    }
    */
    
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, NULL, rpc_reply);
    if (index < 0)
        return true;
    
    char path_name[MAX_LEN];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            rpc_reply->result.errnum = local_getattr(path_name, 
                                                     &rpc_reply->statbuf);
            break;
        case BACKEND_RPC_LEVELDB:
            rpc_reply->result.errnum = metadb_lookup(ldb_mds,
                                                     dir_id, index, path,
                                                     &rpc_reply->statbuf);
            break;  
        default:
            break;
    }

    logMessage(HANDLER_LOG, __func__, 
               "<<< RPC_getattr(d=%d,p=%s): status=[%d]", 
               dir_id, path, rpc_reply->result.errnum);
    return true;
}

bool_t giga_rpc_mknod_1_svc(giga_dir_id dir_id, 
                            giga_pathname path, mode_t mode, short dev,
                            giga_result_t *rpc_reply, 
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(HANDLER_LOG, __func__, 
               ">>> RPC_mknod(d=%d,p=%s): m=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        
        logMessage(HANDLER_LOG, __func__, "ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
  
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, rpc_reply, NULL);
    if (index < 0)
        return true;
    
    // check for splits.
    //
    if ((check_split_eligibility(dir, index) == true) &&
        (dir->split_flag != index)) {
        logMessage(HANDLER_LOG, __func__, 
                   "SPLIT p%d (%d dirents)", index, dir->partition_size[index]);
       
        // don't split an already splitting bucket, just try again
        if (dir->split_flag != index) {
            dir->split_flag = index;
            if ((rpc_reply->errnum = split_bucket(dir, index)) < 0) {
                logMessage(LOG_FATAL, __func__, "**FATAL_ERROR** during split");
                exit(1);    //TODO: do something smarter???
            }
            dir->split_flag = -1;
        }

        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->errnum = -EAGAIN;
    
        logMessage(HANDLER_LOG, __func__, "ERR_retry: split_p%d [status=%d]", 
                   rpc_reply->errnum, index);
        
        return true;
    }
   
    // regular operations (if no splits)
    //
    
    char path_name[MAX_LEN] = {0};
    
    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            //rpc_reply->errnum = local_mknod(path_name, mode, dev);
            if((rpc_reply->errnum = local_mknod(path_name, mode, dev)) < 0)
                logMessage(LOG_FATAL, __func__, "ERR_mknod(%s): [%s]",
                           path_name, strerror(rpc_reply->errnum));
            else
                dir->partition_size[index] += 1;
            break;
        case BACKEND_RPC_LEVELDB:
            // create object in the underlying file system
            // TODO: assume partitioned sub-dirs, and randomly pick a dir
            //       for symlink creation (for PanFS)
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            //rpc_reply->errnum = local_mknod(path_name, mode, dev);
            if((rpc_reply->errnum = local_mknod(path_name, mode, dev)) < 0) {
                logMessage(LOG_FATAL, __func__, "ERR_mknod(%s): [%s]",
                           path_name, strerror(rpc_reply->errnum));
                break;
            }

            logMessage(HANDLER_LOG, __func__, "__ret=%d", rpc_reply->errnum);

            // create object entry (metadata) in levelDB
            object_id += 1;  //TODO: do we need this for non-dir objects?? 
            logMessage(HANDLER_LOG, __func__, "d=%d,p=%d,o=%d,p=%s,rp=%s", 
                       dir_id, index,object_id, path,path_name);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index,
                                              OBJ_MKNOD,
                                              object_id, path, path_name);
            if (rpc_reply->errnum < 0)
                logMessage(LOG_FATAL, __func__, "ERR_mdb_create: (d%d:%s) p%d",
                           dir_id, path, index);
            else
                dir->partition_size[index] += 1;
            break;
        default:
            break;

    }

    logMessage(HANDLER_LOG, __func__, 
               "<<< RPC_mknod(d=%d,p=%s): status=[%d]", dir_id, path,
               rpc_reply->errnum);
    return true;
}


bool_t giga_rpc_mkdir_1_svc(giga_dir_id dir_id, 
                            giga_pathname path, mode_t mode,
                            giga_result_t *rpc_reply, 
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(HANDLER_LOG, __func__, 
               ">>> RPC_mkdir(d=%d,p=%s): mode=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        
        logMessage(HANDLER_LOG, __func__, "ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
    
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, rpc_reply, NULL);
    if (index < 0)
        return true;
    
    // check for splits.
    //
    if (check_split_eligibility(dir, index) == true) {
        logMessage(HANDLER_LOG, __func__, 
                   "SPLIT p%d (%d dirents)", index, dir->partition_size[index]);
        
        if ((rpc_reply->errnum = split_bucket(dir, index)) < 0) {
            logMessage(LOG_FATAL, __func__, "***FATAL_ERROR*** during split");
            exit(1);    //FIXME: do somethign smarter
        }

        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->errnum = -EAGAIN;
    
        logMessage(HANDLER_LOG, __func__, "ERR_retry: split_p%d [status=%d]", 
                   rpc_reply->errnum, index);
        
        return true;
    }

    // regular operations (if no splits)
    //
    
    char path_name[MAX_LEN];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            rpc_reply->errnum = local_mkdir(path_name, mode);
            break;
        case BACKEND_RPC_LEVELDB:
            // create object in the underlying file system
            // FIXME: we need it for NON-directory objects only. 
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            if ((rpc_reply->errnum = local_mkdir(path_name, mode)) < 0) {
                logMessage(LOG_FATAL, __func__, "ERR_mkdir(%s): [%s]",
                           path_name, strerror(rpc_reply->errnum));
                break;
            }
            
            // create object entry (metadata) in levelDB
            object_id += 1; 
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index,
                                              OBJ_DIR,
                                              object_id, path, path_name);
            if (rpc_reply->errnum < 0)
                logMessage(LOG_FATAL, __func__, "ERR_mdb_create: (d%d:%s) p%d",
                           dir_id, path, index);
            else
                dir->partition_size[index] += 1;
            break;
        default:
            break;

    }

    logMessage(HANDLER_LOG, __func__, "<<< RPC_mkdir(d=%d,p=%d): status=[%d]", 
               dir_id, path, rpc_reply->errnum);

    return true;
}


// =============================================================================
// XXX: DO NOT REMOVE THIS
//
//
    /*
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        logMessage(HANDLER_LOG, __func__, "ERR_cache: dir(%d) missing!", dir_id);
        return true;
    }

    // (1): get the giga index/partition for operation
    int index = giga_get_index_for_file(&dir->mapping, (const char*)path);
    int server = giga_get_server_for_index(&dir->mapping, index);
    
    // (2): is this the correct server? 
    // ---- NO = set rpc_reply (errnum=-EAGAIN and copy correct bitmap) and return
    if (server != giga_options_t.serverID) {
        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->errnum = -EAGAIN;
        
        logMessage(HANDLER_LOG, __func__, "ERR_redirect: send to s%d, not me(s%d)",
                   server, giga_options_t.serverID);
        return true;
    }

    // (3): check for splits.
    if (dir->partition_size[index] > SPLIT_THRESHOLD) {
        logMessage(HANDLER_LOG, __func__, 
                   "SPLIT: p%d has %d entries!.", index, dir->partition_size[index]);
        
        if ((rpc_reply->errnum = split_bucket(dir, index)) < 0) {
            logMessage(LOG_FATAL, __func__, "***FATAL_ERROR*** during split");
            exit(1);    //FIXME: do somethign smarter
        }

        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->errnum = -EAGAIN;
    
        logMessage(HANDLER_LOG, __func__, "<<< RPC_mknod: [status=%d] SPLIT_p%d", 
                   rpc_reply->errnum, index);
        
        return true;
    }
    */
    
