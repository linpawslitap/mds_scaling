
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/rpc_giga.h"
#include "common/giga_index.h"

#include "backends/operations.h"

#include "server.h"
#include "split.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>

#define _LEVEL_     LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(_LEVEL_, __func__, format, __VA_ARGS__); 

static
int check_split_eligibility(struct giga_directory *dir, int index)
{
    if ((dir->partition_size[index] >= giga_options_t.split_threshold) &&
        (dir->split_flag == 0) &&
        (giga_is_splittable(&dir->mapping, index) == 1)) { 
        return true;
    }

    return false;
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
        
    LOG_MSG("[%s]-->(p%d,s%d)", path, index, server);
    
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
        else {
            LOG_ERR("ERR_giga_check(%s): both NULL!! fixit!", path);
            exit(1);
        }
        
        LOG_MSG("ERR_redirect: to s%d, not me(s%d)",
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

    LOG_MSG(">>> RPC_init [req=%d]", rpc_req);

    // send bitmap for the "root" directory.
    //
    int dir_id = 0;
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
    rpc_reply->errnum = -EAGAIN;
    memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
           &dir->mapping, sizeof(dir->mapping));

    LOG_MSG("<<< RPC_init: [status=%d]", rpc_reply->errnum);

    return true;
}

int giga_rpc_prog_1_freeresult(SVCXPRT *transp, 
                               xdrproc_t xdr_result, caddr_t result)
{
    (void)transp;
    
    xdr_free(xdr_result, result);

    /* TODO: add more cleanup code. */
    
    //LOG_MSG(">>> RPC_freeresult <<<\n");
    return 1;
}

bool_t giga_rpc_getattr_1_svc(giga_dir_id dir_id, giga_pathname path, 
                              giga_getattr_reply_t *rpc_reply, 
                              struct svc_req *rqstp)
{
    (void)rqstp;
    
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_getattr(d=%d,p=%s)", dir_id, path);

    bzero(rpc_reply, sizeof(giga_getattr_reply_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->result.errnum = -EIO;
        
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, NULL, rpc_reply);
    if (index < 0)
        return true;
    
    char path_name[PATH_MAX];

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

    LOG_MSG("<<< RPC_getattr(d=%d,p=%s): status=[%d]", 
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

    LOG_MSG(">>> RPC_mknod(d=%d,p=%s): m=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }

    int index = 0;

start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, rpc_reply, NULL)) < 0)
        return true;
    
    ACQUIRE_MUTEX(&dir->partition_mtx[index], "mknod(%s)", path);
    
    if(check_giga_addressing(dir, path, rpc_reply, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "mknod(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: mknod(%s) for p(%d) changed.", path, index);
        goto start;
    }
    
    // check for splits.
    if ((check_split_eligibility(dir, index) == true)) {
        ACQUIRE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        dir->split_flag = 1;
        RELEASE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        
        LOG_MSG("SPLIT_p%d[%d entries] caused_by=[%s]", 
                index, dir->partition_size[index], path);
       
        rpc_reply->errnum = split_bucket(dir, index);
        if (rpc_reply->errnum == -EAGAIN) {
            LOG_MSG("ERR_retry: split_p%d", index);
        } else if (rpc_reply->errnum < 0) {
            LOG_ERR("**FATAL_ERROR** during split of p%d", index);
            exit(1);    //TODO: do something smarter???
        } else {
            memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
                   &dir->mapping, sizeof(dir->mapping));
            rpc_reply->errnum = -EAGAIN;
        }

        ACQUIRE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
        dir->split_flag = 0;
        RELEASE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
       
        goto exit_func;
    }
   
    // regular operations (if no splits)
    
    char path_name[PATH_MAX] = {0};
    
    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            if ((rpc_reply->errnum = local_mknod(path_name, mode, dev)) < 0)
                LOG_ERR("ERR_mknod(%s): [%s]", 
                        path_name, strerror(rpc_reply->errnum));
            else
                dir->partition_size[index] += 1;
            break;
        case BACKEND_RPC_LEVELDB:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            
            // TODO: create object in the underlying file system - when big?
            //       for symlink creation (for PanFS)

            // create object entry (metadata) in levelDB
            object_id += 1;  //TODO: do we need this for non-dir objects?? 
            LOG_MSG("d=%d,p=%d,o=%d,p=%s,rp=%s", 
                    dir_id, index,object_id, path,path_name);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index,
                                              OBJ_MKNOD,
                                              object_id, path, path_name);
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            else
                dir->partition_size[index] += 1;
            break;
        default:
            break;
    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx[index], "mknod(%s)", path);

    LOG_MSG("<<< RPC_mknod(d=%d,p=%s): status=[%d]", dir_id, path,
            rpc_reply->errnum);
    return true;
}

bool_t giga_rpc_create_1_svc(giga_dir_id dir_id, 
                             giga_pathname path, mode_t mode, 
                             giga_lookup_t *rpc_reply, 
                             struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_create(d=%d,p=%s): m=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_lookup_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }

    int index = 0;

start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, NULL, NULL)) < 0)
        return true;
    
    ACQUIRE_MUTEX(&dir->partition_mtx[index], "create(%s)", path);
    
    if(check_giga_addressing(dir, path, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "create(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: create(%s) for p(%d) changed.", path, index);
        goto start;
    }
    
    // check for splits.
    if ((check_split_eligibility(dir, index) == true)) {
        ACQUIRE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        dir->split_flag = 1;
        RELEASE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        
        LOG_MSG("SPLIT_p%d[%d entries] caused_by=[%s]", 
                index, dir->partition_size[index], path);
      
        rpc_reply->errnum = split_bucket(dir, index);
        if (rpc_reply->errnum == -EAGAIN) {
            LOG_MSG("ERR_retry: split_p%d", index);
        } else if (rpc_reply->errnum < 0) {
            LOG_ERR("**FATAL_ERROR** during split of p%d", index);
            exit(1);    //TODO: do something smarter???
        } else {
            memcpy(&(rpc_reply->giga_lookup_t_u.bitmap), 
                   &dir->mapping, sizeof(dir->mapping));
            rpc_reply->errnum = -EAGAIN;
            rpc_reply->giga_lookup_t_u.path = NULL;
        }

        ACQUIRE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
        dir->split_flag = 0;
        RELEASE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
       
        goto exit_func;
    }
   
    // regular operations (if no splits)
    
    char path_name[PATH_MAX] = {0};
    
    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            if ((rpc_reply->errnum = creat(path_name, mode)) < 0) {
                LOG_ERR("ERR_create(%s): [%s]", 
                        path_name, strerror(rpc_reply->errnum));
            } else {
                dir->partition_size[index] += 1;
                rpc_reply->errnum = 0;
                rpc_reply->giga_lookup_t_u.path = path_name;
            }
            break;
        case BACKEND_RPC_LEVELDB:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            
            // TODO: create object in the underlying file system - when big?
            //       for symlink creation (for PanFS)

            // create object entry (metadata) in levelDB
            object_id += 1;  //TODO: do we need this for non-dir objects?? 
            LOG_MSG("d=%d,p=%d,o=%d,p=%s,rp=%s", 
                    dir_id, index,object_id, path,path_name);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index,
                                              OBJ_MKNOD,
                                              object_id, path, path_name);
            if (rpc_reply->errnum < 0) {
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            } else {
                dir->partition_size[index] += 1;
                rpc_reply->errnum = 0;
                rpc_reply->giga_lookup_t_u.path = NULL; //FIXME?? what to pass?
            }
            break;
        default:
            break;
    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx[index], "mknod(%s)", path);

    LOG_MSG("<<< RPC_mknod(d=%d,p=%s): status=[%d]", dir_id, path,
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

    LOG_MSG(">>> RPC_mkdir(d=%d,p=%s): mode=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }

    int index = 0;

start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, rpc_reply, NULL)) < 0)
        return true;
    
    ACQUIRE_MUTEX(&dir->partition_mtx[index], "mkdir(%s)", path);
    
    if(check_giga_addressing(dir, path, rpc_reply, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "mkdir(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: mkdir(%s) for p(%d) changed.", path, index);
        goto start;
    }
    
    // check for splits.
    if ((check_split_eligibility(dir, index) == true)) {
        ACQUIRE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        dir->split_flag = 1;
        RELEASE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        
        LOG_MSG("SPLIT_p%d[%d entries] caused_by=[%s]", 
                index, dir->partition_size[index], path);
       
        rpc_reply->errnum = split_bucket(dir, index);
        if (rpc_reply->errnum == -EAGAIN) {
            LOG_MSG("ERR_retry: split_p%d", index);
        } else if (rpc_reply->errnum < 0) {
            LOG_ERR("**FATAL_ERROR** during split of p%d", index);
            exit(1);    //TODO: do something smarter???
        } else {
            memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
                   &dir->mapping, sizeof(dir->mapping));
            rpc_reply->errnum = -EAGAIN;
        }

        ACQUIRE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
        dir->split_flag = 0;
        RELEASE_MUTEX(&dir->split_mtx, "reset_split(p%d)", index);
       
        goto exit_func;
    }
    
    // regular operations (if no splits)
    //
    
    char path_name[PATH_MAX];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(path_name, sizeof(path_name), 
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            rpc_reply->errnum = local_mkdir(path_name, mode);
            break;
        case BACKEND_RPC_LEVELDB:
            // create object in the underlying file system
            // FIXME: we need it for NON-directory objects only.
            /*
            snprintf(path_name, sizeof(path_name), 
                     "%s/%s", giga_options_t.mountpoint, path);
            if ((rpc_reply->errnum = local_mkdir(path_name, mode)) < 0) {
                LOG_ERR("ERR_mkdir(%s): [%s]",
                        path_name, strerror(rpc_reply->errnum));
                break;
            }
            */
            
            // create object entry (metadata) in levelDB
            object_id += 1;  //TODO: do we need this for non-dir objects?? 
            LOG_MSG("d=%d,p=%d,o=%d,p=%s,rp=%s", 
                    dir_id, index,object_id, path,path_name);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index,
                                              OBJ_DIR,
                                              object_id, path, path_name);
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            else
                dir->partition_size[index] += 1;
            break;
        default:
            break;

    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx[index], "mkdir(%s)", path);

    LOG_MSG("<<< RPC_mkdir(d=%d,p=%d): status=[%d]", 
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
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
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
        
        LOG_MSG("ERR_redirect: send to s%d, not me(s%d)",
                   server, giga_options_t.serverID);
        return true;
    }

    // (3): check for splits.
    if (dir->partition_size[index] > SPLIT_THRESHOLD) {
        LOG_MSG("SPLIT: p%d has %d entries", index, dir->partition_size[index]);
        
        if ((rpc_reply->errnum = split_bucket(dir, index)) < 0) {
            logMessage(LOG_FATAL, __func__, "***FATAL_ERROR*** during split");
            exit(1);    //FIXME: do somethign smarter
        }

        memcpy(&(rpc_reply->giga_result_t_u.bitmap), 
               &dir->mapping, sizeof(dir->mapping));
        rpc_reply->errnum = -EAGAIN;
    
        LOG_MSG("<<< RPC_mknod: [ret=%d] SPLIT_p%d", rpc_reply->errnum, index);
        
        return true;
    }
    */
    
