
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

int split_bucket(struct giga_directory *dir, int partition_to_split)
{
    int ret = -1;
    int parent_index = partition_to_split;
    
    int child_index = giga_index_for_splitting(&(dir->mapping), parent_index);
    //int destination_server = child_index % giga_options_t.num_servers;
    int destination_server = giga_get_server_for_index(&dir->mapping, child_index);

    logMessage(HANDLER_LOG, __func__,  "[p%d_on_s%d] ---> [p%d_on_s%d]", 
               parent_index, giga_options_t.serverID, 
               child_index, destination_server); 
        
    char split_dir_path[MAX_LEN] = {0};
    snprintf(split_dir_path, sizeof(split_dir_path), 
             "%s/split-d%d-p%d-p%d", 
             giga_options_t.mountpoint, dir->handle, parent_index, child_index);

    // FIXME: should we even do this for local splitting?? move to remote
    // split??
    
    // create levelDB files and return number of entries in new partition
    //
    logMessage(HANDLER_LOG, __func__, "ldb_extract ...");
    
    mdb_seq_num_t min, max;

    int num_entries = metadb_extract(ldb_mds, dir->handle, 
                                     parent_index, child_index, 
                                     split_dir_path, &min, &max);
    if (num_entries < 0) {
        logMessage(LOG_FATAL, __func__, "ERROR_ldb: extract() FAILED!\n");
        return ret;
    }

    // check if the split is a LOCAL SPLIT (move/rename) or REMOTE SPLIT (rpc)
    //
    if (destination_server == giga_options_t.serverID) {
        
        logMessage(HANDLER_LOG, __func__, "LOCAL_split");
        
        if ((ret = metadb_bulkinsert(ldb_mds, split_dir_path, min, max)) < 0)  
            logMessage(LOG_FATAL, __func__, "ERROR_ldb: bulk_insert(%s) FAILED!", 
                       split_dir_path);
        else
            dir->partition_size[child_index] = num_entries; 
    }
    else {
        logMessage(HANDLER_LOG, __func__, "RPC_split");
        
        giga_result_t rpc_reply;
        CLIENT *rpc_clnt = getConnection(destination_server);

        if (giga_rpc_split_1(dir->handle, parent_index, child_index,
                             (char*)split_dir_path, min, max, num_entries,
                             &rpc_reply, rpc_clnt) 
            != RPC_SUCCESS) {
            logMessage(LOG_FATAL, __func__, "ERROR_rpc: rpc_split failed."); 
            clnt_perror(rpc_clnt, "(rpc_split failed)");
            exit(1);//TODO: retry again?
        }
        
        ret = rpc_reply.errnum;
    }
   
    // check return condition 
    //
    if (ret == -EAGAIN) {
        //TODO: optimization follows -- exchange bitmap on splits
        //
        //update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        //ret = 0;
        //
        logMessage(HANDLER_LOG, __func__, "status: p%d(%d)-->p%d(%d) -- RETRY ...", 
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
    } else if (ret < 0) {
        logMessage(HANDLER_LOG, __func__, "status: p%d(%d)-->p%d(%d) -- FAILURE!", 
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
    } else {
        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child_index);
        dir->partition_size[parent_index] -= num_entries;

        logMessage(HANDLER_LOG, __func__, "status: p%d(%d)-->p%d(%d) -- SUCCESS.", 
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
        ret = 0;
    }

    return ret;
}

bool_t giga_rpc_split_1_svc(giga_dir_id dir_id, 
                            int parent_index, int child_index,
                            giga_pathname path, 
                            uint64_t min_seq, uint64_t max_seq, int num_entries,
                            giga_result_t *rpc_reply, 
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    logMessage(HANDLER_LOG, __func__, ">>> RPC_split: [dir=%d, p%d-->p%d, path=%s}",
               dir_id, parent_index, child_index, path);
    
    object_id += 1; //FIXME: need to move this.

    rpc_reply->errnum = metadb_bulkinsert(ldb_mds, path, min_seq, max_seq);
    
    if (rpc_reply->errnum < 0) {
        logMessage(HANDLER_LOG, __func__, "ERROR_ldb: bulk_insert(%s) FAILED!", path);
    }
    else { 
        struct giga_directory *dir = cache_fetch(&dir_id);
        if (dir == NULL) {
            rpc_reply->errnum = -EIO;
            
            logMessage(HANDLER_LOG, __func__, 
                       "ERROR_cache: dir(%d) missing!", dir_id);
            return true;
        }

        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child_index);
        dir->partition_size[child_index] = num_entries; 
        
        logMessage(HANDLER_LOG, __func__, "p%d: %d entries and updated bitmap.", 
                   child_index, dir->partition_size[child_index]); 
        
        //TODO: optimization follows -- exchange bitmap on splits
        //
        //memcpy(&(rpc_reply->result.giga_result_t_u.bitmap), 
        //       &dir->mapping, sizeof(dir->mapping));
        //rpc_reply->errnum = -EAGAIN;  
    }

    logMessage(HANDLER_LOG, __func__, "<<< RPC_split: [status=%d]", 
               rpc_reply->errnum);
    return true;
}



