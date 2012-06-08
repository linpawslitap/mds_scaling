
#include "common/cache.h"
#include "common/connection.h"
#include "common/defaults.h"
#include "common/debugging.h"
#include "common/rpc_giga.h"
#include "common/options.h"
#include "common/utlist.h"

#include "backends/operations.h"

#include "server.h"
#include "split.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>

#define SPLIT_LOG LOG_DEBUG


struct split_task {
    DIR_handle_t dir_id;
    index_t index;
    struct split_task *prev;
    struct split_task *next;
};

struct split_task *queue = NULL;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

int split_bucket(struct giga_directory *dir, int partition_to_split)
{
    int ret = -1;
    int parent_index = partition_to_split;
    
    int child_index = giga_index_for_splitting(&(dir->mapping), parent_index);
    //int target_server = child_index % giga_options_t.num_servers;
    int target_server = giga_get_server_for_index(&dir->mapping, child_index);

    logMessage(SPLIT_LOG, __func__,  "[p%d_on_s%d] ---> [p%d_on_s%d]", 
               parent_index, giga_options_t.serverID, 
               child_index, target_server); 
        
    char split_dir_path[MAX_LEN] = {0};
    snprintf(split_dir_path, sizeof(split_dir_path), 
             "%s/sst-d%d-p%dp%d", 
             giga_options_t.mountpoint, dir->handle, parent_index, child_index);

    // TODO: should we even do this for local splitting?? move to remote
    // split??
    
    // create levelDB files and return number of entries in new partition
    //
    logMessage(SPLIT_LOG, __func__, "ldb_extract: (for d%d, p%d-->p%d) in (%s)", 
               dir->handle, parent_index, child_index, split_dir_path);

    ACQUIRE_MUTEX(&ldb_mds.mtx_extract, "split_extract");

    mdb_seq_num_t min, max = 0;
    ret = metadb_extract_begin(ldb_mds, dir->handle, 
                               parent_index, child_index, 
                               split_dir_path);
    if (ret < 0) {
        logMessage(LOG_FATAL, __func__, "ERR_ldb: extract_begin() FAILED!\n");
        return ret;
    }

    int num_entries = metadb_extract_do(ldb_mds, &min, &max);

    if (num_entries < 0) {
        logMessage(LOG_FATAL, __func__, "ERR_ldb: extract() FAILED!\n");
        return ret;
    }

    logMessage(SPLIT_LOG, __func__, "ldb_extract_ret: min=%d,max=%d in (%s)", 
               min, max, split_dir_path);

    // check if the split is a LOCAL SPLIT (move/rename) or REMOTE SPLIT (rpc)
    //
    if (target_server == giga_options_t.serverID+111) {
        
        logMessage(SPLIT_LOG, __func__, "LOCAL_split ...");
        
        ACQUIRE_MUTEX(&ldb_mds.mtx_bulkload, "local_split_bulkload");
        
        if ((ret = metadb_bulkinsert(ldb_mds, split_dir_path, min, max)) < 0)  
            logMessage(LOG_FATAL, __func__, "ERR_ldb: bulk_insert(%s) FAILED!", 
                       split_dir_path);
        else
            dir->partition_size[child_index] = num_entries; 
        
        RELEASE_MUTEX(&ldb_mds.mtx_bulkload, "local_split_bulkload");
    }
    else {
        giga_result_t rpc_reply;
        CLIENT *rpc_clnt = getConnection(target_server);

        logMessage(SPLIT_LOG, __func__, 
                   ">>> RPC_split_send: [d%d, (p%d-->p%d), (%d,%d)=%d files] in %s",
                   dir->handle, parent_index, child_index, 
                   min, max, num_entries, split_dir_path);
        
        if (giga_rpc_split_1(dir->handle, parent_index, child_index,
                             (char*)split_dir_path, min, max, num_entries,
                             &rpc_reply, rpc_clnt) 
            != RPC_SUCCESS) {
            logMessage(LOG_FATAL, __func__, "ERR_rpc: rpc_split failed."); 
            clnt_perror(rpc_clnt, "(rpc_split failed)");
            exit(1);//TODO: retry again?
        }
        ret = rpc_reply.errnum;
    
        logMessage(SPLIT_LOG, __func__, "<<< RPC_split_send: [status=%d]", ret);

    }
   
    // check return condition 
    //
    if (ret == -EAGAIN) {
        //TODO: optimization follows -- exchange bitmap on splits
        //
        //update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        //ret = 0;
        //
        logMessage(SPLIT_LOG, __func__, "RETRYING: p%d(%d)-->p%d(%d)",
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
    } else if (ret < 0) {
        logMessage(SPLIT_LOG, __func__, "FAILURE: p%d(%d)-->p%d(%d)", 
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
    } else {
        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child_index);
        dir->partition_size[parent_index] -= num_entries;

        logMessage(SPLIT_LOG, __func__, "SUCCESS: p%d(%d)-->p%d(%d)", 
                   parent_index, dir->partition_size[parent_index], 
                   child_index, dir->partition_size[child_index]); 
        ret = 0;
    }

    metadb_extract_end(ldb_mds);
    
    //TODO: DO WE NEED SPLIT EXTRACT?
    RELEASE_MUTEX(&ldb_mds.mtx_extract, "split_extract");
    
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

    logMessage(SPLIT_LOG, __func__, ">>> RPC_split_recv: [dir%d:p%d-->p%d,path=%s]",
               dir_id, parent_index, child_index, path);
  
    object_id += 0; //dummy FIXME

    ACQUIRE_MUTEX(&ldb_mds.mtx_bulkload, "split_bulkload");
    
    rpc_reply->errnum = metadb_bulkinsert(ldb_mds, path, min_seq, max_seq);
    
    if (rpc_reply->errnum < 0) {
        logMessage(SPLIT_LOG, __func__, "ERR_ldb: bulk_insert(%s) FAILED!", 
                   path);
    }
    else { 
        struct giga_directory *dir = cache_fetch(&dir_id);
        if (dir == NULL) {
            rpc_reply->errnum = -EIO;
            
            logMessage(SPLIT_LOG, __func__, 
                       "ERR_cache: dir(%d) missing!", dir_id);
            return true;
        }

        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child_index);
        dir->partition_size[child_index] = num_entries; 
        
        logMessage(SPLIT_LOG, __func__, "p%d: %d entries and updated bitmap.", 
                   child_index, dir->partition_size[child_index]); 
        
        //TODO: optimization follows -- exchange bitmap on splits
        //
        //memcpy(&(rpc_reply->result.giga_result_t_u.bitmap), 
        //       &dir->mapping, sizeof(dir->mapping));
        //rpc_reply->errnum = -EAGAIN;  
    }
    
    RELEASE_MUTEX(&ldb_mds.mtx_bulkload, "split_bulkload");

    logMessage(SPLIT_LOG, __func__, "<<< RPC_split_recv: [status=%d]", 
               rpc_reply->errnum);
    return true;
}

/*
 * XXX: WORK IN PROGRESS
 *
 */

void* split_thread(void *arg)
{
    (void)arg;

    if (pthread_mutex_lock(&queue_mutex) < 0) {
        logMessage(SPLIT_LOG, __func__, "ERR_pthread: mutex failed.");
    }

    object_id += 0;

    while (!pthread_cond_wait(&queue_cond, &queue_mutex)) {
        while (queue) {
            struct split_task *task = queue;
            DL_DELETE(queue, task);

            pthread_mutex_unlock(&queue_mutex);
            process_split(task->dir_id, task->index);
            pthread_mutex_lock(&queue_mutex);

            free(task);
        }
    }

    return 0;
}

void process_split(DIR_handle_t dir_id, index_t index)
{
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(SPLIT_LOG, __func__, "ERR_cache: dir(%d) missing", dir_id);
        return;
    }
   
    logMessage(LOG_FATAL, __func__, "during split p%d", index);
    if (split_bucket(dir, index) < 0) {
        logMessage(LOG_FATAL, __func__, "***FATAL_ERROR*** during split");
        return;    //FIXME: do somethign smarter
    }

    return;
}

void issue_split(DIR_handle_t *dir_id, index_t index)
{

    if (pthread_mutex_lock(&queue_mutex) < 0) {
        logMessage(SPLIT_LOG, __func__, "ERR_pthread: lock_queue_mtx failed.");
    }

    struct split_task *last = NULL;
    if (queue)
        last = queue->prev;
    
    if (last && 
        (memcmp(&last->dir_id, dir_id, sizeof(dir_id)) == 0) &&
        (memcmp(&last->index, &index, sizeof(index)) == 0))
    {
        if (pthread_mutex_unlock(&queue_mutex) < 0) {
            logMessage(SPLIT_LOG, __func__, "ERR_pthread: unlock_queue_mtx.");
        }

        return;
    }

    struct split_task *task = (struct split_task*)malloc(sizeof(struct split_task));
    if (task == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        return;
    }

    task->dir_id = *dir_id;
    task->index = index;

    DL_APPEND(queue, task);

    if (pthread_cond_signal(&queue_cond) < 0) {
        logMessage(SPLIT_LOG, __func__, "ERR_pthread: signal_queue_cond");
    }

    if (pthread_mutex_unlock(&queue_mutex) < 0) {
        logMessage(SPLIT_LOG, __func__, "ERR_pthread: unlock_queue_mtx.");
    }

    return;
}


