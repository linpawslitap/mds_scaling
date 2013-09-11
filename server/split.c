
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/rpc_giga.h"
#include "common/options.h"
#include "common/utlist.h"

#include "backends/operations.h"

#include "server.h"
#include "split.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>


#define _LEVEL_     LOG_DEBUG

#define LOG_MSG(format, ...) logMessage(_LEVEL_, __func__, format, __VA_ARGS__);

struct split_task {
    DIR_handle_t dir_id;
    index_t index;
    struct split_task *prev;
    struct split_task *next;
};

struct split_task *queue = NULL;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

static int split_in_localFS(struct giga_directory *dir,
                            int parent_index, int parent_srv,
                            int child_index, int child_srv);

static int split_in_levelDB(struct giga_directory *dir,
                            int parent_index, int parent_srv,
                            int child_index, int child_srv);


int split_bucket(struct giga_directory *dir, int partition_to_split)
{
    int ret = -1;

    int parent = partition_to_split;
    int parent_srv = giga_options_t.serverID;

    int child = giga_index_for_splitting(&(dir->mapping), parent);
    //int child_srv = child % giga_options_t.num_servers;
    int child_srv = giga_get_server_for_index(&dir->mapping, child);

    LOG_MSG("[p%d_s%d] ---> [p%d_s%d]", parent, parent_srv, child, child_srv);

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            ret = split_in_localFS(dir, parent, parent_srv, child, child_srv);
            break;
        case BACKEND_RPC_LEVELDB:
            ret = split_in_levelDB(dir, parent, parent_srv, child, child_srv);
            break;
        default:
            break;
    }

    // check return condition
    //
    if (ret == -EAGAIN) {
        //TODO: optimization follows -- exchange bitmap on splits
        //
        //update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        //ret = 0;
        //
        LOG_MSG("RETRYING: p%d(%d)-->p%d(%d)",
                parent, dir->partition_size[parent],
                child, dir->partition_size[child]);
    } else if (ret < 0) {
        LOG_MSG("FAILURE: p%d(%d)-->p%d(%d)",
                parent, dir->partition_size[parent],
                child, dir->partition_size[child]);
    } else {
        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child);

        dir->partition_size[parent] -= ret;

        if (metadb_write_bitmap(ldb_mds, dir->handle, -1, NULL,
                                &dir->mapping) != 0) {
            LOG_ERR("ERR_mdb_write_bitmap(d%d): error writing bitmap.",
                    dir->handle);
            exit(1);
        }

        LOG_MSG("SUCCESS: p%d(%d)-->p%d(%d)", parent, dir->partition_size[parent],
                                              child, ret);
        //ret = 0;
    }

    metadb_extract_clean(ldb_mds);

    return ret;
}

bool_t giga_rpc_split_1_svc(giga_dir_id dir_id,
                            int parent_index, int child_index,
                            giga_pathname path,
                            giga_bitmap mapping,
                            uint64_t min_seq, uint64_t max_seq, int num_entries,
                            giga_result_t *rpc_reply,
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_split_recv: [dir%d,(p%d-->p%d),path=%s]",
               dir_id, parent_index, child_index, path);

    ACQUIRE_MUTEX(&(ldb_mds->mtx_bulkload), "bulkload(from=%s)", path);

    bzero(rpc_reply, sizeof(giga_result_t));

    rpc_reply->errnum = metadb_bulkinsert(ldb_mds, path, min_seq, max_seq);

    if (rpc_reply->errnum < 0) {
        LOG_MSG("ERR_ldb: bulk_insert(%s) FAILED!", path);
    }
    else {
        struct giga_directory *dir = cache_lookup(&dir_id);
        if (dir == NULL) {
            LOG_MSG("ERR_fetching(d=%d): cache_miss ... try_LDB ...", dir_id);

            struct giga_directory *new =
                    new_cache_entry_with_mapping(&dir_id, &mapping);

            //need to fetch it from disk first ...
            //
            if (metadb_read_bitmap(ldb_mds, dir_id, -1, NULL, &new->mapping) < 0) {
                LOG_ERR("ERR_fetching(d=%d): LDB_miss ... create in LDB", dir_id);

                // ... fetch failed, creating a new partition entry
                int ret = metadb_create_dir(ldb_mds, dir_id, -1, NULL, &new->mapping);
                if (ret < 0) {
                    LOG_ERR("ERR_mdb_create(%s): partition entry failed", dir_id);
                    rpc_reply->errnum = ret;
                    return true;
                }
            }
            cache_insert(&dir_id, new);
            cache_release(new);

            if ((dir = cache_lookup(&dir_id)) == NULL) {
                LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
                rpc_reply->errnum = -EIO;
                return true;
            }
        }

        // update bitmap and partition size
        giga_update_mapping(&(dir->mapping), child_index);

        dir->partition_size[child_index] = num_entries;

        if (metadb_write_bitmap(ldb_mds, dir_id, -1, NULL, &dir->mapping) != 0) {
            LOG_ERR("ERR_mdb_write_bitmap(d%d): error writing bitmap.", dir_id);
            exit(1);
        }

        rpc_reply->errnum = num_entries;

        LOG_MSG("p%d: %d entries and updated bitmap.",
                child_index, dir->partition_size[child_index]);
        giga_print_mapping(&dir->mapping);

        //TODO: optimization follows -- exchange bitmap on splits
        //
        //memcpy(&(rpc_reply->result.giga_result_t_u.bitmap),
        //       &dir->mapping, sizeof(dir->mapping));
        //rpc_reply->errnum = -EAGAIN;

        cache_release(dir);
    }

    RELEASE_MUTEX(&(ldb_mds->mtx_bulkload), "bulkload(from=%s)", path);

    LOG_MSG("<<< RPC_split_recv: [status=%d]", rpc_reply->errnum);
    return true;
}

static
int split_in_localFS(struct giga_directory *dir,
                     int parent_index, int parent_srv,
                     int child_index, int child_srv)
{
    int ret = -1;

    // check if the split is a LOCAL SPLIT (move/rename) or REMOTE SPLIT (rpc)
    //
    if (child_srv == parent_srv) {

        LOG_MSG("LOCAL_split ... p[%d] of d%d", parent_index, dir->handle);
    }
    else {
        LOG_MSG("REMOTE_split ... p[%d] of d%d", parent_index, dir->handle);

        /*
        char child_path[MAX_LEN] = {0}
        snprintf(child_path, sizeof(child_path),
                 "%s/%d/", giga_options_t.mountpoint, child_index);

        if (local_mkdir(child_path, mode) < 0) {
            LOG_MSG("***ERROR*** split bkt creation @ %s", child_path);
            return ret;
        }
        */

        char parent_path[PATH_MAX] = {0};
        snprintf(parent_path, sizeof(parent_path),
                 "%s/%d/", giga_options_t.mountpoint, parent_index);

        LOG_MSG("readdir(%s)--->move_to--->srv(%d)", parent_path, child_srv);

        DIR *d_ptr;
        struct dirent *d_ent;

        if ((d_ptr = opendir(parent_path)) == NULL) {
            LOG_MSG("ERR_parent_opendir(%s): %s", parent_path, strerror(errno));
            return -1;
        }

        for (d_ent = readdir(d_ptr); d_ent != NULL; d_ent = readdir(d_ptr)) {
            if ((strcmp(d_ent->d_name, ".") == 0) ||
                (strcmp(d_ent->d_name, "..") == 0))
                continue;

            if (giga_file_migration_status(d_ent->d_name, child_index) == 0) {
                LOG_MSG("--- [%s] _stays_", d_ent->d_name);
                continue;
            }
            LOG_MSG("--- [%s] _moves_", d_ent->d_name);
        }

    }

    return ret;
}

static
int split_in_levelDB(struct giga_directory *dir,
                     int parent_index, int parent_srv,
                     int child_index, int child_srv)
{
    int ret = -1;

    char split_dir_path[PATH_MAX] = {0};
    snprintf(split_dir_path, sizeof(split_dir_path),
             "%s/sst-d%d-p%dp%d",
             giga_options_t.split_dir, dir->handle, parent_index, child_index);

    // TODO: should we even do this for local splitting?? move to remote
    // split??

    // create levelDB files and return number of entries in new partition
    //
    LOG_MSG("ldb_extract: (for d%d, p%d-->p%d) in (%s)",
            dir->handle, parent_index, child_index, split_dir_path);

    //ACQUIRE_MUTEX(&ldb_mds.mtx_extract, "extract(%s)", split_dir_path);
    mdb_seq_num_t min, max = 0;

    ret = metadb_extract_do(ldb_mds, dir->handle,
                            parent_index, child_index,
                            split_dir_path, &min, &max);
    if (ret < 0) {
        LOG_ERR("ERR_ldb: extract(p%d-->p%d)", parent_index, child_index);
        goto exit_func;
    }

    // check if the split is a LOCAL SPLIT (move/rename) or REMOTE SPLIT (rpc)
    //
    if (child_srv == parent_srv) {

        LOG_MSG("LOCAL_split ... p[%d]", child_index);

        ACQUIRE_MUTEX(&(ldb_mds->mtx_bulkload), "bulkload(%s)", split_dir_path);

        if (metadb_bulkinsert(ldb_mds, split_dir_path, min, max) < 0) {
            LOG_ERR("ERR_ldb: bulkload(%s)", split_dir_path);
            ret = -1;
        }
        else {
            dir->partition_size[child_index] = ret;
        }

        RELEASE_MUTEX(&(ldb_mds->mtx_bulkload), "bulkload(%s)", split_dir_path);

    }
    else {
        giga_result_t rpc_reply;
        CLIENT *rpc_clnt = getConnection(child_srv);

        LOG_MSG(">>> RPC_split_send: [d%d, (p%d-->p%d), send %d fds] in %s",
                dir->handle, parent_index, child_index, ret, split_dir_path);

        if (giga_rpc_split_1(dir->handle, parent_index, child_index,
                             (char*)split_dir_path, dir->mapping,
                             min, max, ret, &rpc_reply, rpc_clnt)
            != RPC_SUCCESS) {
            LOG_ERR("ERR_rpc_split(%s)", clnt_spcreateerror(split_dir_path));
            exit(1);//TODO: retry again?
        }
        ret = rpc_reply.errnum;

        LOG_MSG("<<< RPC_split_send: [status=%d]", ret);
    }

    metadb_extract_clean(ldb_mds);

exit_func:
    //RELEASE_MUTEX(&ldb_mds.mtx_extract, "extract(%d,%d)", min, max);

    return ret;
}


/*
 * XXX: WORK IN PROGRESS
 *
 */

void* split_thread(void *arg)
{
    (void)arg;

    if (pthread_mutex_lock(&queue_mutex) < 0) {
        logMessage(LOG_FATAL, __func__, "ERR_pthread: queue_mtx failed.");
    }

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
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
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
        logMessage(LOG_FATAL, __func__, "ERR_pthread: queue_mtx failed.");
    }

    struct split_task *last = NULL;
    if (queue)
        last = queue->prev;

    if (last &&
        (memcmp(&last->dir_id, dir_id, sizeof(dir_id)) == 0) &&
        (memcmp(&last->index, &index, sizeof(index)) == 0))
    {
        if (pthread_mutex_unlock(&queue_mutex) < 0) {
            logMessage(LOG_FATAL, __func__, "ERR_pthread: unlock_queue_mtx failed.");
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
        logMessage(LOG_FATAL, __func__, "ERR_pthread: signal_queue_cond");
    }

    if (pthread_mutex_unlock(&queue_mutex) < 0) {
        logMessage(LOG_FATAL, __func__, "ERR_pthread: unlock_queue_mtx failed.");
    }

    return;
}


