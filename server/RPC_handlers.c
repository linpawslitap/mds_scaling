
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

        LOG_MSG("ERR_redirect: to s%d, not me(s%d)",
                server, giga_options_t.serverID);
        index = -1;
    }

    return index;
}

static
struct giga_directory* fetch_dir_mapping(giga_dir_id dir_id)
{
    struct giga_directory *dir = NULL;

    if ((dir = cache_lookup(&dir_id)) == NULL) {
        // if cache miss, get from LDB
        //
        LOG_MSG("ERR_fetching(d=%d): cache_miss ... try_LDB ...", dir_id);

        int zeroth_srv = 0;
        struct giga_directory *new = new_cache_entry(&dir_id, zeroth_srv);

        if (metadb_read_bitmap(ldb_mds, dir_id, -1, NULL, &new->mapping) != 0) {
            LOG_ERR("ERR_fetching(d=%d): LDB_miss!", dir_id);
            return NULL;
            //exit(1);
        }

        cache_insert(&dir_id, new);

        assert(new);
        LOG_MSG("SUCCESS_fetching(d=%d): sending mapping", dir_id);
        return new;
    }

    assert(dir);
    LOG_MSG("SUCCESS_fetching(d=%d): sending mapping", dir_id);
    return dir;
}

static
void release_dir_mapping(struct giga_directory *dir)
{
    cache_release(dir);
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
    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    /*
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
    */
    rpc_reply->errnum = -EAGAIN;
    memcpy(&(rpc_reply->giga_result_t_u.bitmap), &dir->mapping, sizeof(dir->mapping));

    release_dir_mapping(dir);

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

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    /*
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {

        int zeroth_srv = 0;
        struct giga_directory *new = new_cache_entry(&dir_id, zeroth_srv);
        if (metadb_read_bitmap(ldb_mds, dir_id, -1, NULL, &new->mapping) != 0) {
        //rpc_reply->result.errnum = -EIO;
        //LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        //return true;
    }
    */

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

            // check if stat-ing a directory, return the mapping structure
            // stored in LDB for the dentry
            // remember that for a dentry that is a directory, we store the
            // statbuf and mapping as the value in LDB.
            //
            if (S_ISDIR(rpc_reply->statbuf.st_mode)) {
                LOG_MSG("rpc_getattr(%s) returns a directory for d%d",
                        path, rpc_reply->statbuf.st_ino);

                //struct giga_directory *tmp = new_cache_entry((DIR_handle_t*)&rpc_reply->statbuf.st_ino, 0);
                struct giga_directory *tmp = (struct giga_directory*)malloc(sizeof(struct giga_directory));
                if (metadb_read_bitmap(ldb_mds, dir_id, index, path, &tmp->mapping) != 0) {
                    LOG_ERR("mdb_read(%s): error reading dir=%d bitmap.", path, dir_id);
                    exit(1);
                }
                //cache_insert((DIR_handle_t*)&rpc_reply->statbuf.st_ino, tmp);

                memcpy(&(rpc_reply->result.giga_result_t_u.bitmap),
                       &tmp->mapping, sizeof(tmp->mapping));
                giga_print_mapping(&rpc_reply->result.giga_result_t_u.bitmap);
                rpc_reply->result.errnum = -EAGAIN;

                //cache_release(tmp);
            }
            rpc_reply->file_size = rpc_reply->statbuf.st_size;

            break;
        default:
            break;
    }

    release_dir_mapping(dir);

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

    struct giga_directory *dir = fetch_dir_mapping(dir_id);

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
            if (metadb_write_bitmap(ldb_mds, dir_id, -1, NULL, &dir->mapping) != 0) {
                LOG_ERR("mdb_write_bitmap(d%d): error writing bitmap.", dir_id);
                exit(1);
            }
            giga_print_mapping(&dir->mapping);
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

            // create object entry (metadata) in levelDB

            LOG_MSG("d=%d,p=%d,p=%s", dir_id, index, path);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index, path, "");
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

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_mknod(d=%d,p=%s): status=[%d]", dir_id, path,
            rpc_reply->errnum);
    return true;
}

bool_t giga_rpc_readdir_serial_1_svc(scan_args_t args,
                                     readdir_return_t *rpc_reply,
                                     struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);

    bzero(rpc_reply, sizeof(readdir_return_t));

    int dir_id = args.dir_id;

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = ENOENT;
        //rpc_reply->readdir_return_t_u.result.num_entries = 0;
        LOG_MSG("ERR_mdb_readdir: d%d no present at this server.", dir_id);
        return true;
    }

    int partition_id = args.partition_id;

    LOG_MSG(">>> RPC_readdir(d=%d, p[%d])", dir_id, partition_id);

    char *buf;
    if ((buf = (char*)malloc(sizeof(char)*MAX_BUF)) == NULL) {
        LOG_ERR("ERR_malloc: [%s]", strerror(errno));
        exit(1);
    }

    char end_key[PATH_MAX] = {0};
    int num_ent = 0;
    int more_ents_flag = 0;

    scan_list_t ls;
    scan_list_t *ls_ptr = &(rpc_reply->readdir_return_t_u.result.list);

    LOG_MSG("readdir(%d:%d:start@[%s])", dir_id, partition_id, args.start_key.scan_key_val);
    rpc_reply->errnum = metadb_readdir(ldb_mds, dir_id, &partition_id,
                                       args.start_key.scan_key_val, buf, MAX_BUF,
                                       &num_ent, end_key, &more_ents_flag);
    if (rpc_reply->errnum == ENOENT) {
        LOG_ERR("ERR_mdb_readdir(d%d_p%d_sk[%s]) ...",
                dir_id, partition_id, args.start_key.scan_key_val);
        goto exit_func;
    }

    metadb_readdir_iterator_t *iter = NULL;

    iter = metadb_create_readdir_iterator(buf, MAX_BUF, num_ent);
    metadb_readdir_iter_begin(iter);

    // copy all entries in the linked list to be returned
    int entry = 0;
    LOG_MSG("PRINT_readdir() buf with %d entries ...", num_ent);
    while (metadb_readdir_iter_valid(iter)) {
        size_t len;

        const char* objname = metadb_readdir_iter_get_objname(iter, &len);
        const struct stat* statbuf = metadb_readdir_iter_get_stat(iter);

        ls = *ls_ptr = (scan_entry_t*)malloc(sizeof(scan_entry_t));
        ls->entry_name = strdup(objname);
        ls->info.permission = statbuf->st_mode
                            & (S_IRWXU | S_IRWXG | S_IRWXO);
        ls->info.is_dir = S_ISDIR(statbuf->st_mode);
        ls->info.uid = statbuf->st_uid;
        ls->info.uid = statbuf->st_gid;
        ls->info.size = statbuf->st_size;
        ls->info.atime = statbuf->st_atime;
        ls->info.ctime = statbuf->st_ctime;
        ls_ptr = &ls->next;

        entry += 1;

        metadb_readdir_iter_next(iter);
    }
    *ls_ptr = NULL;
    metadb_destroy_readdir_iterator(iter);

    rpc_reply->readdir_return_t_u.result.more_entries_flag = more_ents_flag;
    rpc_reply->readdir_return_t_u.result.num_entries = num_ent;
    rpc_reply->readdir_return_t_u.result.end_partition = partition_id;

    int key_len = strlen(end_key);
    rpc_reply->readdir_return_t_u.result.end_key.scan_key_val = strdup(end_key);
    rpc_reply->readdir_return_t_u.result.end_key.scan_key_val[key_len] = '\0';
    rpc_reply->readdir_return_t_u.result.end_key.scan_key_len = strlen(rpc_reply->readdir_return_t_u.result.end_key.scan_key_val);

    LOG_MSG("readdir_ret: end_key=[%s],len=%d",
            rpc_reply->readdir_return_t_u.result.end_key.scan_key_val,
            rpc_reply->readdir_return_t_u.result.end_key.scan_key_len);

    if (more_ents_flag == 0) {
        memcpy(&(rpc_reply->readdir_return_t_u.result.bitmap),
               &dir->mapping, sizeof(dir->mapping));
    }

    free(buf);

exit_func:
    release_dir_mapping(dir);
    LOG_MSG("<<< RPC_readdir(d=%d, p[%d]): status=[%d]",
            dir_id, partition_id, rpc_reply->errnum);

    return true;
}

/*
bool_t giga_rpc_readdir_1_svc(giga_dir_id dir_id, int partition_id,
                              readdir_result_t *rpc_reply,
                              struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);

    LOG_MSG(">>> RPC_readdir(d=%d, p[%d])", dir_id, partition_id);

    bzero(rpc_reply, sizeof(readdir_result_t));

    //int ret = metadb_readdir(ldb_mds, dir_id, partition_id, buf, NULL);
    char *buf = (char*)malloc(sizeof(char)*MAX_BUF);
    if (buf == NULL) {
        LOG_ERR("ERR_malloc: [%s]", strerror(errno));
        exit(1);
    }
    char *start_key = NULL;
    char end_key[MAX_LEN];
    int num_ent = 0;
    int more_ents_flag = 0;

    scan_list_t ls;
    scan_list_t *ls_ptr = &(rpc_reply->readdir_result_t_u.list);

    do {
        rpc_reply->errnum = metadb_readdir(ldb_mds, dir_id, partition_id,
                                           start_key, buf, MAX_BUF,
                                           &num_ent, end_key, &more_ents_flag);
        if (rpc_reply->errnum == ENOENT) {
            LOG_ERR("ERR_mdb_readdir(d%d_p%d_sk[%s]) ...",
                    dir_id, partition_id, start_key);
            break;
        }

        if (end_key == NULL)
            LOG_MSG("readdir(%d:%d): end_key NULL", dir_id, partition_id);

        if (start_key != NULL)
            free(start_key);

        metadb_readdir_iterator_t *iter = NULL;

        iter = metadb_create_readdir_iterator(buf, MAX_BUF, num_ent);
        metadb_readdir_iter_begin(iter);

        int entry = 0;
        LOG_MSG("PRINT_readdir() buf with %d entries ...", num_ent);
        while (metadb_readdir_iter_valid(iter)) {
            size_t len;
            struct stat statbuf;

            const char* objname = metadb_readdir_iter_get_objname(iter, &len);
            const char* realpath = metadb_readdir_iter_get_realpath(iter, &len);

            metadb_readdir_iter_get_stat(iter, &statbuf);

            assert((statbuf.st_mode & S_IFDIR) > 0);
            assert(memcmp(objname, realpath, len) == 0);

            LOG_MSG("#%d: \t obj=[%s] \t sym=[%s]", entry, objname, realpath);

            ls = *ls_ptr = (scan_entry_t*)malloc(sizeof(scan_entry_t));
            ls->entry_name = strdup(objname);
            ls_ptr = &ls->next;

            entry += 1;

            metadb_readdir_iter_next(iter);
        }
        *ls_ptr = NULL;
        metadb_destroy_readdir_iterator(iter);
        start_key = end_key;
    } while (more_ents_flag != 0);

    free(buf);

    LOG_MSG(">>> RPC_readdir(d=%d, p[%d]): status=[%d]",
            dir_id, partition_id, rpc_reply->errnum);

    return true;
}
*/

void create_dir_in_storage(int object_id) {
    char path_name[PATH_MAX];
#ifdef PANFS
    int vol_i;
    for (vol_i=0; vol_i < giga_options_t.num_pfs_volumes; ++vol_i) {
        sprintf(path_name, "%s/files/%d",
                giga_options_t.pfs_volumes[vol_i], object_id);
        local_mkdir(path_name, DEFAULT_MODE);
    }
#endif
#ifdef NFS
    sprintf(path_name, "%s/files/%d",
            DEFAULT_FILE_VOL, object_id);
    local_mkdir(path_name, DEFAULT_MODE);
#else
    (void) path_name;
    (void) object_id;
#endif
#ifdef PVFS
    sprintf(path_name, "%s/files/%d",
            DEFAULT_FILE_VOL, object_id);
    local_mkdir(path_name, DEFAULT_MODE);
#endif

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
    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    /*
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
        rpc_reply->errnum = -EIO;
        LOG_MSG("ERR_cache: dir(%d) missing", dir_id);
        return true;
    }
    */

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
            if (metadb_write_bitmap(ldb_mds, dir_id, -1, NULL, &dir->mapping) != 0) {
                LOG_ERR("mdb_write_bitmap(d%d): error reading bitmap.", dir_id);
                exit(1);
            }
            giga_print_mapping(&dir->mapping);
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

    if (giga_options_t.backend_type == BACKEND_RPC_LOCALFS) {
            snprintf(path_name, sizeof(path_name),
                     "%s/%d/%s", giga_options_t.mountpoint, index, path);
            rpc_reply->errnum = local_mkdir(path_name, mode);
    }
    else if (giga_options_t.backend_type == BACKEND_RPC_LEVELDB) {
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
            // and create partition entry for this object

            int object_id = metadb_get_next_inode_count(ldb_mds);
            int zeroth_server = giga_options_t.serverID;  //FIXME: randomize please!
            struct giga_directory *new_dir = new_cache_entry(&object_id, zeroth_server);
            cache_insert(&object_id, new_dir);

            LOG_MSG("d=%d,p=%d,o=%d,p=%s", dir_id, index, object_id, path);

            rpc_reply->errnum = metadb_create_dir(ldb_mds, dir_id, index,
                                                  path, &new_dir->mapping);
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            else {
                dir->partition_size[index] += 1;

                // create the partition entry ...
                //XXX: needs an RPC because zeroth server could be elsewhere
                rpc_reply->errnum = metadb_create_dir(ldb_mds, object_id, -1, NULL,
                                                      &new_dir->mapping);
                if (rpc_reply->errnum < 0)
                    LOG_ERR("ERR_mdb_create(%s): partition entry, d%d", path, object_id);

                create_dir_in_storage(object_id);
            }

            cache_release(new_dir);
    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx[index], "mkdir(%s)", path);

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_mkdir(d=%d,p=%d): status=[%d]",
            dir_id, path, rpc_reply->errnum);

    return true;
}

bool_t giga_rpc_read_1_svc(giga_dir_id dir_id, giga_pathname path,
                            int size, int offset,
                            giga_read_reply_t *rpc_reply,
                            struct svc_req *rqstp)
{
    (void)rqstp;

    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_read(d=%d,p=%s,offset=%d,size=%d)",
            dir_id, path, offset, size);

    bzero(rpc_reply, sizeof(giga_read_reply_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result), NULL);
    if (index < 0) {
        rpc_reply->result.errnum = 1;
        return true;
    }

    int state;
    int buf_len;
    char buf[FILE_THRESHOLD];

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
            rpc_reply->result.errnum
                  = metadb_get_file(ldb_mds,
                                   dir_id, index, path,
                                   &state, buf, &buf_len);
            if (rpc_reply->result.errnum == 0) {
                switch (state) {
                  case RPC_LEVELDB_FILE_IN_DB:
                      rpc_reply->data.state = state;
                      if (offset + size > buf_len)
                          size = buf_len - offset;
                      if (size > 0) {
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_val
                              = (char*) malloc(size);
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_len
                              = size;
                          memcpy(
                            rpc_reply->data.giga_read_t_u.buf.giga_file_data_val
                            ,buf+offset, size);
                          rpc_reply->result.errnum = size;
                      } else {
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_val
                              = NULL;
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_len
                              = 0;
                      }
                      break;
                  case RPC_LEVELDB_FILE_IN_FS:
                      rpc_reply->result.errnum = 0;
                      rpc_reply->data.state = state;
                      rpc_reply->data.giga_read_t_u.link = strdup(buf);
                      break;
                  default:
                    break;
                }
            }
            break;
        default:
            break;
    }

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_read(d=%d,p=%s): state=[%d],status=[%d]",
            dir_id, path, state, rpc_reply->result.errnum);
    return true;

}

const char* get_storage_location() {
#ifdef PANFS
    int id = rand() % giga_options_t.num_pfs_volumes;
    return giga_options_t.pfs_volumes[id];
#endif
#ifdef NFS
    return DEFAULT_FILE_VOL;
#endif
#ifdef PVFS
    return DEFAULT_FILE_VOL;
#endif

    return NULL;
}

bool_t giga_rpc_write_1_svc(giga_dir_id dir_id, giga_pathname path,
                            giga_file_data data, int offset,
                            giga_write_reply_t *rpc_reply,
                            struct svc_req *rqstp)
{
    (void)rqstp;

    assert(rpc_reply);
    assert(path);

    int size = data.giga_file_data_len;

    LOG_MSG(">>> RPC_write(d=%d,p=%s,offset=%d,size=%d)",
            dir_id, path, offset, size);

    bzero(rpc_reply, sizeof(giga_write_reply_t));
    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, &(rpc_reply->result), NULL))<0)
    {
        rpc_reply->link = strdup("");
        return true;
    }

    ACQUIRE_MUTEX(&dir->partition_mtx[index], "write(%s)", path);

    if (check_giga_addressing(dir, path, &(rpc_reply->result), NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "write(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: write(%s) for p(%d) changed.", path, index);
        goto start;
    }

    int state;
    char fpath[PATH_MAX]={0};
    char buf[FILE_THRESHOLD];
    int buf_len = 0;
    int fd;

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
#ifdef HDFS
          state = RPC_LEVELDB_FILE_IN_DB;
#else
            rpc_reply->result.errnum =
                  metadb_get_file(ldb_mds,
                                  dir_id, index, path,
                                  &state, buf, &buf_len);
            if (rpc_reply->result.errnum != 0) {
                rpc_reply->result.errnum = -rpc_reply->result.errnum;
                rpc_reply->link = strdup("");
                break;
            }
#endif
            if (state == RPC_LEVELDB_FILE_IN_DB) {
                if (size + offset <= FILE_THRESHOLD) {
                  rpc_reply->state = state;
                  rpc_reply->result.errnum =
                      metadb_write_file(ldb_mds,
                                        dir_id, index, path,
                                        data.giga_file_data_val,
                                        size, offset);
                  rpc_reply->link = strdup("");
                } else {
                    rpc_reply->state = RPC_LEVELDB_FILE_IN_FS;

                    sprintf(fpath, "%s/files/%d/%s",
                            get_storage_location(), dir_id, path);
                    rpc_reply->link = strdup(fpath);

                    fd = open(fpath, O_RDWR | O_CREAT, 0777);
                    if (fd > 0) {
                        if (pwrite(fd, buf, buf_len, 0) < 0) {
                            rpc_reply->result.errnum = errno;
                            LOG_ERR("Fail to write migrated file: %d, %s, %s, %d, %s",
                                dir_id, path, fpath, errno, strerror(errno));
                            close(fd);
                        } else {
                            close(fd);
                            metadb_write_link(ldb_mds, dir_id, index, path, fpath);
                            rpc_reply->result.errnum = 0;
                        }
                    } else {
                        LOG_ERR("Fail to create migrated file: %d, %s, %s, %d, %s",
                                dir_id, path, fpath, errno, strerror(errno));
                        rpc_reply->result.errnum = errno;
                    }
                }
            } else {
                rpc_reply->state = state;
                rpc_reply->link = strdup(buf);
            }
            break;
        default:
            break;
    }

    RELEASE_MUTEX(&dir->partition_mtx[index], "write(%s)", path);

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_write(d=%d,p=%s): link=[%s] state=[%d] status=[%d]",
            dir_id, path, rpc_reply->link,
            rpc_reply->state, rpc_reply->result.errnum);
    return true;
}

bool_t giga_rpc_fetch_1_svc(giga_dir_id dir_id, giga_pathname path,
                            giga_fetch_reply_t *rpc_reply,
                            struct svc_req *rqstp)
{

    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_fetch(d=%d,p=%s)", dir_id, path);

    bzero(rpc_reply, sizeof(giga_fetch_reply_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result), NULL);
    if (index < 0) {
        rpc_reply->result.errnum = 1;
        return true;
    }

    int state;
    char* buf = (char*) malloc(FILE_THRESHOLD * sizeof(char));
    int buf_len;

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
            rpc_reply->result.errnum =
                  metadb_get_file(ldb_mds,
                                  dir_id, index, path,
                                  &state, buf, &buf_len);
            if (rpc_reply->result.errnum == 0) {
                rpc_reply->data.state = state;
                switch (state) {
                  case RPC_LEVELDB_FILE_IN_DB:
                      if (buf_len > 0) {
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_val
                              = buf;
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_len
                              = buf_len;
                      } else {
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_val
                              = NULL;
                          rpc_reply->data.giga_read_t_u.buf.giga_file_data_len
                              = 0;
                      }
                      break;
                  case RPC_LEVELDB_FILE_IN_FS:
                      rpc_reply->data.giga_read_t_u.link = buf;
                      break;
                  default:
                    break;
                }

            }
            break;
        default:
            break;
    }

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_fetch(d=%d,p=%s): state=[%d], status=[%d]",
            dir_id, path, rpc_reply->data.state,
            rpc_reply->result.errnum);
    return true;
}

bool_t giga_rpc_updatelink_1_svc(giga_dir_id dir_id,
                                 giga_pathname path,
                                 giga_pathname link,
                                 giga_result_t *rpc_reply,
                                 struct svc_req *rqstp)
{
    (void)rqstp;

    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_updatelink(d=%d,p=%s,link=%s)",
            dir_id, path, link);

    bzero(rpc_reply, sizeof(giga_result_t));
    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, rpc_reply, NULL))<0)
    {
        return true;
    }

    ACQUIRE_MUTEX(&dir->partition_mtx[index], "updatelink(%s)", path);

    if (check_giga_addressing(dir, path, rpc_reply, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "updatelink(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: updatelink(%s) for p(%d) changed.", path, index);
        goto start;
    }

    rpc_reply->errnum = metadb_write_link(ldb_mds, dir_id, index, path, link);

    RELEASE_MUTEX(&dir->partition_mtx[index], "updatelink(%s)", path);

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_updatelink(d=%d,p=%s): status=[%d]",
            dir_id, path, rpc_reply->errnum);
    return true;
}

bool_t giga_rpc_open_1_svc(giga_dir_id dir_id, giga_pathname path, int mode,
                           giga_open_reply_t *rpc_reply,
                           struct svc_req *rqstp)
{
    (void)rqstp;
    (void)mode;
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_open(d=%d,p=%s)", dir_id, path);

    bzero(rpc_reply, sizeof(giga_open_reply_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result), NULL);
    if (index < 0) {
        rpc_reply->link = strdup("");
        return true;
    }

    int state;
    char buf[FILE_THRESHOLD];
    int buf_len;

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
            rpc_reply->result.errnum =
                  metadb_get_file(ldb_mds,
                                  dir_id, index, path,
                                  &state, buf, &buf_len);
            if (rpc_reply->result.errnum == 0) {
                rpc_reply->state = state;
                if (state == RPC_LEVELDB_FILE_IN_FS) {
                    rpc_reply->link = strdup(buf);
                } else {
                    rpc_reply->link = strdup("");
                }
            }
            break;
        default:
            break;
    }

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_open(d=%d,p=%s): state=[%d], link=[%s], status=[%d]",
            dir_id, path, rpc_reply->state, rpc_reply->link,
            rpc_reply->result.errnum);
    return true;
}

bool_t giga_rpc_close_1_svc(giga_dir_id dir_id, giga_pathname path,
                            giga_result_t *rpc_reply,
                            struct svc_req *rqstp)
{
    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_close(d=%d,p=%s)", dir_id, path);
    bzero(rpc_reply, sizeof(giga_result_t));

    /*
    rpc_reply->errnum = 0;

    return true;
    */
    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, rpc_reply, NULL)) < 0)
    {
        return true;
    }

    ACQUIRE_MUTEX(&dir->partition_mtx[index], "close(%s)", path);

    if(check_giga_addressing(dir, path, rpc_reply, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx[index], "close(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: write(%s) for p(%d) changed.", path, index);
        goto start;
    }

    int state;
    char link[PATH_MAX];
    int link_len;

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
            rpc_reply->errnum =
                  metadb_get_state(ldb_mds,
                                   dir_id, index, path,
                                   &state, link, &link_len);
            if (rpc_reply->errnum == 0) {
                if (state == RPC_LEVELDB_FILE_IN_FS) {
                    struct stat stbuf;
                    rpc_reply->errnum = stat(link, &stbuf);
                    if (rpc_reply->errnum == 0) {
                        metadb_setattr(ldb_mds,
                                       dir_id, index, path, &stbuf);
                    }
                }
            }
            break;
        default:
            break;
    }

    RELEASE_MUTEX(&dir->partition_mtx[index], "close(%s)", path);

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_close(d=%d,p=%s): status=[%d]",
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

