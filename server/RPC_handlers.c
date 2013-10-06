
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/rpc_giga.h"
#include "common/giga_index.h"
#include "common/hash.h"
#include "common/measure.h"
#include "backends/operations.h"

#include "server.h"
#include "split.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <arpa/inet.h>

#define _LEVEL_     LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(_LEVEL_, __func__, format, __VA_ARGS__);

#define DEBUG_MSG(format, ...) \
    logMessage(LOG_ERR, __func__, format, __VA_ARGS__);

static measurement_t measurement;
static measurement_t split_measurement;
static int zeroth_server_assigner;
static int stop_monitor_thread;

static
int check_split_eligibility(struct giga_directory *dir, int index)
{
    if ((dir->partition_size >= giga_options_t.split_threshold) &&
        (dir->split_flag == 0) &&
        (giga_is_splittable(&dir->mapping, index) == 1) &&
        (get_num_split_tasks_in_progress() == 0)) {
        return true;
    }

    return false;
}

// returns "index" on correct or "-1" on error
//
static
int check_giga_addressing(struct giga_directory *dir, giga_pathname path,
                          giga_result_t *rpc_reply,
                          giga_getattr_reply_t *stat_rpc_reply,
                          giga_lookup_t *lookup_reply)
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
        else if (lookup_reply != NULL) {
            assert (lookup_reply == NULL);
            memcpy(&(lookup_reply->giga_lookup_t_u.bitmap),
                   &dir->mapping, sizeof(dir->mapping));
            lookup_reply->errnum = -EAGAIN;
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

        int zeroth_srv = giga_options_t.serverID;
        struct giga_directory *new = new_cache_entry(&dir_id, zeroth_srv);

        if (metadb_read_bitmap(ldb_mds, dir_id, -1, NULL, &new->mapping) != 0) {
            LOG_ERR("ERR_fetching(d=%d): LDB_miss!", dir_id);
            return NULL;
        }

        cache_insert(&dir_id, new);

        //assert(new);
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

static
int get_server_for_new_inode(giga_dir_id dir_id, char* path) {
    /*
    zeroth_server_assigner++;
    if (zeroth_server_assigner >= giga_options_t.num_servers)
      zeroth_server_assigner = 0;
    return zeroth_server_assigner;
    */
    (void) dir_id;
    (void) path;
    return rand() % giga_options_t.num_servers;
    /*
    (void) dir_id;
    uint32_t hash = getStrHash(path, strlen(path), 0);
    return hash % giga_options_t.num_servers;
    */
}

void *monitor_thread(void *unused)
{
    (void)unused;

    struct sockaddr_in recv_addr;
    bzero(&recv_addr, sizeof(recv_addr));
    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(10600);
    if ((inet_aton("127.0.0.1", &recv_addr.sin_addr)) == 0) {
      printf("failed to setup monitor thread\n");
      return NULL;
    }
    int fd;
    if ((fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
      printf("failed to setup monitor socket\n");
      return NULL;
    }

    struct timespec ts;
    ts.tv_sec = 5;
    ts.tv_nsec = 0;

    struct timespec rem;
    measurement_t last_measure;
    measurement_clear(&last_measure);

    char message[256];

    while (stop_monitor_thread == 0) {
        int ret = nanosleep(&ts, &rem);
        if (ret == -1){
            if (errno == EINTR)
                nanosleep(&rem, NULL);
        }

        double delta_num = measurement.num_ - last_measure.num_;
        double delta_sum = measurement.sum_ - last_measure.sum_;
        last_measure = measurement;
        double avg_latency = (delta_num > 0) ? delta_sum / delta_num : 0.0;
        time_t now_time = time(NULL);
        snprintf(message, 256,
                "giga_server_ops %ld %.0f\n"
                "giga_server_avg_latency %ld %.3f\n"
                "giga_server_split_ops %ld %.0f\n"
                "giga_server_split_time %ld %.0f\n",
                now_time, last_measure.num_,
                now_time, avg_latency,
                now_time, split_measurement.num_,
                now_time, split_measurement.sum_);
        sendto(fd, message, strlen(message), 0,
               (struct sockaddr *) &recv_addr, sizeof(recv_addr));
    }
    return NULL;
}

void init_rpc_handlers() {
    zeroth_server_assigner = giga_options_t.serverID;
    srand(zeroth_server_assigner);
    measurement_clear(&measurement);
    measurement_clear(&split_measurement);

    pthread_t tid;
    int ret = pthread_create(&tid, NULL, monitor_thread, NULL);
    (void) ret;
}

void destroy_rpc_handlers() {
    stop_monitor_thread = 1;
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
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

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

    uint64_t start_time = now_micros();

    bzero(rpc_reply, sizeof(giga_getattr_reply_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->result.errnum = -ENOENT;
      return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, NULL, rpc_reply, NULL);
    if (index < 0)
        goto exit_func;

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

                if (metadb_read_bitmap(ldb_mds, dir_id, index, path,
                                &rpc_reply->result.giga_result_t_u.bitmap) != 0) {
                    LOG_ERR("mdb_read(%s): error reading dir=%d bitmap.",
                            path, dir_id);
                    rpc_reply->result.errnum = -ENOENT;
                    goto exit_func;
                } else {
                  rpc_reply->zeroth_server =
                    rpc_reply->result.giga_result_t_u.bitmap.zeroth_server;
                  if (rpc_reply->zeroth_server == giga_options_t.serverID) {
                      rpc_reply->result.errnum = -EAGAIN;
                  }
                  fuse_cache_insert(dir_id, path,
                                    rpc_reply->statbuf.st_ino,
                                    rpc_reply->zeroth_server);
                }
            }
            rpc_reply->file_size = rpc_reply->statbuf.st_size;

            break;
        default:
            break;
    }

exit_func:

    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

    LOG_MSG("<<< RPC_getattr(d=%d,p=%s): status=[%d]",
            dir_id, path, rpc_reply->result.errnum);
    return true;
}

bool_t giga_rpc_lookup_1_svc(giga_dir_id dir_id, giga_pathname path,
                             giga_lookup_t *rpc_reply,
                             struct svc_req *rqstp)
{
    (void)rqstp;

    assert(rpc_reply);
    assert(path);

    LOG_MSG(">>> RPC_lookup(d=%d,p=%s)", dir_id, path);

    uint64_t start_time = now_micros();

    bzero(rpc_reply, sizeof(giga_lookup_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, NULL, NULL, rpc_reply);
    if (index < 0)
        goto exit_func;

    time_t tmp_time;
    giga_dir_id ret_dir_id;
    int ret_zeroth_server;
    if ((ret_dir_id = fuse_cache_lookup(dir_id, path, &tmp_time,
                                        &ret_zeroth_server)) != -1) {
        rpc_reply->giga_lookup_t_u.result.dir_id = ret_dir_id;
        rpc_reply->giga_lookup_t_u.result.zeroth_server = ret_zeroth_server;
    } else {
        struct stat statbuf;
        rpc_reply->errnum = metadb_lookup(ldb_mds,
                                          dir_id, index, path,
                                          &statbuf);
        if (S_ISDIR(statbuf.st_mode)) {
            struct giga_mapping_t mapping;
            if (metadb_read_bitmap(ldb_mds, dir_id, index, path,
                                   &mapping) != 0) {
                rpc_reply->errnum = -ENOENT;
                goto exit_func;
            } else {
              rpc_reply->giga_lookup_t_u.result.dir_id = statbuf.st_ino;
              rpc_reply->giga_lookup_t_u.result.zeroth_server =
                                                  mapping.zeroth_server;

              fuse_cache_insert(dir_id, path,
                                statbuf.st_ino,
                                mapping.zeroth_server);
            }
        }
    }

exit_func:

    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

    LOG_MSG("<<< RPC_lookup(d=%d,p=%s): status=[%d]",
            dir_id, path, rpc_reply->errnum);
    return true;
}

bool_t giga_rpc_getmapping_1_svc(giga_dir_id dir_id,
                                 giga_result_t *rpc_reply,
                                 struct svc_req *rqstp) {
    (void) rqstp;
    bzero(rpc_reply, sizeof(giga_result_t));
    LOG_MSG(">>> RPC_getmapping(d=%d)", dir_id);

    uint64_t start_time = now_micros();

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    rpc_reply->errnum = -EAGAIN;
    memcpy(&(rpc_reply->giga_result_t_u.bitmap),
           &(dir->mapping), sizeof(struct giga_mapping_t));

    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

    LOG_MSG("<<< RPC_getmapping(d=%d): status=[%d]",
            dir_id, rpc_reply->errnum);
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

    uint64_t start_time = now_micros();

    LOG_MSG(">>> RPC_mknod(d=%d,p=%s): m=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    int index = 0;

start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, rpc_reply, NULL, NULL)) < 0)
        goto exit_func_release;

    ACQUIRE_MUTEX(&dir->partition_mtx, "mknod(%s)", path);

    if(check_giga_addressing(dir, path, rpc_reply, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "mknod(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: mknod(%s) for p(%d) changed.", path, index);
        goto start;
    }

    // check for splits.
    if ((check_split_eligibility(dir, index) == true)) {
        ACQUIRE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        dir->split_flag = 1;
        RELEASE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);

        LOG_MSG("SPLIT_p%d[%d entries] caused_by=[%s]",
                index, dir->partition_size, path);

        uint64_t start_split_time = now_micros();
        measurement_add(&split_measurement, 0);
        rpc_reply->errnum = split_bucket(dir, index);
        uint64_t split_latency = now_micros() - start_split_time;
        measurement_add(&split_measurement, split_latency);

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
                dir->partition_size += 1;
            break;
        case BACKEND_RPC_LEVELDB:

            // create object entry (metadata) in levelDB

            LOG_MSG("d=%d,p=%d,p=%s", dir_id, index, path);
            rpc_reply->errnum = metadb_create(ldb_mds, dir_id, index, path, "");
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            else
                dir->partition_size += 1;
            break;
        default:
            break;
    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx, "mknod(%s)", path);

exit_func_release:
    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

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
    uint64_t start_time = now_micros();

    LOG_MSG(">>> RPC_mkdir(d=%d,p=%s): mode=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));
    struct giga_directory *dir = fetch_dir_mapping(dir_id);

    int index = 0;

start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, rpc_reply, NULL, NULL)) < 0)
        goto exit_func_release;

    ACQUIRE_MUTEX(&dir->partition_mtx, "mkdir(%s)", path);

    if(check_giga_addressing(dir, path, rpc_reply, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "mkdir(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: mkdir(%s) for p(%d) changed.", path, index);
        goto start;
    }

    // check for splits.
    if ((check_split_eligibility(dir, index) == true)) {
        ACQUIRE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);
        dir->split_flag = 1;
        RELEASE_MUTEX(&dir->split_mtx, "set_split(p%d)", index);

        LOG_MSG("SPLIT_p%d[%d entries] caused_by=[%s]",
                index, dir->partition_size, path);

        uint64_t start_split_time = now_micros();
        measurement_add(&split_measurement, 0);
        rpc_reply->errnum = split_bucket(dir, index);
        uint64_t split_latency = now_micros() - start_split_time;
        measurement_add(&split_measurement, split_latency);

        if (rpc_reply->errnum == -EAGAIN) {
            LOG_MSG("ERR_retry: split_p%d", index);
        } else if (rpc_reply->errnum < 0) {
            LOG_ERR("**FATAL_ERROR** during split of p%d", index);
            exit(1);    //TODO: do something smarter???
        } else {
            memcpy(&(rpc_reply->giga_result_t_u.bitmap),
                   &dir->mapping, sizeof(dir->mapping));
            if (metadb_write_bitmap(ldb_mds, dir_id, -1, NULL, &dir->mapping) != 0) {
                LOG_ERR("mdb_write_bitmap(d%d): error write bitmap.", dir_id);
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
            // create object entry (metadata) in levelDB
            // and create partition entry for this object

            int object_id = metadb_get_next_inode_count(ldb_mds);
            int zeroth_server = get_server_for_new_inode(dir_id, path);
            struct giga_directory *new_dir =
              new_cache_entry(&object_id, zeroth_server);

            LOG_MSG("d=%d,p=%d,o=%d,path=%s,zeroth_srv=%d",
                    dir_id, index, object_id, path, zeroth_server);


            //TODO: DEBUG ONLY
            assert(new_dir->mapping.id == object_id);

            //TODO: no need to copy mapping?
            rpc_reply->errnum = metadb_create_dir(ldb_mds, dir_id, index,
                                                  path, &new_dir->mapping);
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_create(%s): p%d of d%d", path, index, dir_id);
            else {

                dir->partition_size += 1;

                if (zeroth_server != giga_options_t.serverID) {
                    giga_result_t rpc_mkzeroth_reply;
                    CLIENT *rpc_clnt = getConnection(zeroth_server);

                    LOG_MSG(">>> RPC_mkzeroth_send: [d%d, s%d-->s%d)]",
                            object_id, giga_options_t.serverID, zeroth_server);

                    if (giga_rpc_mkzeroth_1(object_id, &rpc_mkzeroth_reply,
                                            rpc_clnt) != RPC_SUCCESS) {
                        LOG_ERR("ERR_rpc_mkdir(%d)", object_id);
                        exit(1);//TODO: retry again?
                    }
                    rpc_reply->errnum = rpc_mkzeroth_reply.errnum;
                } else {
                    rpc_reply->errnum = metadb_create_dir(ldb_mds, object_id,
                                                  -1, NULL, &new_dir->mapping);
                    /*
                    if (rpc_reply->errnum == 0)
                        create_dir_in_storage(dir_id);
                    */
                }
            }
            free(new_dir);
    }

exit_func:

    RELEASE_MUTEX(&dir->partition_mtx, "mkdir(%s)", path);

exit_func_release:

    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

    LOG_MSG("<<< RPC_mkdir(d=%d,p=%s): status=[%d]",
            dir_id, path, rpc_reply->errnum);

    return true;
}

bool_t giga_rpc_mkzeroth_1_svc(giga_dir_id dir_id,
                               giga_result_t *rpc_reply,
                               struct svc_req *rqstp) {
    //TODO: missing locks?
    (void) rqstp;
    LOG_MSG(">>> RPC_mkzeroth(d=%d)", dir_id);

    int zeroth_server = giga_options_t.serverID;

    struct giga_directory *new_dir = new_cache_entry(&dir_id, zeroth_server);

    //TODO: DEBUG ONLY
    assert(dir_id == new_dir->mapping.id);

    LOG_MSG("mkzeroth(d=%d)", dir_id);
    rpc_reply->errnum = metadb_create_dir(ldb_mds, dir_id, -1, NULL,
                                          &new_dir->mapping);
    if (rpc_reply->errnum < 0)
        LOG_ERR("ERR_mdb_mkzeroth(%d)", dir_id);

    cache_insert(&dir_id, new_dir);
    cache_release(new_dir);

    /*
    create_dir_in_storage(dir_id);
    */
    LOG_MSG("<<< RPC_mkzeroth(d=%d): status=[%d]",
            dir_id, rpc_reply->errnum);

    return true;
}

bool_t giga_rpc_chmod_1_svc(giga_dir_id dir_id,
                            giga_pathname path, mode_t mode,
                            giga_result_t *rpc_reply,
                            struct svc_req *rqstp) {

    (void)rqstp;
    assert(rpc_reply);
    assert(path);

    uint64_t start_time = now_micros();

    LOG_MSG(">>> RPC_chmod(d=%d,p=%s): m=[0%3o]", dir_id, path, mode);

    bzero(rpc_reply, sizeof(giga_result_t));

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    int index = 0;
start:
    // check for giga specific addressing checks.
    //
    if ((index = check_giga_addressing(dir, path, rpc_reply, NULL, NULL)) < 0)
        goto exit_func_release;

    ACQUIRE_MUTEX(&dir->partition_mtx, "mkdir(%s)", path);

    if(check_giga_addressing(dir, path, rpc_reply, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "mkdir(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: mkdir(%s) for p(%d) changed.", path, index);
        goto start;
    }

    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LEVELDB:
            rpc_reply->errnum = metadb_chmod(ldb_mds, dir_id, index, path,
                                             mode);
            if (rpc_reply->errnum < 0)
                LOG_ERR("ERR_mdb_chmod(%s): p%d of d%d", path, index, dir_id);
            break;
        default:
            break;
    }

    RELEASE_MUTEX(&dir->partition_mtx, "mknod(%s)", path);

exit_func_release:
    release_dir_mapping(dir);

    uint64_t latency = now_micros() - start_time;
    measurement_add(&measurement, latency);

    LOG_MSG("<<< RPC_chmod(d=%d,p=%s): status=[%d]", dir_id, path,
            rpc_reply->errnum);
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
    if (dir == NULL) {
      rpc_reply->result.errnum = -ENOENT;
      return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result), NULL, NULL);
    if (index < 0) {
        rpc_reply->result.errnum = 1;
        goto exit_func;
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

exit_func:

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
    if (dir == NULL) {
      rpc_reply->result.errnum = -ENOENT;
      return true;
    }

    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, &(rpc_reply->result),
                                     NULL, NULL))<0)
    {
        rpc_reply->link = strdup("");
        return true;
    }

    ACQUIRE_MUTEX(&dir->partition_mtx, "write(%s)", path);

    if (check_giga_addressing(dir, path, &(rpc_reply->result),
                              NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "write(%s)", path);
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

    RELEASE_MUTEX(&dir->partition_mtx, "write(%s)", path);

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
    if (dir == NULL) {
      rpc_reply->result.errnum = -ENOENT;
      return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result),
                                      NULL, NULL);
    if (index < 0) {
        rpc_reply->result.errnum = 1;
        goto exit_func;
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

exit_func:
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
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, rpc_reply, NULL, NULL))<0)
    {
        return true;
    }

    ACQUIRE_MUTEX(&dir->partition_mtx, "updatelink(%s)", path);

    if (check_giga_addressing(dir, path, rpc_reply, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "updatelink(%s)", path);
        LOG_MSG("RECOMPUTE_INDEX: updatelink(%s) for p(%d) changed.",
                path, index);
        goto start;
    }

    rpc_reply->errnum = metadb_write_link(ldb_mds, dir_id, index, path, link);

    RELEASE_MUTEX(&dir->partition_mtx, "updatelink(%s)", path);

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
    if (dir == NULL) {
      rpc_reply->result.errnum = -ENOENT;
      return true;
    }

    // check for giga specific addressing checks.
    //
    int index = check_giga_addressing(dir, path, &(rpc_reply->result),
                                      NULL, NULL);
    if (index < 0) {
        rpc_reply->link = strdup("");
        goto exit_func;
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

exit_func:
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

    struct giga_directory *dir = fetch_dir_mapping(dir_id);
    if (dir == NULL) {
      rpc_reply->errnum = -ENOENT;
      return true;
    }

    int index;

start:
    // check for giga specific addressing checks.
    //
    if ((index=check_giga_addressing(dir, path, rpc_reply, NULL, NULL)) < 0)
        goto exit_func;

    ACQUIRE_MUTEX(&dir->partition_mtx, "close(%s)", path);

    if(check_giga_addressing(dir, path, rpc_reply, NULL, NULL) != index) {
        RELEASE_MUTEX(&dir->partition_mtx, "close(%s)", path);
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

exit_func:
    RELEASE_MUTEX(&dir->partition_mtx, "close(%s)", path);

    release_dir_mapping(dir);

    LOG_MSG("<<< RPC_close(d=%d,p=%s): status=[%d]",
            dir_id, path, rpc_reply->errnum);
    return true;
}
