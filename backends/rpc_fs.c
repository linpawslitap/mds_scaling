
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/giga_index.h"
#include "common/rpc_giga.h"

#include <errno.h>
#include <fcntl.h>
#include <rpc/rpc.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>

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

static
void init_cache()
{
    int dir_id = ROOT_DIR_ID; //FIXME: dir_id for "root"
    int srv_id = 0;

    cache_init();
    struct giga_directory *dir = new_cache_entry(&dir_id, srv_id);
    cache_insert(&dir_id, dir);

    giga_print_mapping(&dir->mapping);

    return;
}

int rpc_init()
{
    int ret = 0;

    int dir_id = ROOT_DIR_ID; // update root server's bitmap
    int server_id = 0;

    init_cache();

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
        struct giga_directory *dir = cache_lookup(&dir_id);
        if (dir == NULL) {
            LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
            ret = -EIO;
        }
        else {
            update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap); 
            ret = 0;
        }
        giga_print_mapping(&dir->mapping);
        cache_release(dir);
    } else if (errnum < 0) {
        ret = errnum;
    }


    LOG_MSG("<<< RPC_init: s[%d]", server_id);

    return ret;
}

void rpc_disconnect()
{
    rpcDisconnect();
}

struct giga_directory* rpc_getpartition(int object_id, int zeroth_server) {
    struct giga_directory* dir  = cache_lookup(&object_id);
    if (dir != NULL)
      return dir;

    CLIENT *rpc_clnt = getConnection(zeroth_server);
    giga_result_t rpc_mapping_reply;
    if (giga_rpc_getmapping_1(object_id, &rpc_mapping_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_getmapping(%d)", object_id);
        exit(1);//TODO: retry again?
    }
    if (rpc_mapping_reply.errnum == -EAGAIN) {
        rpc_mapping_reply.giga_result_t_u.bitmap.id = object_id;
        dir = new_cache_entry_with_mapping(&object_id,
                             &rpc_mapping_reply.giga_result_t_u.bitmap);
        if (dir->mapping.zeroth_server != (unsigned int) zeroth_server) {
            LOG_ERR("ERR_rpc_getpartition(%d %d)",
                    object_id, zeroth_server);
            dir->mapping.zeroth_server = zeroth_server;
        }
        cache_insert(&object_id, dir);
    }
    //assert(dir != NULL);
    return dir;
}

int rpc_getattr(int dir_id, int zeroth_srv, const char *path,
                struct stat *stbuf, int *ret_zeroth_server)
{
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    *ret_zeroth_server = 0;
    giga_getattr_reply_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_getattr(%d, %s): to s[%d]", dir_id, path, server_id);

    if (giga_rpc_getattr_1(dir_id, (char*)path, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_getattr(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    //TODO: do we need to update dircache when object_id already exists in dircache?
    // check return condition
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        if (S_ISDIR(rpc_reply.statbuf.st_mode)) {
            *stbuf = rpc_reply.statbuf;
            stbuf->st_size = rpc_reply.file_size;
            stbuf->st_blocks = stbuf->st_size / 4096;
            int object_id = stbuf->st_ino;
            struct giga_directory *next_dir = cache_lookup(&object_id);
            if (next_dir == NULL) {
                LOG_MSG("GETATTR(%s) returns a directory for d%d",
                        path, stbuf->st_ino);
                giga_print_mapping(&rpc_reply.result.giga_result_t_u.bitmap);
                struct giga_directory *new = new_cache_entry_with_mapping(
                            &object_id, &rpc_reply.result.giga_result_t_u.bitmap);
                new->mapping.zeroth_server = rpc_reply.zeroth_server;
                *ret_zeroth_server = new->mapping.zeroth_server;
                cache_insert(&object_id, new);
                cache_release(new);
            } else {
                *ret_zeroth_server = next_dir->mapping.zeroth_server;
                cache_release(next_dir);
            }
            ret = 0;
        }
        else {
            update_client_mapping(dir,
                  &rpc_reply.result.giga_result_t_u.bitmap);
            LOG_MSG("bitmap update from s%d -- RETRY ...", server_id);
            goto retry;
        }
    } else if (ret >= 0) {
        *stbuf = rpc_reply.statbuf;
        stbuf->st_size = rpc_reply.file_size;
        stbuf->st_blocks = stbuf->st_size / 4096;
        if (stbuf == NULL)
            LOG_MSG("ERR_getattr(%s): statbuf is NULL!", path);

        if (S_ISDIR(stbuf->st_mode)) {
            int object_id = stbuf->st_ino;
            int zeroth_server = rpc_reply.zeroth_server;
            LOG_MSG("GETATTR(%s) returns a directory for d%d at %d",
                    path, object_id, zeroth_server);

            *ret_zeroth_server = zeroth_server;
            struct giga_directory *next_dir =
                rpc_getpartition(object_id, zeroth_server);
            if (next_dir == NULL)
              return -1;
            cache_release(next_dir);
        }
    }

    cache_release(dir);

    LOG_MSG("<<< RPC_getattr(%d, %s): ret_zeroth_server=[%d] status=[%d]%s",
             dir_id, path, *ret_zeroth_server, ret, strerror(ret));

    return ret;
}

int rpc_chmod(int dir_id, int zeroth_srv, const char *path, mode_t new_mode)
{
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_chmod(%s): to s[%d]", path, server_id);

    if (giga_rpc_chmod_1(dir_id, (char*)path, new_mode, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_chmod(%s)", clnt_spcreateerror(path));
        exit(1);
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

    cache_release(dir);
    LOG_MSG("<<< RPC_chmod(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

int rpc_remove(int dir_id, int zeroth_srv, const char *path) {
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_remove(%s): to s[%d]", path, server_id);

    if (giga_rpc_remove_1(dir_id, (char*)path, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_remove(%s)", clnt_spcreateerror(path));
        exit(1);
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

    cache_release(dir);
    LOG_MSG("<<< RPC_remove(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

int rpc_getval(int dir_id, int zeroth_srv, const char *path,
               char* *buf, int* buf_len) {
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_getval_reply_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_getval(%s): to s[%d]", path, server_id);

    if (giga_rpc_getval_1(dir_id, (char*)path, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_getval(%s)", clnt_spcreateerror(path));
        exit(1);
    }

    // check return condition
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret < 0) {
        ;
    } else {
        *buf_len = rpc_reply.data.giga_file_data_len;
        *buf = rpc_reply.data.giga_file_data_val;
        ret = 0;
    }

    cache_release(dir);
    LOG_MSG("<<< RPC_getval(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

int rpc_putval(int dir_id, int zeroth_srv, const char *path,
               char* buf, int buf_len) {
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_result_t rpc_reply;
    giga_file_data data;
    data.giga_file_data_val = buf;
    data.giga_file_data_len = buf_len;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_putval(%s): to s[%d]", path, server_id);


    if (giga_rpc_putval_1(dir_id, (char*)path, data, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_putval(%s)", clnt_spcreateerror(path));
        exit(1);
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

    cache_release(dir);
    LOG_MSG("<<< RPC_remove(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}


int rpc_mkdir(int dir_id, int zeroth_srv, const char *path, mode_t mode)
{
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;      //TODO: randomize zeroth server
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
    }

    cache_release(dir);

    LOG_MSG("<<< RPC_mkdir(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

int rpc_mknod(int dir_id, int zeroth_srv, const char *path,
              mode_t mode, dev_t dev)
{
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_result_t rpc_reply;

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_mknod(%s): to s[%d]", path, server_id);

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

    cache_release(dir);

    LOG_MSG("<<< RPC_mknod(%s): status=[%d]%s", path, ret, strerror(ret));

    return ret;
}

struct readdir_args {
    struct giga_directory dir_t;;
    int dir_id;
    int server_id;
    int entries;
};

pthread_mutex_t final_ls_mtx;
static scan_list_t final_ls_start;
static scan_list_t final_ls_end;

void *readdir_thread(void *params)
{
    int ret;
    struct readdir_args *a = (struct readdir_args*) params;

    struct giga_directory dir = (struct giga_directory)a->dir_t;
    int dir_id = (int)a->dir_id;
    int server_id = (int)a->server_id;

    /*
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        //ret = -EIO;
        pthread_exit(NULL);
    }
    */

    readdir_return_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));
    scan_list_t ls;
    int more_ents_flag = 0;

    CLIENT *rpc_clnt;
retry:
    rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_readdir(%d): to s[%d]", dir_id, server_id);

    scan_args_t args;
    args.dir_id = dir_id;
    args.partition_id = -1;
    args.start_key.scan_key_val = NULL;
    args.start_key.scan_key_len = 0;

    int ents=0;
    do {
        memset(&rpc_reply, 0, sizeof(rpc_reply));

        LOG_MSG("readdir_again with (d%d,p%d,key=[%s],len=%d)", 
                args.dir_id, args.partition_id, 
                args.start_key.scan_key_val, args.start_key.scan_key_len);

        if (giga_rpc_readdir_serial_1(args, &rpc_reply, rpc_clnt) != RPC_SUCCESS) {
            LOG_ERR("ERR_rpc_readdir(dirid=%d,s=%d)", dir_id, server_id);
            exit(1);//TODO: retry again?
        }

        // check return condition 
        //
        ret = rpc_reply.errnum;
        if (ret == -EAGAIN) {
            //update_client_mapping(dir, &rpc_reply.readdir_return_t_u.bitmap);
            update_client_mapping(&dir, &rpc_reply.readdir_return_t_u.bitmap);
            LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
            goto retry;
        } else if (ret == ENOENT) {
            LOG_MSG("ERR_rpc_readdir(dirid=%d,s=%d) is absent", dir_id, server_id);
            break;
        } else if (ret < 0) {
            ;
        } else {

            //if (rpc_reply.readdir_return_t_u.result.num_entries == 0) {
            //    LOG_MSG("ERR_rpc_readdir(dirid=%d,s=%d) is absent", dir_id, server_id);
            //    break;
            //}

            ACQUIRE_MUTEX(&final_ls_mtx, "readdir_result_mtx(%d)", server_id);
            if ((args.start_key.scan_key_val == NULL) && (final_ls_start == NULL))
                final_ls_start = rpc_reply.readdir_return_t_u.result.list;
            else 
                final_ls_end->next =rpc_reply.readdir_return_t_u.result.list;

            LOG_MSG("num_ents[s%d] = %d", server_id, rpc_reply.readdir_return_t_u.result.num_entries);
            for (ls=rpc_reply.readdir_return_t_u.result.list; ls!=NULL; ls=ls->next) {
                //LOG_MSG("dentry=[%s]", ls->entry_name);
                if (ls->next == NULL) {
                        final_ls_end = ls;
                }
            }
            RELEASE_MUTEX(&final_ls_mtx, "readdir_result_mtx(%d)", server_id);

            more_ents_flag = rpc_reply.readdir_return_t_u.result.more_entries_flag;

            if (more_ents_flag) {
                LOG_MSG("p=%d,end_key=[%s],len=%d", 
                        rpc_reply.readdir_return_t_u.result.end_partition,
                        rpc_reply.readdir_return_t_u.result.end_key.scan_key_val,
                        rpc_reply.readdir_return_t_u.result.end_key.scan_key_len);

                args.partition_id = rpc_reply.readdir_return_t_u.result.end_partition;
                args.start_key.scan_key_len = rpc_reply.readdir_return_t_u.result.end_key.scan_key_len;
                args.start_key.scan_key_val = strdup(rpc_reply.readdir_return_t_u.result.end_key.scan_key_val);
                args.start_key.scan_key_val[args.start_key.scan_key_len] = '\0';

            } else {
                update_client_mapping(&dir, &rpc_reply.readdir_return_t_u.result.bitmap);
            }

        }

        ents += rpc_reply.readdir_return_t_u.result.num_entries;
    } while(more_ents_flag != 0);

    a->entries = ents;
    LOG_MSG("readdir[s%d]=%d", server_id, ents);

    //pthread_exit((void*)ret);
    pthread_exit(NULL);
}

scan_list_t rpc_readdir(int dir_id, int zeroth_srv, const char *path, int *num_entries)
{
    //scan_list_t final_ls_start = NULL;
    //scan_list_t final_ls_end = NULL;

    pthread_mutex_init(&final_ls_mtx, NULL);
    ACQUIRE_MUTEX(&final_ls_mtx, "readdir_result_mtx([%d][%s])", dir_id, path);
    final_ls_start = NULL;
    final_ls_end = NULL;
    RELEASE_MUTEX(&final_ls_mtx, "readdir_result_mtx([%d][%s])", dir_id, path);

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return NULL;
    }

    //FIXME: only for the servers that hold the partitions.
    int max_servers = giga_options_t.num_servers;
    //int max_servers = 2;
    pthread_t tid[max_servers];

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    struct readdir_args* args = (struct readdir_args*)
        malloc(max_servers * sizeof(struct readdir_args));

    void *status;
    int i=0;
    for (i=0; i<max_servers; i++) { //#### start_for_loop for all partitions
        args[i].dir_t = *dir;
        args[i].dir_id = dir_id;
        args[i].server_id = i;
        args[i].entries = 0;

        if (pthread_create(&tid[i], &attr, readdir_thread, (void *)&args[i])) {
            fprintf(stderr, "pthread_create() error: scan on s[%d]\n", i);
            exit(1);
        }
    }

    *num_entries = 0;
    pthread_attr_destroy(&attr);
    for (i=0; i<max_servers; i++) { //#### start_for_loop for all partitions

        int ret = pthread_join(tid[i], &status);
        if (ret) {
            fprintf(stderr, "pthread_join(s[%d]) error: %d\n", i, ret);
            exit(1);
        }
        *num_entries += args[i].entries;
        LOG_MSG("<<< readdir[%d]: status=[%ld]", i, (long)status);
    }

    free(args);
    cache_release(dir);

    LOG_MSG("<<< RPC_readdir(%s): return", path);

    return final_ls_start;
}

#if 0
scan_list_t rpc_readdir(int dir_id, const char *path)
{
    int ret = 0;

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        ret = -EIO;
    }

    scan_list_t final_ls_start = NULL;
    scan_list_t final_ls_end = NULL;

    int server_id = 0;

    giga_print_mapping(&dir->mapping);

    int partitions[MAX_GIGA_PARTITIONS] = {0};
    giga_get_all_partitions(&dir->mapping, partitions);

    int i=0;
    for (i=0; i<MAX_GIGA_PARTITIONS; i++) 
    {   //#### start_for_loop for all partitions

    if (partitions[i] == 0)
        continue;
    int p_id = i;

    readdir_return_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));
    scan_list_t ls;
    int more_ents_flag = 0;

retry:
    server_id = giga_get_server_for_index(&dir->mapping, p_id);
    //server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_readdir(%s): to s[%d]", path, server_id);

    scan_args_t args;
    args.dir_id = dir_id;
    args.partition_id = p_id;
    args.start_key.scan_key_val = NULL;
    args.start_key.scan_key_len = 0;

    do {
        memset(&rpc_reply, 0, sizeof(rpc_reply));

        LOG_MSG("readdir_again with (%d,%d,key=[%s],len=%d)", 
                args.dir_id, args.partition_id, 
                args.start_key.scan_key_val, args.start_key.scan_key_len);

        if (giga_rpc_readdir_serial_1(args, &rpc_reply, rpc_clnt) != RPC_SUCCESS) {
            LOG_ERR("ERR_rpc_readdir(%s)", clnt_spcreateerror(path));
            exit(1);//TODO: retry again?
        }

        // check return condition 
        //
        ret = rpc_reply.errnum;
        if (ret == -EAGAIN) {
            update_client_mapping(dir, &rpc_reply.readdir_return_t_u.bitmap);
            LOG_MSG("bitmap update from s%d -- RETRY ...", server_id); 
            goto retry;
        } else if (ret < 0) {
            ;
        } else {
            if ((args.start_key.scan_key_val == NULL) && (final_ls_start == NULL))
                final_ls_start = rpc_reply.readdir_return_t_u.result.list;
            else 
                final_ls_end->next =rpc_reply.readdir_return_t_u.result.list;

            LOG_MSG("num_ents = %d", rpc_reply.readdir_return_t_u.result.num_entries);
            for (ls=rpc_reply.readdir_return_t_u.result.list; ls!=NULL; ls=ls->next) {
                //LOG_MSG("dentry=[%s]", ls->entry_name);
                if (ls->next == NULL) 
                        final_ls_end = ls;
            }

            more_ents_flag = rpc_reply.readdir_return_t_u.result.more_entries_flag;

            if (more_ents_flag) {
                args.start_key.scan_key_len = rpc_reply.readdir_return_t_u.result.end_key.scan_key_len;
                args.start_key.scan_key_val = strdup(rpc_reply.readdir_return_t_u.result.end_key.scan_key_val);
                args.start_key.scan_key_val[args.start_key.scan_key_len] = '\0';
            } else {
                update_client_mapping(dir, &rpc_reply.readdir_return_t_u.result.bitmap);
                giga_get_all_partitions(&dir->mapping, partitions);
            }

        }
    } while(more_ents_flag != 0);

    } //### end_for_loop for all partitions

    LOG_MSG("<<< RPC_readdir(%s): status=[%d]%s", path, ret, strerror(ret));

    return final_ls_start;
}
#endif

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

int rpc_write(int dir_id, int zeroth_srv, const char* path,
              const char* buf, size_t size,
              off_t offset, int* state, char* symlink)
{
    int ret = 0;
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;

    giga_write_reply_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

    //Find the right server
retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_write(%s): to s[%d]", path, server_id);

    giga_file_data write_data;
    write_data.giga_file_data_val = (char *) buf;
    write_data.giga_file_data_len = size;
    if (giga_rpc_write_1(dir_id, (char*) path, write_data, offset,
                         &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_write(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    // check return condition
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret >= 0) {
        *state = rpc_reply.state;
        if (rpc_reply.state == RPC_LEVELDB_FILE_IN_FS) {
          strncpy(symlink, rpc_reply.link, PATH_MAX);
          symlink[PATH_MAX -1] = '\0';
        }
    }
    cache_release(dir);

    LOG_MSG("<<< RPC_write(%s): status=[%d]", path, ret);
    return ret;
}

int rpc_read(int dir_id, int zeroth_srv, const char* path,
             char* buf, size_t size,
             off_t offset, int* state, char* symlink)
{
    int ret = 0;
    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;

    giga_read_reply_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

    //Find the right server
retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_read(%s): to s[%d]", path, server_id);

    if (giga_rpc_read_1(dir_id, (char*)path, size, offset,
                         &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_read(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    // check return condition
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap);
        LOG_MSG("bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret >= 0) {
        *state = rpc_reply.data.state;
        if (rpc_reply.data.state == RPC_LEVELDB_FILE_IN_FS) {
          strncpy(symlink, rpc_reply.data.giga_read_t_u.link, PATH_MAX);
          symlink[PATH_MAX -1] = '\0';
        } else {
          memcpy(buf, rpc_reply.data.giga_read_t_u.buf.giga_file_data_val,
                      rpc_reply.data.giga_read_t_u.buf.giga_file_data_len);
        }
    }
    cache_release(dir);

    LOG_MSG("<<< RPC_read(%s): status=[%d]", path, ret);
    return ret;
}


int rpc_fetch(int dir_id, int zeroth_srv, const char *path,
             int* state, char *buf, int* buf_len)
{
    int ret = 0;

    LOG_MSG(">>> RPC_fetch(%s, d%d)", path, dir_id);

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    static giga_fetch_reply_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_fetch(%s): to s%d]", path, server_id);

    if (giga_rpc_fetch_1(dir_id, (char*)path, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_fetch(%s)", clnt_spcreateerror(path));
        exit(1);
    }

    // check return condition
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap);
        LOG_MSG("rpc_fetch: bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret == 0){
        *state = rpc_reply.data.state;
        if (rpc_reply.data.state == RPC_LEVELDB_FILE_IN_FS) {
          strncpy(buf, rpc_reply.data.giga_read_t_u.link, PATH_MAX);
          *buf_len = strlen(buf);
        } else {
          *buf_len = rpc_reply.data.giga_read_t_u.buf.giga_file_data_len;
          memcpy(buf, rpc_reply.data.giga_read_t_u.buf.giga_file_data_val,
                *buf_len);
        }
    }
    cache_release(dir);

    LOG_MSG("RPC_fetch(%s): status=[%d]%s", path, ret, strerror(ret));
    return ret;
}

int rpc_updatelink(int dir_id, int zeroth_srv, const char *path,
                   const char* link)
{
    int ret = 0;

    LOG_MSG(">>> RPC_updatelink(%s, d%d)", path, dir_id);

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    static giga_result_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_updatelink(%s): to s%d]", path, server_id);

    if (giga_rpc_updatelink_1(dir_id, (char*)path, (char*)link,
                              &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_updatelink(%s)", clnt_spcreateerror(path));
        exit(1);
    }

    // check return condition
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        LOG_MSG("rpc_updatelink: bitmap update from s%d -- RETRY ...",
                server_id);
        goto retry;
    }
    cache_release(dir);

    LOG_MSG("RPC_updatelink(%s): status=[%d]%s", path, ret, strerror(ret));
    return ret;
}


int rpc_open(int dir_id, int zeroth_srv, const char *path, int mode,
             int* state, char *link)
{
    int ret = 0;

    LOG_MSG(">>> RPC_open(%s, d%d)", path, dir_id);

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    static giga_open_reply_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_open(%s): to s%d]", path, server_id);

    if (giga_rpc_open_1(dir_id, (char*)path, mode, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_open(%s)", clnt_spcreateerror(path));
        exit(1);//TODO: retry again?
    }

    // check return condition
    //
    ret = rpc_reply.result.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.result.giga_result_t_u.bitmap);
        LOG_MSG("rpc_open: bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret == 0){
        *state = rpc_reply.state;
        if (rpc_reply.state == RPC_LEVELDB_FILE_IN_FS) {
           strncpy(link, rpc_reply.link, PATH_MAX);
        }
    }
    cache_release(dir);

    LOG_MSG("RPC_open(%s): status=[%d]%s", path, ret, strerror(ret));
    return ret;
}

int rpc_close(int dir_id, int zeroth_srv, const char *path)
{
    int ret = 0;

    struct giga_directory *dir = cache_lookup(&dir_id);
    if (dir == NULL) {
      dir = rpc_getpartition(dir_id, zeroth_srv);
    }
    if (dir == NULL) {
        LOG_MSG("ERR_cache: dir(%d) missing!", dir_id);
        return -EIO;
    }

    int server_id = 0;
    giga_result_t rpc_reply;
    memset(&rpc_reply, 0, sizeof(rpc_reply));

retry:
    server_id = get_server_for_file(dir, path);
    CLIENT *rpc_clnt = getConnection(server_id);

    LOG_MSG(">>> RPC_close(%s): to s[%d]", path, server_id);

    if (giga_rpc_close_1(dir_id, (char*)path, &rpc_reply, rpc_clnt)
        != RPC_SUCCESS) {
        LOG_ERR("ERR_rpc_close(%s)", clnt_spcreateerror(path));
        exit(1);
    }

    // check return condition
    //
    ret = rpc_reply.errnum;
    if (ret == -EAGAIN) {
        update_client_mapping(dir, &rpc_reply.giga_result_t_u.bitmap);
        LOG_MSG("rpc_close: bitmap update from s%d -- RETRY ...", server_id);
        goto retry;
    } else if (ret == 0){

    }

    cache_release(dir);
    LOG_MSG("<<< RPC_close(%s): status=[%d]%s", path, ret, strerror(ret) );
    return ret;
}
