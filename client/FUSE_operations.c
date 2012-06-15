
#include "common/cache.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"
#include "common/rpc_giga.h"

#include "backends/operations.h"

#include "client.h"
#include "FUSE_operations.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fuse.h>
#include <rpc/rpc.h>
#include <unistd.h>

#define _LEVEL_     LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(_LEVEL_, __func__, format, __VA_ARGS__); 

static struct MetaDB ldb_mds;
static uint64_t object_id = 0;

static int  parse_path_components(const char *path, char *file, char *dir);
static void get_full_path(char fpath[MAX_LEN], const char *path);

#define FUSE_ERROR(x)   -(x)

/*
 * Parse the full path name of the file to give the name of the file and the
 * name of the file's parent directory.
 *
 * -- Example: A path "/a/b/f.txt" yields "f.txt" as file and "b" as dir
 */
static int parse_path_components(const char *path, char *file, char *dir)
{
	const char *p = path;
	if (!p || !file)
		return -1;

	if (strcmp(path, "/") == 0) {
		strcpy(file, "/");
		if (dir)
			strcpy(dir, "/");

		return 0;
	}

    int pathlen = strlen(path);

    if (pathlen > MAX_LEN)
        return -ENAMETOOLONG;
    
    p += pathlen;
	while ( (*p) != '/' && p != path)
		p--; // Come back till '/'

    if (pathlen - (int)(p - path) > MAX_LEN)
        return -ENAMETOOLONG;

    // Copy after slash till end into filename
	strncpy(file, p+1, MAX_LEN); 
	if (dir) {
		if (path == p)
			strncpy(dir, "/", 2);
		else {
            // Copy rest into dirpath 
			strncpy(dir, path, (int)(p - path )); 
			dir[(int)(p - path)] = '\0';
		}
	}

    LOG_MSG("parsed [%s] to {file=[%s],dir=[%s]}", path, file, dir);
	return 0;
}

/*
 * Appends a given path name with something else.
 *
 */
static void get_full_path(char fpath[MAX_LEN], const char *path)
{
    strncpy(fpath, 
            giga_options_t.mountpoint, strlen(giga_options_t.mountpoint)+1);  
    strncat(fpath, path, MAX_LEN);          //XXX: long path names with break!

    LOG_MSG("converted [%s] to [%s]", path, fpath);

    return;
}

void* GIGAinit(struct fuse_conn_info *conn)
{
    LOG_MSG(">>> FUSE_init(%d)", ROOT_DIR_ID);

    (void)conn;

    char ldb_name[MAX_LEN];

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_LEVELDB:
            snprintf(ldb_name, sizeof(ldb_name), "%s/l%d", 
                     DEFAULT_LEVELDB_DIR, 0);
            metadb_init(&ldb_mds, ldb_name);
            object_id = 0;
            if (metadb_create(ldb_mds,
                              ROOT_DIR_ID, 0,
                              OBJ_DIR,
                              object_id, "/", giga_options_t.mountpoint) < 0) {
                LOG_ERR("root entry creation error(%s)", ROOT_DIR_ID);
                exit(1);
            }
            break;
        case BACKEND_RPC_LOCALFS:
            break;
        case BACKEND_RPC_LEVELDB:
            rpcInit();
            //rpcConnect();     //FIXME: I don't need this 
            if (rpc_init() < 0) {
                LOG_ERR("RPC_init_err(%s)", ROOT_DIR_ID);
                exit(1);
            }  
            break;
        default:
            break;
    }
    
    LOG_MSG("<<< FUSE_init(%d)", ROOT_DIR_ID);
    return NULL;
}

void GIGAdestroy(void * unused)
{
    (void)unused;

    logClose();
    
    LOG_MSG(">>> FUSE_destroy(%d) <<<", ROOT_DIR_ID);

    // FIXME: check cleanup code.
    //rpc_disconnect();
}


int GIGAgetattr(const char *path, struct stat *statbuf)
{
    LOG_MSG(">>> FUSE_getattr(p=[%s]): stat=[0x%08x]", path, statbuf);

    int ret = 0;
    char fpath[MAX_LEN] = {0};
    char dir[MAX_LEN] = {0};
    char file[MAX_LEN] = {0};
    int dir_id = 0;
   
    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_getattr(fpath, statbuf);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            ret  = metadb_lookup(ldb_mds,
                                 0, 0, file,
                                 statbuf);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            parse_path_components(path, file, dir);
            //TODO: convert "dir" to "dir_id"
            ret = rpc_getattr(dir_id, file, statbuf);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LOCALFS:
            parse_path_components(path, file, dir);
            //TODO: convert "dir" to "dir_id"
            ret = rpc_getattr(dir_id, file, statbuf);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }

    LOG_MSG("<<< FUSE_getattr(p=%s): ret=[%d]", path, ret);
    
    return ret;
}

int GIGAmkdir(const char *path, mode_t mode)
{
    LOG_MSG(">>> FUSE_mkdir(p=[%s]): mode=[%lo]", path, (unsigned long)mode);

    int ret = 0;
    char fpath[MAX_LEN] = {0};
    char dir[MAX_LEN] = {0};
    char file[MAX_LEN] = {0};
    int dir_id = 0;
    
    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_mkdir(fpath, mode);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            object_id += 1;
            ret  = metadb_create(ldb_mds, 0, 0,
                                 OBJ_DIR,
                                 object_id, file, path);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            parse_path_components(path, file, dir);
            ret = rpc_mkdir(dir_id, file, mode);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LOCALFS:
            parse_path_components(path, file, dir);
            ret = rpc_mkdir(dir_id, file, mode);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }
    
    LOG_MSG("<<< FUSE_mkdir(p=%s): status=[%d] ", path, ret);
    
    return ret;
}

    
int GIGAmknod(const char *path, mode_t mode, dev_t dev)
{
    LOG_MSG(">>> FUSE_mknod(p=[%s]): mode=[0%3o],dev=[%lld]", path, mode, dev);

    int ret = 0;
    char fpath[PATH_MAX];
    char dir[MAX_LEN] = {0};
    char file[MAX_LEN] = {0};
    int dir_id = 0;
    
    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_mknod(fpath, mode, dev);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_LOCAL_LEVELDB:
            parse_path_components(path, file, dir);
            object_id += 1;
            ret  = metadb_create(ldb_mds, 0, 0,
                                 OBJ_MKNOD,
                                 object_id, file, path);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LOCALFS:
            ;
        case BACKEND_RPC_LEVELDB:
            parse_path_components(path, file, dir);
            ret = rpc_mknod(dir_id, file, mode, dev);
            ret = FUSE_ERROR(ret);
            break;
        default:
            break;
    }
    
    LOG_MSG("<<< FUSE_mknod(p=%s): status=[%d] ", path, ret);
    return ret;
}

/*
#######
*/


int GIGAsymlink(const char *path, const char *link)
{
    LOG_MSG(" ==> symlink(path=[%s], link=[%s])", path, link);

    int ret = 0;
    char flink[MAX_LEN] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(flink, link);
            ret = local_symlink(path, flink);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            break;
        default:
            break;
    }
    
    return ret;
}


int GIGAreadlink(const char *path, char *link, size_t size)
{
    LOG_MSG(" ==> readlink(path=[%s],link=[%s],size[%d])", path, link, size);
    
    int ret = 0;
    char fpath[MAX_LEN] = {0};

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            ret = local_readlink(fpath, link, size);
            ret = FUSE_ERROR(ret);
            break;
        case BACKEND_RPC_LEVELDB:
            break;
        default:
            break;
    }
    
    LOG_MSG("ret_readlink(link=[%s], size[%d])", path, link, strlen(link));
    
    return ret;
}

int GIGAopen(const char *path, struct fuse_file_info *fi)
{
    LOG_MSG(" ==> open(path=[%s], fi=[0x%08x])", path, fi);
    
    int ret = 0;
    char fpath[MAX_LEN] = {0};
    int fd;

    switch (giga_options_t.backend_type) {
        case BACKEND_LOCAL_FS:
            get_full_path(fpath, path);
            if ((ret = local_open(fpath, fi->flags, &fd)) < 0)
                ret = FUSE_ERROR(ret);
            fi->fh = fd;
            break;
        case BACKEND_RPC_LEVELDB:
            break;
        default:
            break;
    }

    /*
    if ((fd = open(fpath, fi->flags)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "open(%s) failed: %s", fpath, strerror(errno));
        ret = FUSE_ERROR(errno);
    }
    
    fi->fh = fd;
    */

    LOG_MSG(" ret_open(path=[%s], fi=[%d])", path, fi->fh);
    
    return ret;
}
    
