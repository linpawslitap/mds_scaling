#ifndef DEFAULTS_H
#define DEFAULTS_H   

/* client and server settings specific constants */
#define DEFAULT_MOUNT "./"
#define DEFAULT_PVFS_FS "tcp://localhost:3334/pvfs2-fs"

/* The following excerpt is from the /usr/include/fuse/fuse.h
 *
 * IMPORTANT: you should define FUSE_USE_VERSION before including this
 * header.  To use the newest API define it to 26 (recommended for any
 * new application), to use the old API define it to 21 (default) 22
 * or 25, to use the even older 1.X API define it to 11.
*/
#define FUSE_USE_VERSION    26

#define FUSE_SUCCESS    0

/*
 * Default options for client/server related information
 */

#define DEFAULT_NUM_OF_SERVERS  1               /* server list size */
#define DEFAULT_SERVER_NUMBER   0               /* server number for self */
#define DEFAULT_CLI_MNT_POINT   "/tmp/giga_c/" /* FUSE mount point @ client */
/*#define DEFAULT_SRV_BACKEND     "/tmp/giga_s/"*/ /* Backend @ server */
#define DEFAULT_SRV_BACKEND     "/m/pvfs/giga_srv/" /* Backend @ server */
#define DEFAULT_BACKEND_TYPE    0               /* Backend type = local */

#define DEFAULT_PORT            55677           /* Server's port number */
#define DEFAULT_IP              "127.0.0.1"     /* Server's IP address */

#define DEFAULT_LOG_DIRECTORY   "/tmp/logs/"   /* log directory location */
#define DEFAULT_LOG_FD_PREFIX   "giga_log"      /* log file prefix */

#define DEFAULT_CONF_FILE       "/tmp/test_conf_file"

/*#define DEFAULT_LEVELDB_DIR     "/tmp/ldb"*/
#define DEFAULT_LEVELDB_DIR     "/m/pvfs/giga_ldb/"
#define DEFAULT_LEVELDB_PREFIX  "ldb-giga"

#define DEFAULT_SPLIT_THRESHOLD 100

#define ROOT_DIR_ID 0

/* 
 * Sizes of different string lengths and buffer lengths 
 * 
 */
#define MAX_LEN     512     /* things with a "length" (e.g., path name, ip) */
#define MAX_SIZE    4096    /* things with a "buffer" (e.g., read/write) */

/*
#define MAX_FILENAME_LEN    256
#define MAX_PATHNAME_LEN    4096
#define MAX_HOSTNAME_LEN    64

#define MAX_DBG_STR_LEN     128

#define MAX_IP_ADDR_LEN     24
*/

/* 
 * File/Directory permission bits 
 * */
#define DEFAULT_MODE    (S_IRWXU | S_IRWXG | S_IRWXO )

#define USER_RW         (S_IRUSR | S_IWUSR)
#define GRP_RW          (S_IRGRP | S_IWGRP)
#define OTHER_RW        (S_IROTH | S_IWOTH)

#define CREATE_MODE     (USER_RW | GRP_RW | OTHER_RW)
#define CREATE_FLAGS    (O_CREAT | O_APPEND)
#define CREATE_RDEV     0


#endif /* DEFAULTS_H */
