/*
 * RPC definitions for the SkyeFs
 */

#include <asm-generic/errno-base.h>

#ifdef RPC_HDR
%#include <sys/types.h>
%#include "giga_index.h"
%#include "rpc_helper.h"
#elif RPC_XDR
%#include "rpc_helper.h"
%#pragma GCC diagnostic ignored "-Wunused-variable"
%#pragma GCC diagnostic ignored "-Wunused-parameter"
#elif RPC_SVC
%#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

typedef int giga_dir_id;
typedef string giga_pathname<PATH_MAX>;
typedef opaque giga_file_data<65536>;
typedef struct giga_mapping_t giga_bitmap;

typedef struct scan_entry_t* scan_list_t;

struct info_t {
    int permission;
    int is_dir;
    int uid;
    int gid;
    int size;
    int atime;
    int ctime;
};

struct scan_entry_t {
    giga_pathname           entry_name;
    info_t                  info;
    struct scan_entry_t*    next;
};

typedef opaque scan_key<PATH_MAX>;
/*
typedef string scan_key<>;
typedef char* scan_key;
*/

struct scan_result_t {
    scan_key    end_key;
    int         end_partition;
    scan_list_t list;
    int         num_entries;
    int         more_entries_flag;
    giga_bitmap bitmap;
};

struct scan_args_t {
    scan_key    start_key;
    int         dir_id;
    int         partition_id;
};

union readdir_return_t switch (int errnum) {
    case 0:
        struct scan_result_t result;
    case -EAGAIN:
        giga_bitmap bitmap;
    default:
      void;
};

union readdir_result_t switch (int errnum) {
    case 0:
        scan_list_t list;
    case -EAGAIN:
        giga_bitmap bitmap;
    default:
        void;
};

struct giga_timestamp_t {
    int tv_sec;
    long tv_nsec;
};

union giga_result_t switch (int errnum) {
    case -EAGAIN:
        giga_bitmap bitmap;
    default:
        void;
};

union giga_lookup_t switch (int errnum) {
    case 0:
        giga_dir_id dir_id;
    case -EAGAIN:
        giga_bitmap bitmap;
    default:
        void;
};

union giga_read_t switch (int state) {
    case RPC_LEVELDB_FILE_IN_DB:
        giga_file_data buf;
    case RPC_LEVELDB_FILE_IN_FS:
        giga_pathname link;
    default:
        void;
};

struct giga_getattr_reply_t {
    struct stat statbuf;
    int file_size;
    int zeroth_server;
    giga_result_t result;
    /**int fn_retval;*/
};

struct giga_open_reply_t {
    int state;
    giga_pathname link;
    giga_result_t result;
    /**int fn_retval;*/
};

struct giga_fetch_reply_t {
    giga_read_t data;
    giga_result_t result;
    /**int fn_retval;*/
};

struct giga_write_reply_t {
    giga_result_t result;
    int state;
    giga_pathname link;
    /**int fn_retval;*/
};

struct giga_read_reply_t {
    giga_result_t result;
    giga_read_t data;
    /**int fn_retval;*/
};

/* RPC definitions */

program GIGA_RPC_PROG {                 /* program number */
version GIGA_RPC_VERSION {          /* version number */
      /* Initial RPC.
           - REQUEST: client sends "number of servers".
           - REPLY: servers check, respond with its know "number of servers".
        */
      /*int GIGA_RPC_INIT(int) = 1;*/
        giga_result_t GIGA_RPC_INIT(int) = 1;

        giga_getattr_reply_t GIGA_RPC_GETATTR(giga_dir_id, giga_pathname) = 101;

        giga_result_t GIGA_RPC_GETMAPPING(giga_dir_id) = 102;

        giga_result_t GIGA_RPC_MKDIR(giga_dir_id, giga_pathname, mode_t) = 201;

        giga_result_t GIGA_RPC_MKZEROTH(giga_dir_id) = 202;

        giga_result_t GIGA_RPC_MKNOD(giga_dir_id, giga_pathname,
                                     mode_t, short) = 301;

        giga_result_t GIGA_RPC_CHMOD(giga_dir_id, giga_pathname, mode_t) = 302;

        /*
        readdir_result_t GIGA_RPC_READDIR(giga_dir_id, int) = 501;
        readdir_return_t GIGA_RPC_READDIR_REQ(giga_dir_id, int, scan_key) = 502;
        */
        readdir_return_t GIGA_RPC_READDIR_SERIAL(scan_args_t) = 502;

        /* {dir_to_split, parent_index, child_index, path_leveldb_files,
            partition map} */
        giga_result_t GIGA_RPC_SPLIT(giga_dir_id, int, int,
                                     giga_pathname,
                                     giga_bitmap bitmap,
                                     uint64_t, uint64_t, int) = 401;

        giga_write_reply_t GIGA_RPC_WRITE(giga_dir_id, giga_pathname,
                                         giga_file_data data, int offset) = 601;

        giga_result_t GIGA_RPC_UPDATELINK(giga_dir_id, giga_pathname,
                                          giga_pathname) = 602;


        giga_read_reply_t GIGA_RPC_READ(giga_dir_id, giga_pathname,
                                        int size, int offset) = 701;

        giga_open_reply_t GIGA_RPC_OPEN(giga_dir_id, giga_pathname,
                                        int mode ) = 801;

        giga_fetch_reply_t GIGA_RPC_FETCH(giga_dir_id, giga_pathname) = 802;

        giga_result_t GIGA_RPC_CLOSE(giga_dir_id, giga_pathname) = 901;


        /* CLIENT API */
        /*giga_lookup_t RPC_CREATE(giga_dir_id, giga_pathname, mode_t) = 101;*/

  } = 1;
} = 522222; /* FIXME: Is this a okay value for program number? */
