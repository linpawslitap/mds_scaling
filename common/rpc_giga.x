/*
 * RPC definitions for the SkyeFs
 */

#include <asm-generic/errno-base.h>
#include "defaults.h"

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
typedef string giga_pathname<MAX_LEN>;
typedef opaque giga_file_data<MAX_SIZE>;
typedef struct giga_mapping_t giga_bitmap;

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

struct giga_getattr_reply_t {
    struct stat statbuf;
    giga_result_t result;
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

        giga_result_t GIGA_RPC_MKDIR(giga_dir_id, giga_pathname, mode_t) = 201;

        giga_result_t GIGA_RPC_MKNOD(giga_dir_id, giga_pathname, mode_t, short) = 301;
        /* {dir_to_split, parent_index, child_index, path_leveldb_files} */
        giga_result_t GIGA_RPC_SPLIT(giga_dir_id, int, int, giga_pathname, 
                                     uint64_t, uint64_t, int) = 401;
		
        /* CLIENT API */
		/*giga_lookup_t RPC_CREATE(giga_dir_id, giga_pathname, mode_t) = 101;*/

	} = 1;
} = 522222; /* FIXME: Is this a okay value for program number? */
