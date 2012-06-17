#ifndef OPTIONS_H
#define OPTIONS_H

#include <rpc/rpc.h>

typedef enum backends {
    // Non-networked, local backends
    BACKEND_LOCAL_FS,           // Local file system
    BACKEND_LOCAL_LEVELDB,      // Local levelDB

    // Networked, RPC-based backends
    BACKEND_RPC_LOCALFS,        // ship ops via RPC to server 
    BACKEND_RPC_LEVELDB         // LevelDB mounted on networked config
} backend_t;

#define NFS 

#ifdef  LOCAL        /* LocalFS */
#define DEFAULT_BACKEND_TYPE    BACKEND_LOCAL_FS
#define DEFAULT_SRV_BACKEND     "/tmp/giga_srv/"
#define DEFAULT_LEVELDB_DIR     "/tmp/giga_ldb/"
#define DEFAULT_SPLIT_DIR       "/tmp/splits/"
#endif

#ifdef  PVFS        /* PVFS mounted backends (for everything) */
#define DEFAULT_BACKEND_TYPE    BACKEND_RPC_LEVELDB
#define DEFAULT_SRV_BACKEND     "/m/pvfs/giga_srv/"
#define DEFAULT_LEVELDB_DIR     "/m/pvfs/giga_ldb/"
#define DEFAULT_SPLIT_DIR       "/m/pvfs/splits/"
#endif

#ifdef  NFS         /* LevelDB splits through NFS, everything else is local */
#define DEFAULT_BACKEND_TYPE    BACKEND_RPC_LEVELDB
#define DEFAULT_SRV_BACKEND     "/l0/giga_srv/"
#define DEFAULT_LEVELDB_DIR     "/l0/giga_ldb/"
#define DEFAULT_SPLIT_DIR       "/users/svp/_splits/"
#endif


#define GIGA_CLIENT 12345
#define GIGA_SERVER 67890

// Configuration options used by GIGA+ client and server.
//
struct giga_options {

    //Common for both clients and servers 
    //
    char *hostname;             // SELF hostname
    char *ip_addr;              // SELF server ip address
    int port_num;               // SELF server port num
   
    int num_servers;            // num of servers in the server list 
    const char **serverlist;    // server list GIGA+ nodes
    
    char *mountpoint;           // client's mountpoint and server's backend
    backend_t backend_type;     // string to specify type of backend
   
    // Server specific parameters.
    //
    int serverID;               // ID of the current server
    int split_threshold;        // default split threshold 
    
    // Client-specific parameters.
    //

};

extern struct giga_options giga_options_t;

void initGIGAsetting(int cli_or_srv, char *mnt_dir, const char *srv_list_file);

#endif
