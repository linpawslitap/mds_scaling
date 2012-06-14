#ifndef OPTIONS_H
#define OPTIONS_H

#include <rpc/rpc.h>

#include "defaults.h"

typedef enum backends {
    /* Non-networked, local backends */
    BACKEND_LOCAL_FS,           /* Local file system */
    BACKEND_LOCAL_LEVELDB,      /* Local levelDB */

    /* Networked, RPC-based backends */
    BACKEND_RPC_LOCALFS,        /* ship ops via RPC to server */
    BACKEND_RPC_LEVELDB
} backend_t;

#define GIGA_CLIENT 12345
#define GIGA_SERVER 67890

/*
 * Configuration options used by GIGA+ client and server.
 */
struct giga_options {

   /* 
    * Common for both clients and servers 
    * */
   char *mountpoint;            /* client's mountpoint and server's backend */
   backend_t backend_type;      /* string to specify type of backend */
   
   char *hostname;              /* SELF hostname */
   char *ip_addr;               /* SELF ip address */
   int port_num;
   
   int num_servers;             /* num of servers in the server list */
   const char **serverlist;     /* server list GIGA+ nodes */
   int split_threshold;         /* default split threshold */
   
   /* 
    * Server specific parameters 
    * */
   int serverID;                       /* ID of the current server */

   /* 
    * Client-specific parameters.
    * */

};

extern struct giga_options giga_options_t;

void initGIGAsetting(int cli_or_srv, char *mnt_dir, const char *srv_list_file);

#endif
