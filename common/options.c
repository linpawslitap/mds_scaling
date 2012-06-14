
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"
#include "common/options.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define LOG_MSG(format, ...) logMessage(LOG_FATAL, NULL, format, __VA_ARGS__); 

static char* backends_str[] = {
    /* Non-networked, local backends */
    "BACKEND_LOCAL_FS",           /* Local file system */
    "BACKEND_LOCAL_LEVELDB",      /* Local levelDB */

    /* Networked, RPC-based backends */
    "BACKEND_RPC_LOCALFS",        /* ship ops via RPC to server */
    "BACKEND_RPC_LEVELDB"
};

static int giga_proc_type;

static 
void init_default_backends(const char *cli_mnt)
{
    //giga_options_t.backend_type = BACKEND_LOCAL_LEVELDB;
    giga_options_t.backend_type = BACKEND_RPC_LEVELDB;
    //giga_options_t.backend_type = BACKEND_RPC_LOCALFS;

    giga_options_t.mountpoint = (char*)malloc(sizeof(char) * MAX_LEN);
    if (giga_options_t.mountpoint == NULL) {
        LOG_MSG("ERR_malloc: %s", strerror(errno));
        exit(1);
    }

    if (giga_proc_type == GIGA_CLIENT) { 
        //strncpy(giga_options_t.mountpoint, 
        //        DEFAULT_CLI_MNT_POINT, strlen(DEFAULT_CLI_MNT_POINT)+1);
        snprintf(giga_options_t.mountpoint, MAX_LEN, "%s/", cli_mnt); 
    }
    else if (giga_proc_type == GIGA_SERVER) {
        snprintf(giga_options_t.mountpoint, MAX_LEN, 
                 "%s/s%d/", DEFAULT_SRV_BACKEND, giga_options_t.serverID); 
        /*
        if (mkdir(giga_options_t.mountpoint, DEFAULT_MODE) < 0) {
            if (errno != EEXIST) {
                LOG_MSG("ERR_mkdir(%s): [%s]", 
                        giga_options_t.mountpoint, strerror(errno));
                exit(1);
            }
        }
        */
    }

}


static 
void init_self_network_IDs()
{
    giga_options_t.hostname = NULL;
    giga_options_t.ip_addr = NULL;
    giga_options_t.port_num = DEFAULT_PORT;
    
    if ((giga_options_t.ip_addr = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        LOG_MSG("ERR_malloc: %s", strerror(errno));
        exit(1);
    }
    
    getHostIPAddress(giga_options_t.ip_addr, MAX_LEN);

    if ((giga_options_t.hostname = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        LOG_MSG("ERR_malloc: %s", strerror(errno));
        exit(1);
    }
    if (gethostname(giga_options_t.hostname, MAX_LEN) < 0) {
        LOG_MSG("ERR_gethostname: %s", strerror(errno));
        exit(1);
    }

}


static 
void parse_serverlist_file(const char *serverlist_file)
{
    FILE *conf_fp;
    char ip_addr[MAX_LEN];

    if ((conf_fp = fopen(serverlist_file, "r+")) == NULL) {
        LOG_MSG("ERR_open(%s): %s", serverlist_file, strerror(errno));
        exit(1);
    }
   
    giga_options_t.split_threshold = DEFAULT_SPLIT_THRESHOLD;
    giga_options_t.serverlist = NULL;
    giga_options_t.num_servers = 0;
    
    if ((giga_options_t.serverlist = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        LOG_MSG("ERR_malloc: %s", strerror(errno));
        fclose(conf_fp);
        exit(1);
    }

    //logMessage(LOG_TRACE, __func__, "SERVER_LIST=...");
    while (fgets(ip_addr, MAX_LEN, conf_fp) != NULL) {
        if (ip_addr[0] == '#')
            continue;

        ip_addr[strlen(ip_addr)-1]='\0';

        int i = giga_options_t.num_servers;
        giga_options_t.serverlist[i] = (char*)malloc(sizeof(char)*MAX_LEN);
        if (giga_options_t.serverlist[i] == NULL) {
            LOG_MSG("ERR_malloc: %s", strerror(errno));
            fclose(conf_fp);
            exit(1);
        }
        strncpy((char*)giga_options_t.serverlist[i], ip_addr, strlen(ip_addr)+1);

        giga_options_t.num_servers += 1;

        if (strcmp(giga_options_t.serverlist[i], giga_options_t.ip_addr) == 0)
            giga_options_t.serverID = i;

        //logMessage(LOG_TRACE, __func__, "-->server_%d={%s}\n", 
        //           giga_options_t.num_servers-1, 
        //           giga_options_t.serverlist[giga_options_t.num_servers-1]); 
    }

    fclose(conf_fp);
}

static
void print_settings()
{
    if (giga_proc_type == GIGA_CLIENT) { 
        LOG_MSG("### client[%d]\n",(int)getpid());
    } else {
        assert(giga_options_t.serverID != -1); 
        LOG_MSG("### server[%d]\n", giga_options_t.serverID); 
    }

    LOG_MSG("-- SELF_HOSTNAME=%s", giga_options_t.hostname);
    LOG_MSG("-- SELF_IP=%s", giga_options_t.ip_addr);
    LOG_MSG("-- SELF_PORT=%d\n", giga_options_t.port_num);
    
    LOG_MSG("-- BACKEND_TYPE=%s", backends_str[giga_options_t.backend_type]);
    LOG_MSG("-- BACKEND_MNT=%s\n", giga_options_t.mountpoint);
    
    LOG_MSG("-- NUM_SERVERS=%d",giga_options_t.num_servers);
    LOG_MSG("-- SPLIT_THRESHOLD=%d\n", giga_options_t.split_threshold);
    

#if 0
    fprintf(stdout, "==================\n"); 
    fprintf(stdout, "Settings: server[%d]\n", giga_options_t.serverID); 
    fprintf(stdout, "==================\n"); 
    fprintf(stdout, "\tSELF_HOSTNAME=%s\n", giga_options_t.hostname);
    fprintf(stdout, "\tSELF_IP=%s\n", giga_options_t.ip_addr);
    fprintf(stdout, "\tSELF_PORT=%d\n", giga_options_t.port_num);
    fprintf(stdout, "\n");
    fprintf(stdout, "\tBACKEND_TYPE=%s\n", backends_str[giga_options_t.backend_type]);
    fprintf(stdout, "\tBACKEND_MNT=%s\n", giga_options_t.mountpoint);
    fprintf(stdout, "\n");
    fprintf(stdout, "\tNUM_SERVERS=%d\n",giga_options_t.num_servers);
    fprintf(stdout, "\tSPLIT_THRESHOLD=%d\n",giga_options_t.split_threshold);
    fprintf(stdout, "==================\n"); 
#endif    

    return;
}

void initGIGAsetting(int cli_or_srv, char *cli_mnt, const char *srv_list_file)
{
    giga_proc_type = cli_or_srv;  // client or server flag

    init_self_network_IDs();
    parse_serverlist_file(srv_list_file);
    if (giga_proc_type == GIGA_CLIENT)
        init_default_backends((const char*)cli_mnt);
    else
        init_default_backends(NULL);

    print_settings();
}
