
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"
#include "common/options.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
void init_default_backends()
{
    //logOpen(DEFAULT_LOG_FILE_LOCATIONc, LOG_WARN);

    giga_options_t.backend_type = BACKEND_RPC_LEVELDB;
    //giga_options_t.backend_type = BACKEND_RPC_LOCALFS;

    giga_options_t.mountpoint = (char*)malloc(sizeof(char)*MAX_LEN);
    if (giga_options_t.mountpoint == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }
    if (giga_proc_type == GIGA_CLIENT) 
        strncpy(giga_options_t.mountpoint, 
                DEFAULT_CLI_MNT_POINT, strlen(DEFAULT_CLI_MNT_POINT)+1);
    else if (giga_proc_type == GIGA_SERVER)
        strncpy(giga_options_t.mountpoint, 
                DEFAULT_SRV_BACKEND, strlen(DEFAULT_SRV_BACKEND)+1);

    logMessage(LOG_TRACE, __func__, "BACKEND_TYPE=%s", 
               backends_str[giga_options_t.backend_type]);
    logMessage(LOG_TRACE, __func__, "BACKEND_MNT=%s", giga_options_t.mountpoint);
}


static 
void init_self_network_IDs()
{
    giga_options_t.hostname = NULL;
    giga_options_t.ip_addr = NULL;
    giga_options_t.port_num = DEFAULT_PORT;
    
    if ((giga_options_t.ip_addr = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }
    
    getHostIPAddress(giga_options_t.ip_addr, MAX_LEN);

    if ((giga_options_t.hostname = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        exit(1);
    }
    if (gethostname(giga_options_t.hostname, MAX_LEN) < 0) {
        logMessage(LOG_FATAL, __func__, "gethostname_err: %s", strerror(errno));
        exit(1);
    }

    logMessage(LOG_TRACE, __func__, "SELF_HOSTNAME=%s", giga_options_t.hostname);
    logMessage(LOG_TRACE, __func__, "SELF_IP=%s", giga_options_t.ip_addr);
    logMessage(LOG_TRACE, __func__, "SELF_PORT=%d", giga_options_t.port_num);
}


static 
void parse_serverlist_file(const char *serverlist_file)
{
    FILE *conf_fp;
    char ip_addr[MAX_LEN];

    if ((conf_fp = fopen(serverlist_file, "r+")) == NULL) {
        logMessage(LOG_FATAL, __func__, "err_open(conf=%s).", serverlist_file);
        exit(1);
    }
   
    giga_options_t.split_threshold = DEFAULT_SPLIT_THRESHOLD;
    giga_options_t.serverlist = NULL;
    giga_options_t.num_servers = 0;
    
    if ((giga_options_t.serverlist = malloc(sizeof(char*)*MAX_LEN)) == NULL) {
        logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
        fclose(conf_fp);
        exit(1);
    }

    logMessage(LOG_TRACE, __func__, "SERVER_LIST=...");
    while (fgets(ip_addr, MAX_LEN, conf_fp) != NULL) {
        ip_addr[strlen(ip_addr)-1]='\0';

        int i = giga_options_t.num_servers;
        giga_options_t.serverlist[i] = (char*)malloc(sizeof(char)*MAX_LEN);
        if (giga_options_t.serverlist[i] == NULL) {
            logMessage(LOG_FATAL, __func__, "malloc_err: %s", strerror(errno));
            exit(1);
        }
        strncpy((char*)giga_options_t.serverlist[i], ip_addr, strlen(ip_addr)+1);

        giga_options_t.num_servers += 1;

        if (strcmp(giga_options_t.serverlist[i], giga_options_t.ip_addr) == 0)
            giga_options_t.serverID = i;

        logMessage(LOG_TRACE, __func__, "-->server_%d={%s}\n", 
                   giga_options_t.num_servers-1, 
                   giga_options_t.serverlist[giga_options_t.num_servers-1]); 
    }

    logMessage(LOG_TRACE, __func__, "NUM_SERVERS=%d",giga_options_t.num_servers);

    fclose(conf_fp);
}

static
void print_settings()
{
    if (giga_proc_type == GIGA_CLIENT) {
        logMessage(LOG_FATAL, __func__, "=================="); 
        logMessage(LOG_FATAL, __func__, "Settings: client. ");
        logMessage(LOG_FATAL, __func__, "=================="); 
        
        logMessage(LOG_FATAL, __func__, 
                   "SELF_HOSTNAME=%s", giga_options_t.hostname);
        logMessage(LOG_FATAL, __func__, 
                   "SELF_IP=%s", giga_options_t.ip_addr);
        logMessage(LOG_FATAL, __func__, 
                   "SELF_PORT=%d", giga_options_t.port_num);
        
        logMessage(LOG_FATAL, __func__, "\n");
        
        logMessage(LOG_FATAL, __func__, 
                   "BACKEND_TYPE=%s", backends_str[giga_options_t.backend_type]);
        logMessage(LOG_FATAL, __func__, 
                   "BACKEND_MNT=%s", giga_options_t.mountpoint);
        
        logMessage(LOG_FATAL, __func__, "\n");
        
        logMessage(LOG_FATAL, __func__, 
                   "NUM_SERVERS=%d",giga_options_t.num_servers);
        logMessage(LOG_FATAL, __func__, 
                   "SPLIT_THRESHOLD=%d", giga_options_t.split_threshold);
        
        logMessage(LOG_FATAL, __func__, "==================\n");

        return;
    }

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
    
    return;
}

void initGIGAsetting(int process_type, const char *serverlist_file)
{
    giga_proc_type = process_type;  // client or server flag

    init_default_backends();
    init_self_network_IDs();
    parse_serverlist_file(serverlist_file);

    print_settings();
}
