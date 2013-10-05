/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-13 11:51:49
* File Name: ./rpc_fs_test.c
* Description:
 ************************************************************************/

#include "common/options.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"
#include "common/rpc_giga.h"
#include "backends/operations.h"

struct giga_options giga_options_t;

int main(int argc, char *argv[]) {

    (void) argc;
    (void) argv;

    char log_file[PATH_MAX] = {0};
    snprintf(log_file, sizeof(log_file),
             "%s.c.%d", DEFAULT_LOG_FILE_PATH, (int)getpid());
    int ret;
    if ((ret = logOpen(log_file, DEFAULT_LOG_LEVEL)) < 0) {
        fprintf(stdout, "***ERROR*** during opening log(%s) : [%s]\n",
                log_file, strerror(ret));
        return ret;
    }
    memset(&giga_options_t, 0, sizeof(struct giga_options));
    initGIGAsetting(GIGA_CLIENT, DEFAULT_MNT, CONFIG_FILE);

    rpcInit();
    if (rpc_init() < 0) {
        LOG_ERR("RPC_init_err(%s)", ROOT_DIR_ID);
        exit(1);
    }

    int server_id;
    for (server_id = 0; server_id < 64; ++server_id) {
        struct giga_directory* dir = rpc_getpartition(34855, server_id);
        if (dir !=  NULL) {
          printf("Found the directory: %d.\n", server_id);
        } else {
          printf("Not found the directory: %d.\n", server_id);
        }
    }

    logClose();
    return 0;
}
