
#include "server.h"
#include "split.h"

#include "common/rpc_giga.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/options.h"

#include "backends/operations.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <rpc/clnt.h>
#include <rpc/pmap_clnt.h>
#include <rpc/rpc.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

#define SRV_LOG     LOG_DEBUG

#define LOG_MSG(format, ...) \
    logMessage(SRV_LOG, __func__, format, __VA_ARGS__);

#define INIT_OBJECT_ID  ((int)(giga_options_t.serverID*1000))

// RPC specific functions
//
extern SVCXPRT *svcfd_create (int __sock, u_int __sendsize, u_int __recvsize);
// FIXME: rpcgen should put this in giga_rpc.h, but it doesn't. Why?
extern void giga_rpc_prog_1(struct svc_req *rqstp, register SVCXPRT *transp);

// Methods to handle requests from client connections
//
static void * handler_thread(void *arg);
static pthread_t listen_tid;    // connection listener thread
static pthread_t split_tid;     // giga splitting thread

// Methods to setup server's socket connections
static void server_socket();
static void setup_listener(int listen_fd);
static void * main_select_loop(void * listen_fd);

static void sig_handler(const int sig);

// Methods to initialize GIGA+ specific directories and data structures
//
static void init_root_partition();
static void init_giga_mapping();

int main(int argc, char **argv)
{
    int ret = 0;

    if (argc == 2) {
        printf("usage: %s -p <port_number> -f <server_list_config>\n",argv[0]);
        exit(EXIT_FAILURE);
    }

    
    setbuf(stderr, NULL);           // set STDERR non-buffering 
    log_fp = stderr;        

    signal(SIGINT, sig_handler);    // handling SIGINT
    signal(SIGTERM, sig_handler);    // handling SIGINT
   
    // initialize logging
    char log_file[PATH_MAX] = {0};
    snprintf(log_file, sizeof(log_file), "%s.s", DEFAULT_LOG_FILE_PATH);
    if ((ret = logOpen(log_file, DEFAULT_LOG_LEVEL)) < 0) {
        fprintf(stdout, "***ERROR*** during opening log(%s) : [%s]\n",
                log_file, strerror(ret));
        return ret;
    }

    // init GIGA+ options.
    memset(&giga_options_t, 0, sizeof(struct giga_options));
    initGIGAsetting(GIGA_SERVER, NULL, CONFIG_FILE);    

    init_giga_mapping();    // init GIGA+ mapping structure.
    init_root_partition();  // init root partition on each server.

    server_socket();        // start server socket(s). 

    if (pthread_create(&split_tid, 0, split_thread, NULL) < 0) 
        LOG_ERR("ERR_pthread_create(): split_thread(%d)", split_tid);

    if (pthread_detach(split_tid) < 0)
        LOG_ERR("ERR_pthread_detach(): split_thread(%d)", split_tid);

    // FIXME: we sleep 15 seconds here to let the other servers startup.  This
    // mechanism needs to be replaced by an intelligent reconnection system.
    sleep(10);
    if (giga_options_t.num_servers >= 1) {
        rpcInit();          // initialize RPC connections
        rpcConnect();       // try connecting to all servers
    }

    LOG_ERR("### server[%d] up ...\n", giga_options_t.serverID);

    void *retval;

    pthread_join(listen_tid, &retval);

    exit((long)retval);
}

/******************************
 *  STATIC functions in use.  *
 ******************************/

static 
void sig_handler(const int sig)
{
    (void)sig;
    metadb_close(ldb_mds);
    LOG_ERR("SIGINT=%d handled.\n", sig);
    exit(1);
}

static 
void * handler_thread(void *arg)
{
    int fd = (int) (long) arg;
    SVCXPRT *svc = svcfd_create(fd, 0, 0);
    
    if(!svc_register(svc, GIGA_RPC_PROG, GIGA_RPC_VERSION, giga_rpc_prog_1, 0)) {
        fprintf(stdout, "ERROR: svc_register() error.\n");
        svc_destroy(svc);
        goto leave;
    }
    
    while (1) {
        fd_set readfds, exceptfds;
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);

        FD_ZERO(&exceptfds);
        FD_SET(fd, &exceptfds);

        if (select(fd + 1, &readfds, NULL, &exceptfds, NULL) < 0) {
            logMessage(LOG_DEBUG, __func__, 
                      "select()ing error on a socket. %s", strerror(errno));
            break;
        }

        if (FD_ISSET(fd, &exceptfds)) {
            logMessage(LOG_DEBUG, __func__, 
                       "Leave RPC select(), descripter registered an exception.\n");
            break;
        }

        if (FD_ISSET(fd, &readfds)){
            svc_getreqset(&readfds);
        }
    }

leave:
    close(fd);

    logMessage(LOG_DEBUG, __func__, "Connection closed.");

    return 0;
}

static void* 
main_select_loop(void * listen_fd_arg)
{
    int conn_fd;
    long listen_fd = (long) listen_fd_arg;
    
    while (1) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(listen_fd, &fds);

        int i = select(listen_fd+1, &fds, 0, 0, 0);
        if (i <= 0) {
            logMessage(LOG_DEBUG, __func__, "select()ing error.");
            continue;
        }

        struct sockaddr_in remote_addr;
        socklen_t len = sizeof(remote_addr);
        conn_fd = accept(listen_fd, (struct sockaddr *) &remote_addr, &len);
        if (conn_fd < 0) {
            logMessage(LOG_DEBUG, __func__, "err_accept()ing: %s", strerror(errno));
            continue;
        }
        logMessage(LOG_DEBUG, __func__, "connection accept()ed from {%s:%d}.", 
                   inet_ntoa(remote_addr.sin_addr),ntohs(remote_addr.sin_port));
        
        pthread_t tid;
        if (pthread_create(&tid, NULL, 
                           handler_thread, (void *)(unsigned long)conn_fd) < 0) {
            logMessage(LOG_DEBUG, __func__, "ERROR: during pthread_create().");
            close(conn_fd);
            continue;
        } 

        if (pthread_detach(tid) < 0){
            logMessage(LOG_DEBUG, __func__, "ERROR: unable to detach thread().");
        }
    }
    
    logMessage(LOG_DEBUG, __func__,  "WARNING: Exiting select(). WHY??? HOW???");

    return NULL;
}

static 
void setup_listener(int listen_fd)
{
    struct sockaddr_in serv_addr;
    bzero(&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(giga_options_t.port_num);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    //FIXME for local testing
    //serv_addr.sin_addr.s_addr = inet_addr("128.2.209.15");
   
    // bind() the socket to the appropriate ip:port combination
    if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(listen_fd);
        logMessage(LOG_FATAL, __func__, "ERROR: bind() failed.");
        exit(1);
    }
   
    // listen() for incoming connections
    if (listen(listen_fd, NUM_BACKLOG_CONN) < 0) {
        close(listen_fd);
        logMessage(LOG_FATAL, __func__, "ERROR: while listen()ing.");
        exit(1);
    }

    pthread_create(&listen_tid, NULL, main_select_loop, (void*)(long)listen_fd);
    
    logMessage(LOG_DEBUG, __func__, "Listener setup (port %d of %s). Success.",
               ntohs(serv_addr.sin_port), inet_ntoa(serv_addr.sin_addr));

    return;
}

/** Set socket options for server use.
 *
 * FIXME: Document these options
 */
static 
void set_sockopt_server(int sock_fd)
{
    int flags;
   
    if ((flags = fcntl(sock_fd, F_GETFL, 0)) < 0) {
        close(sock_fd);
        logMessage(LOG_FATAL, __func__, "ERROR: fcntl(F_GETFL) failed.");
        exit(1);
    }
    
    if (fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sock_fd);
        logMessage(LOG_FATAL, __func__, "ERROR: fcntl(F_SETFL) failed.");
        exit(1);
    }

    flags = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, 
                   (void *)&flags, sizeof(flags)) < 0) {
        logMessage(LOG_DEBUG, __func__, "ERROR: setsockopt(SO_REUSEADDR).");
    }
    
    if (setsockopt(sock_fd, SOL_SOCKET, SO_KEEPALIVE, 
                   (void *)&flags, sizeof(flags)) < 0) {
        logMessage(LOG_DEBUG, __func__, "ERROR: setsockopt(SO_KEEPALIVE).");
    }
    /* FIXME
    if (setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, 
                   (void *)&flags, sizeof(flags)) < 0) {
        err_ret("ERROR: setsockopt(SO_LINGER).");
    }
    */
    
    if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, 
                   (void *)&flags, sizeof(flags)) < 0) {
        logMessage(LOG_DEBUG, __func__, "ERROR: setsockopt(TCP_NODELAY).");
    }

    return;
}

static 
void server_socket()
{
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { 
        logMessage(LOG_FATAL, __func__, "ERROR: socket() creation failed.");
        exit(1);
    }

    set_sockopt_server(listen_fd);
    setup_listener(listen_fd);
}

static 
void init_root_partition()
{
    LOG_ERR("Check default directories for s[%d] ...", giga_options_t.serverID);

    if (mkdir(DEFAULT_SRV_BACKEND, DEFAULT_MODE) < 0) {
        if (errno != EEXIST) {
            LOG_ERR("ERR_mkdir(%s): for srv backend [%s]", 
                    DEFAULT_SRV_BACKEND, strerror(errno));
            exit(1);
        }
    }
    
    if (mkdir(DEFAULT_LEVELDB_DIR, DEFAULT_MODE) < 0) {
        if (errno != EEXIST) { 
            LOG_ERR("ERR_mkdir(%s): for leveldb [%s]", 
                    DEFAULT_LEVELDB_DIR, strerror(errno));
            exit(1);
        }
    }
    
    if (mkdir(DEFAULT_SPLIT_DIR, DEFAULT_MODE) < 0) {
        if (errno != EEXIST) { 
            LOG_ERR("ERR_mkdir(%s): for splits [%s]", 
                    DEFAULT_SPLIT_DIR, strerror(errno));
            exit(1);
        }
    }
    
    if (mkdir(giga_options_t.mountpoint, DEFAULT_MODE) < 0) {
        if (errno != EEXIST) {
            LOG_ERR("ERR_mkdir(%s): for mountpoint [%s]", 
                    giga_options_t.mountpoint, strerror(errno));
            exit(1);
        }
    }

    // initialize backends for each server
    //
    char ldb_name[PATH_MAX] = {0};
    int mdb_setup = 0;
    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(ldb_name, sizeof(ldb_name), "%s/0/", 
                     giga_options_t.mountpoint);
            if (local_mkdir(ldb_name, DEFAULT_MODE) < 0) 
                exit(1);
            break;
        case BACKEND_RPC_LEVELDB:
            snprintf(ldb_name, sizeof(ldb_name), 
                     "%s/l%d", DEFAULT_LEVELDB_DIR, giga_options_t.serverID);
            // FIXME: use new semantics of metadb_init.
            mdb_setup = metadb_init(&ldb_mds, ldb_name);
            if (mdb_setup == -1) {
                LOG_ERR("mdb_init(%s): init error", ldb_name);
                exit(1);
            }
            else if (mdb_setup == 1) {    
                LOG_MSG("creating new file system in %s", ldb_name);
                object_id = INIT_OBJECT_ID; 

#if 0
                int dir_id = ROOT_DIR_ID; //FIXME: dir_id for "root"
                struct giga_directory *dir = cache_fetch(&dir_id);
                if (dir == NULL) {
                    LOG_MSG("Dir (id=%d) not in cache!", dir_id);
                    exit(1);
                }
#endif
                // special case for ROOT
                //
                int dir_id = ROOT_DIR_ID;
                struct giga_directory *dir = cache_lookup(&dir_id);

                if (metadb_create_dir(ldb_mds, ROOT_DIR_ID, 0, "/",
                                      &dir->mapping) < 0) {
                    LOG_ERR("mdb_create(%s): error creating root", ldb_name);
                    exit(1);
                }

                if (metadb_create_dir(ldb_mds, ROOT_DIR_ID, -1, NULL,
                                      &dir->mapping) < 0) {
                    LOG_ERR("mdb_create(%s): error creating root mapping structure", ldb_name);
                    exit(1);
                }

                cache_release(dir);

            }
            else if (mdb_setup == 0) {
                LOG_MSG("reading old file system from %s", ldb_name);
                
#if 0                
                int dir_id = ROOT_DIR_ID; //FIXME: dir_id for "root"
                struct giga_directory *dir = cache_fetch(&dir_id);
                if (dir == NULL) {
                    LOG_MSG("Dir (id=%d) not in cache!", dir_id);
                    exit(1);
                }
#endif
                
                int dir_id = ROOT_DIR_ID;
                struct giga_directory *dir = cache_lookup(&dir_id);
                
                if (metadb_read_bitmap(ldb_mds, dir_id, -1, NULL, &dir->mapping) != 0) {
                    LOG_ERR("mdb_read(%s): error reading ROOT bitmap.", ldb_name);
                    exit(1);
                }
                cache_release(dir);
            }
            break;
        default:
            break;
    }

    return;
}

static
void init_giga_mapping()
{
    logMessage(LOG_TRACE, __func__, "init giga mapping");

    int dir_id = ROOT_DIR_ID; //FIXME: dir_id for "root"
    int srv_id = 0;
    
    cache_init();
    struct giga_directory *dir = new_cache_entry(&dir_id, srv_id);
    cache_insert(&dir_id, dir);
    cache_release(dir);

#if 0
    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(LOG_DEBUG, __func__, "Dir (id=%d) not in cache!", dir_id);
        exit(1);
    }
    giga_print_mapping(&dir->mapping);
#endif


    return;
}


