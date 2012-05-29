
#include "server.h"

#include "common/rpc_giga.h"
#include "common/connection.h"
#include "common/debugging.h"
#include "common/defaults.h"
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


//struct server_settings srv_settings;
//struct giga_options giga_options;

static pthread_t listen_tid;

extern SVCXPRT *svcfd_create (int __sock, u_int __sendsize, u_int __recvsize);
// FIXME: rpcgen should put this in giga_rpc.h, but it doesn't. Why?
extern void giga_rpc_prog_1(struct svc_req *rqstp, register SVCXPRT *transp);

// Methods to handle requests from client connections
static void * handler_thread(void *arg);

// Methods to setup server's socket connections
static void server_socket();
static void setup_listener(int listen_fd);
static void * main_select_loop(void * listen_fd);

static 
void sig_handler(const int sig)
{
    (void)sig;
    printf("SIGINT handled.\n");
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
    logMessage(LOG_DEBUG, __func__, "root partition at.");
    //TODO
    //
    
    // check if server's backend directory is created.
    //
    struct stat stbuf;
    if (lstat(giga_options_t.mountpoint, &stbuf) < 0) {
        if (errno == EEXIST) {
            logMessage(LOG_DEBUG, __func__, 
                       "mountpoint(%s) exists.", giga_options_t.mountpoint);
        }
        else if (errno == ENOENT) {
            if (mkdir(giga_options_t.mountpoint, DEFAULT_MODE) < 0) {
                logMessage(LOG_FATAL, __func__, 
                           "root creation error: %s", strerror(errno));
                exit(1);
            }
        }
        else {
            logMessage(LOG_FATAL, __func__, 
                       "init root partition error: %s", strerror(errno));
        }
    }

    // initialize backend based on the type of backend.
    //
    char ldb_name[MAX_LEN] = {0};
    switch (giga_options_t.backend_type) {
        case BACKEND_RPC_LOCALFS:
            snprintf(ldb_name, sizeof(ldb_name), "%s/0/", 
                     giga_options_t.mountpoint);
            if (local_mkdir(ldb_name, CREATE_MODE) < 0) {
                logMessage(LOG_FATAL, __func__, "root partition creation error.");
                exit(1);
            }
        case BACKEND_RPC_LEVELDB:
            //TODO: leveldb setup and initialization
            snprintf(ldb_name, sizeof(ldb_name), 
                     "%s/%d-%s", 
                     DEFAULT_LEVELDB_DIR, giga_options_t.serverID,
                     DEFAULT_LEVELDB_PREFIX);
            metadb_init(&ldb_mds, ldb_name);
            object_id = 0;
            if (metadb_create(ldb_mds, 
                              ROOT_DIR_ID, 0,
                              OBJ_DIR, 
                              object_id, "/", giga_options_t.mountpoint) < 0) {
                logMessage(LOG_FATAL, __func__, "root entry creation error.");
                exit(1);
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

    int dir_id = 0; //FIXME: dir_id for "root"

    struct giga_directory *dir = cache_fetch(&dir_id);
    if (dir == NULL) {
        logMessage(LOG_DEBUG, __func__, "Dir (id=%d) not in cache!", dir_id);
        exit(1);
    }

    giga_print_mapping(&dir->mapping);

    return;
}


int main(int argc, char **argv)
{
    if (argc == 2) {
        printf("usage: %s -p <port_number> -f <server_list_config>\n",argv[0]);
        exit(EXIT_FAILURE);
    }

    // set STDERR non-buffering 
    setbuf(stderr, NULL);
    log_fp = stderr;

    /*
    char * fs_spec = NULL;
    char c;
    while (-1 != (c = getopt(argc, argv,
                             "p:"           // port number
                             "f:"           // mount point for server "root"
           ))) {
        switch(c) {
            case 'p':
                srv_settings.port_num = atoi(optarg);
                break;
            case 'f':
                fs_spec = strdup(optarg);
                break;
            default:
                fprintf(stdout, "Illegal parameter: %c\n", c);
                exit(1);
                break;
        }

    }
    */

    signal(SIGINT, sig_handler);    // handling SIGINT
    
    logOpen(DEFAULT_LOG_FILE_LOCATIONs, DEFAULT_LOG_LEVEL); // init logging.
    initGIGAsetting(GIGA_SERVER, DEFAULT_CONF_FILE);    // init GIGA+ options.

    if (giga_options_t.serverID == -1){
        logMessage(LOG_FATAL, __func__, 
                   "ERROR: server hostname does not match any server in list.");
        exit(1);
    }
    
    // FIXME: do we need to check for serverID (if 0 or something else)?

    init_root_partition();  // init root partition on each server.
    init_giga_mapping();    // init GIGA+ mapping structure.

    server_socket();        // start server socket(s). 

    // FIXME: we sleep 15 seconds here to let the other servers startup.  This
    // mechanism needs to be replaced by an intelligent reconnection system.
    //sleep(15);
    if (giga_options_t.num_servers > 1) {
        if (rpcConnect()) {
            logMessage(LOG_FATAL, __func__, "Error making RPC conns: exit .");
            exit(1);
        }
    }

    logMessage(LOG_DEBUG, __func__, "Server %d up!", giga_options_t.serverID);

    void *retval;

    pthread_join(listen_tid, &retval);

    exit((long)retval);
}
