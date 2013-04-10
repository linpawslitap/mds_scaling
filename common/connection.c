
#include "options.h"
#include "rpc_giga.h"
#include "connection.h"
#include "debugging.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <rpc/rpc.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static CLIENT **rpc_clnts;

static int rpc_host_connect(CLIENT **rpc_client, const char *host);

char *my_hostname = NULL;

CLIENT *getConnection(int srv_id)
{
    assert(srv_id >= 0 && srv_id < giga_options_t.num_servers);

    if (rpc_clnts[srv_id] == NULL) {
        LOG_ERR("Trying connection for server[%d] ...", srv_id);
        if (rpc_host_connect(&rpc_clnts[srv_id],
                             giga_options_t.serverlist[srv_id]) < 0) {
            LOG_ERR("ERROR connecting to server [%d]", srv_id);
            return NULL;
        }
        struct timeval to;
        to.tv_sec = 60;
        to.tv_usec = 0;
        clnt_control(rpc_clnts[srv_id], CLSET_TIMEOUT, (char*)&to);
    }

    return rpc_clnts[srv_id];
}

void rpcInit()
{
    int i = 0;

    rpc_clnts = malloc(sizeof(CLIENT*) * giga_options_t.num_servers);
    if (rpc_clnts == NULL) {
        LOG_ERR("ERR_malloc(): %d rpc conns", giga_options_t.num_servers);
        exit(1);
    }

    for (i = 0; i < giga_options_t.num_servers; i++)
        rpc_clnts[i] = NULL;
}

void rpcConnect(void)
{
    int i;

    for (i = 0; i < giga_options_t.num_servers; i++) {
        if (rpc_host_connect(&rpc_clnts[i], giga_options_t.serverlist[i]) < 0)
            LOG_ERR("CONN_ERROR: -> s[%d]", i);
        else {
            struct timeval to;
            to.tv_sec = 60;
            to.tv_usec = 0;
            clnt_control(rpc_clnts[i], CLSET_TIMEOUT, (char*)&to);
            LOG_ERR("CONN_SETUP: -> s[%d]", i);
        }
    }
}

void rpcDisconnect(void)
{
    int i;
    for (i = 0; i < giga_options_t.num_servers; i++)
        clnt_destroy (rpc_clnts[i]);
}

static
int rpc_host_connect(CLIENT **rpc_client, const char *host)
{
    int sock = RPC_ANYSOCK;

    struct hostent *he;
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DEFAULT_PORT);

    if ((he = gethostbyname(host)) == NULL) {
        LOG_ERR("ERR_gethostbyname(%s): %s", host, hstrerror(h_errno));
        return -1;
    }

    memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

    *rpc_client = clnttcp_create(&addr,
                                 GIGA_RPC_PROG, GIGA_RPC_VERSION,
                                 &sock, 0, 0);
    if (*rpc_client == NULL) {
        LOG_ERR("ERR_rpc_conn: %s", clnt_spcreateerror((char*)host));
        return -1;
    }

    return 0;
}

void getHostIPAddress(char *ip_addr, int ip_addr_len)
{
    char hostname[HOST_NAME_MAX] = {0};
    hostname[HOST_NAME_MAX-1] = '\0';
    gethostname(hostname, HOST_NAME_MAX-1);

    //fprintf(stdout, "[%s] finding IP addr of host=%s\n", __func__, hostname);

    /*
    //FIXME: for local desktop testing on SVP's machine.
    if (strcmp(hostname, "gs5017") == 0) {
        snprintf(ip_addr, ip_addr_len, "128.2.209.15");
        fprintf(stdout, "[%s] host=%s has IP=%s\n", __func__, hostname, ip_addr);
        return;
    }
    */

    int gai_result;
    struct addrinfo hints, *info;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;
    hints.ai_protocol = 0;

    if ((gai_result = getaddrinfo(hostname, NULL, &hints, &info)) != 0) {
        fprintf(stdout, "[%s] getaddrinfo(%s) failed. [%s]\n",
                __func__, hostname, gai_strerror(gai_result));
        exit(1);
    }

    //fprintf(stdout, "[%s] finding non-loopback IP addr ... \n", __func__);

    void *ptr = NULL;
    struct addrinfo *p;
    for (p = info; p != NULL; p = p->ai_next) {
        inet_ntop (p->ai_family, p->ai_addr->sa_data, ip_addr, ip_addr_len);
        switch (p->ai_family) {
            case AF_INET:
                ptr = &((struct sockaddr_in *) p->ai_addr)->sin_addr;
                break;
            case AF_INET6:
                ptr = &((struct sockaddr_in6 *) p->ai_addr)->sin6_addr;
                break;
        }

        inet_ntop (p->ai_family, ptr, ip_addr, ip_addr_len);
        //fprintf(stdout, "\t IPv%d address: %s (%s)\n",
        //        p->ai_family == PF_INET6 ? 6 : 4, ip_addr, p->ai_canonname);
    }

    //fprintf(stdout, "[%s] host=%s has IP=%s\n", __func__, hostname, ip_addr);

    return;
}

