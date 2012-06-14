#ifndef CONNECTION_H
#define CONNECTION_H
#include <rpc/clnt.h>
#include <rpc/pmap_clnt.h>
#include <rpc/rpc.h>

extern char *my_hostname;

CLIENT *getConnection(int serverid);
void rpcInit();
void rpcConnect(void);
void rpcDisconnect(void);

void getHostIPAddress(char *ip_addr, int ip_addr_len);

#endif
