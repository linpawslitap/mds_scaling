#ifndef GIGA_RPC_HELPER_H
#define GIGA_RPC_HELPER_H   

#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "giga_index.h"

bool_t xdr_mode_t(XDR *xdrs, mode_t *mode);
bool_t xdr_giga_mapping_t(XDR *xdrs, struct giga_mapping_t *objp);

bool_t xdr_stat();
bool_t xdr_statvfs();

//extern bool_t xdr_stat();
//extern bool_t xdr_statvfs();

#endif /* GIGA_RPC_HELPER_H */
