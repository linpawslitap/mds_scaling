#ifndef SERVER_H
#define SERVER_H

#include <semaphore.h>

#include "backends/operations.h"
#include "common/options.h"
#include "common/cache.h"

#define NUM_BACKLOG_CONN 128

struct MetaDB *ldb_mds;  //TODO: make this thread-safe.

struct giga_directory giga_dir_t;

struct giga_options giga_options_t;

void init_rpc_handlers();

void destroy_rpc_handlers();

#endif /* SERVER_H */
