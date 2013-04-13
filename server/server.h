#ifndef SERVER_H
#define SERVER_H

#include <semaphore.h>

#include "backends/operations.h"
#include "common/options.h"
#include "common/cache.h"
#include "server/fhlist.h"

#define NUM_BACKLOG_CONN 128

static int object_id;

struct MetaDB ldb_mds;  //TODO: make this thread-safe.

struct giga_directory giga_dir_t;

struct giga_options giga_options_t;

#endif /* SERVER_H */
