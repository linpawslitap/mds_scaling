#ifndef SERVER_H
#define SERVER_H   

#include <semaphore.h>

#include "backends/operations.h"
#include "common/options.h"
#include "common/cache.h"

#define NUM_BACKLOG_CONN 128

#define SPLIT_THRESHOLD 4000

static struct MetaDB ldb_mds;

static int object_id;

struct giga_directory giga_dir_t;

struct giga_options giga_options_t;

#endif /* SERVER_H */
