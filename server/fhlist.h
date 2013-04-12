#ifndef FHLIST_H
#define FHLIST_H

#include "common/rpc_giga.h"
#include "uthash.h"

// initialize the file handle list
int fhlist_init();

void fhlist_open_file(giga_dir_id dir_id, giga_pathname path);

void fhlist_close_file(giga_dir_id dir_id, giga_pathname path);

int fhlist_get_count(giga_dir_id dir_id, giga_pathname path);

#endif
