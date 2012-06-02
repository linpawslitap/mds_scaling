/*************************************************************************
* Author: Kai Ren
* Created Time: 2012-05-30 15:03:58
* File Name: ./metadb_insert_test.c
* Description:
 ************************************************************************/
#include "operations.h"
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 10000
#define FILE_FORMAT "fd%d"

void run_test(int nargs, char* args[]) {
    if (nargs < 2) {
        return;
    }

    char* dbname = args[1];
    int pid;
    sscanf(args[3], "%d", &pid);

    struct MetaDB mdb;
    metadb_init(&mdb, dbname);

    char filename[MAX_FILENAME_LEN];
    metadb_inode_t dir_id = 0;
    int partition_id = 0;
    int new_partition_id = 0;
    size_t i;
    size_t num_test_entries = 10000;
    size_t num_migrated_entries = 0;
    struct stat statbuf;
    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, (int) i);

        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        assert(metadb_create(mdb, dir_id, partition_id, OBJ_DIR, i,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        if (giga_file_migration_status(filename, new_partition_id)) {
            ++num_migrated_entries;
        }
    }

    printf("%ld\n", num_migrated_entries);

    metadb_close(mdb);
}

int main(int nargs, char* args[]) {
    run_test(nargs, args);
    return 0;
}
