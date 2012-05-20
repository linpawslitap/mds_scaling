#include "operations.h"
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

#define MAX_FILENAME_LEN 1024

void print_entries(void* buf, metadb_key_t* iter_key, metadb_obj_t* iter_obj) {
    printf("Entry %ld %d %s %ld\n",
           iter_key->parent_id, iter_key->partition_id, iter_key->name_hash,
           iter_obj->statbuf.st_ino);
}

void run_test() {
    char dbname[] = "/tmp/testdb";
    char dbname2[] = "/tmp/testdb2";
    struct MetaDB mdb;
    struct MetaDB mdb2;

    metadb_init(&mdb, dbname);
    metadb_init(&mdb2, dbname2);

    int dir_id = 0;
    int partition_id = 0;
    int new_partition_id = 1;

    char filename[MAX_FILENAME_LEN];
    char backup[MAX_FILENAME_LEN];
    struct stat statbuf;
    int num_migrated_entries = 0;
    metadb_inode_t i = 0;

    snprintf(filename, MAX_FILENAME_LEN, "%08x", 10000);
    metadb_test_put_and_get(mdb, dir_id, partition_id, filename);
    metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);

    for (i = 0; i < 50; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, "%16lx", i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, "%16lx", i);

        assert(metadb_create(mdb, dir_id, partition_id, OBJ_DIR, i,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        assert(statbuf.st_ino == i);
        if (giga_file_migration_status(backup, new_partition_id)) {
            ++num_migrated_entries;
            printf("%ld\n", i);
        }
    }

    printf("moved entries: %d \n", num_migrated_entries);

    char extname[] = "/tmp/extract";
    int ret =
        metadb_extract(mdb, dir_id, partition_id, new_partition_id, extname);

    assert(num_migrated_entries == ret);
    /*
    assert(metadb_bulkinsert(mdb2, extname) == 0);

    assert(metadb_readdir(mdb2, dir_id, new_partition_id, NULL, print_entries) == 0);

    int num_found_entries = 0;
    for (i = 0; i < 50; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, "%16lx", i);

        ret = metadb_lookup(mdb2, dir_id, new_partition_id, filename, &statbuf);
        if (ret == 0) {
            assert(statbuf.st_ino == i);
            ++num_found_entries;
        }
    }
    assert(num_migrated_entries == num_found_entries);
    */
    metadb_close(mdb);
    metadb_close(mdb2);
}

int main() {
    run_test();
    return 0;
}
