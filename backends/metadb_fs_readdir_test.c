#include "operations.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>


#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 100000
#define FILE_FORMAT "%016x"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

int num_print_entries;

static
void myreaddir(struct MetaDB mdb,
               metadb_inode_t dir_id,
               int partition_id) {
    char* buf = (char *) malloc(MAX_BUF_SIZE);
    char end_key[128];
    char* start_key = NULL;
    int num_ent = 0;
    int more_entries_flag = 0;
    do {
        int ret = metadb_readdir(mdb, dir_id, partition_id,
                                 start_key, buf, MAX_BUF_SIZE,
                                 &num_ent, end_key, &more_entries_flag);
        ASSERT(ret >= 0);
        printf("%d\n", num_ent);
        metadb_readdir_iterator_t* iter =
            metadb_create_readdir_iterator(buf, MAX_BUF_SIZE, num_ent);
        metadb_readdir_iter_begin(iter);
        while (metadb_readdir_iter_valid(iter)) {
            ++num_print_entries;
            size_t len;
            const char* objname =
                metadb_readdir_iter_get_objname(iter, &len);
            const char* realpath =
                metadb_readdir_iter_get_realpath(iter, &len);
            struct stat statbuf;
                metadb_readdir_iter_get_stat(iter, &statbuf);
            if (num_print_entries > 1) {
                ASSERT((statbuf.st_mode & S_IFREG) > 0);
            }
            ASSERT(memcmp(objname, realpath, len) == 0);
            metadb_readdir_iter_next(iter);
        }
        metadb_destroy_readdir_iterator(iter);
        start_key = end_key;
    } while (more_entries_flag != 0);
    free(buf);
}

void run_test(int nargs, char* args[]) {
    if (nargs < 2) {
        return;
    }

    char* dbname = args[1];

    struct MetaDB mdb;

    metadb_init(&mdb, dbname);

    int dir_id = 0;
    int partition_id = -1;

    char filename[MAX_FILENAME_LEN];
    int num_test_entries = 50000;

    metadb_val_dir_t dir_map;
    metadb_val_dir_t dir_map_ret;
    dir_map.zeroth_server = 111;
    dir_map.server_count = 222;
    dir_map.curr_radix = 333;

    ASSERT(metadb_create_dir(mdb, 0, 0, 0, NULL, &dir_map) == 0);
    ASSERT(metadb_read_bitmap(mdb, 0, 0, NULL, &dir_map_ret) == 0);
    ASSERT(dir_map_ret.zeroth_server == 111);
    ASSERT(dir_map_ret.server_count == 222);
    ASSERT(dir_map_ret.curr_radix == 333);

    dir_map.zeroth_server = 333;
    dir_map.server_count = 444;
    dir_map.curr_radix = 555;
    ASSERT(metadb_write_bitmap(mdb, 0, 0, NULL, &dir_map) == 0);
    ASSERT(metadb_read_bitmap(mdb, 0, 0, NULL, &dir_map_ret) == 0);
    ASSERT(dir_map_ret.zeroth_server == 333);
    ASSERT(dir_map_ret.server_count == 444);
    ASSERT(dir_map_ret.curr_radix == 555);

    int i;
    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, "localhost_3234_f%d", i);

        ASSERT(metadb_create(mdb, dir_id, partition_id, OBJ_FILE, i,
                             filename, filename) == 0);
    }

    num_print_entries = 0;
    myreaddir(mdb, dir_id, partition_id);
    num_test_entries += 1;
    ASSERT(num_print_entries == num_test_entries);

    printf("inserted entries: %d %d\n", num_test_entries, num_print_entries);
    metadb_close(mdb);
}

int main(int nargs, char* args[]) {
    run_test(nargs, args);
    return 0;
}
