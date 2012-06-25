#include "operations.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>


#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 1000
#define FILE_FORMAT "%016lx"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); }

int num_print_entries;

static
void print_meta_obj_key(metadb_key_t *mkey) {
    printf("%ld, %ld, ", mkey->parent_id, mkey->partition_id);
    int i;
    for (i = 0; i < HASH_LEN; ++i)
        printf("%c", mkey->name_hash[i]);
    printf("\n");
}

static
void init_meta_obj_key(metadb_key_t *mkey,
                       metadb_inode_t dir_id,
                       int partition_id, const char* path)
{
    mkey->parent_id = dir_id;
    mkey->partition_id = partition_id;
    memset(mkey->name_hash, 0, sizeof(mkey->name_hash));
    giga_hash_name(path, mkey->name_hash);
}
static
void myreaddir(struct MetaDB mdb,
               metadb_inode_t dir_id,
               int partition_id) {
    char* buf = (char *) malloc(MAX_BUF_SIZE);
    char* end_key = NULL;
    char* start_key = NULL;
    size_t num_ent = 0;
    do {
        int ret = metadb_readdir(mdb, dir_id, partition_id,
                                 start_key, buf, MAX_BUF_SIZE,
                                 &num_ent, &end_key);
        ASSERT(ret >= 0);
        if (start_key != NULL) {
            free(start_key);
        }
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
            ASSERT((statbuf.st_mode & S_IFDIR) > 0);
            ASSERT(memcmp(objname, realpath, len) == 0);
            metadb_readdir_iter_next(iter);
        }
        start_key = end_key;
    } while (end_key != NULL);
    free(buf);
}

void run_test(int nargs, char* args[]) {
    if (nargs < 4) {
        return;
    }

    char* dbname = args[1];
    char* dbname2 = args[2];
    char* extname = args[3];

    struct MetaDB mdb;
    struct MetaDB mdb2;

    metadb_init(&mdb, dbname);
    metadb_init(&mdb2, dbname2);

    int ret;
    int dir_id = 0;
    int partition_id = 0;
    int new_partition_id = 1;

    char filename[MAX_FILENAME_LEN];
    char backup[MAX_FILENAME_LEN];
    struct stat statbuf;
    int num_migrated_entries = 0;
    metadb_inode_t i = 0;

    snprintf(filename, MAX_FILENAME_LEN, "%08x", 10000);
    struct giga_mapping_t mybitmap;

    ASSERT(metadb_create(mdb, 0, 0, OBJ_DIR, 0, "/", "/") == 0);

    ret = metadb_read_bitmap(mdb, 0, 0, "/", &mybitmap);
    ASSERT(ret == 0);

    mybitmap.curr_radix = 0;
    mybitmap.zeroth_server = 10;
    mybitmap.server_count = 21;

    ret = metadb_write_bitmap(mdb, 0, 0, "/", &mybitmap);
    ASSERT(ret == 0);

    memset(&mybitmap, 0, sizeof(mybitmap));
    ret = metadb_read_bitmap(mdb, 0, 0, "/", &mybitmap);
    ASSERT(ret == 0);
    ASSERT(mybitmap.curr_radix == 0);
    ASSERT(mybitmap.zeroth_server == 10);
    ASSERT(mybitmap.server_count == 21);

    metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);

    size_t num_test_entries = MAX_NUM_ENTRIES;

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb, dir_id, partition_id, OBJ_DIR, i,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        ASSERT(statbuf.st_ino == i);
        if (giga_file_migration_status(backup, new_partition_id)) {
            ++num_migrated_entries;
        }
    }

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf) == 0);
        ASSERT(statbuf.st_ino == i);
    }

    printf("moved entries: %d \n", num_migrated_entries);

    num_print_entries = 0;
    myreaddir(mdb, dir_id, partition_id);

    uint64_t min_seq, max_seq;
    ret = metadb_extract_do(mdb, dir_id, partition_id,
                                new_partition_id, extname,
                                &min_seq, &max_seq);
    printf("extract entries: %d\n", ret);
    ASSERT(num_migrated_entries == ret);

    printf("extname: %s\n", extname);
    ASSERT(metadb_bulkinsert(mdb2, extname, min_seq, max_seq) == 0);

    ret = metadb_extract_clean(mdb);
    ASSERT(ret == 0);

    num_print_entries = 0;
    myreaddir(mdb2, dir_id, new_partition_id);

    printf("%d, %d, %ld\n", num_migrated_entries, num_print_entries, max_seq);
    ASSERT(num_migrated_entries == num_print_entries);

    printf("\n\n");

    num_print_entries = 0;
    myreaddir(mdb2, dir_id, new_partition_id);
    printf("After myreaddir %d %d\n", num_migrated_entries, num_print_entries);
    ASSERT(num_migrated_entries == num_print_entries);

    printf("\n\n");

    int num_found_entries = 0;
    metadb_key_t testkey;

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);

        init_meta_obj_key(&testkey, dir_id, new_partition_id, filename);
        print_meta_obj_key(&testkey);

        ret = metadb_lookup(mdb2, dir_id, new_partition_id, filename, &statbuf);
        if (ret == 0) {
            ASSERT(statbuf.st_ino == i);
            ++num_found_entries;
        }
    }

    printf("%d %d\n", num_migrated_entries, num_found_entries);
    ASSERT(num_migrated_entries == num_found_entries);

    for (i = num_test_entries; i < num_test_entries*2; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb2, dir_id, partition_id, OBJ_DIR, i,
                             filename, filename) == 0);
        metadb_lookup(mdb2, dir_id, partition_id, filename, &statbuf);
        ASSERT(statbuf.st_ino == i);
    }

    for (i = num_test_entries; i < num_test_entries*4; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb, dir_id, partition_id, OBJ_DIR, i,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        ASSERT(statbuf.st_ino == i);
    }

    metadb_close(mdb);
    metadb_close(mdb2);
}

int main(int nargs, char* args[]) {
    run_test(nargs, args);
    return 0;
}
