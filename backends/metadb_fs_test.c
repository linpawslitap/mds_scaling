#include "operations.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>


#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 100000
#define FILE_FORMAT "%016lx"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__);}

int num_print_entries;

static
void print_meta_obj_key(metadb_key_t *mkey) {
    (void)mkey;
    /*
    printf("%ld, %ld, ", mkey->parent_id, mkey->partition_id);
    int i;
    for (i = 0; i < HASH_LEN; ++i)
        printf("%c", mkey->name_hash[i]);
    printf("\n");
    */
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
    char end_key[128];
    char* start_key = NULL;
    int num_ent = 0;
    int more_entries_flag;
    do {
        int ret = metadb_readdir(mdb, dir_id, &partition_id,
                                 start_key, buf, MAX_BUF_SIZE,
                                 &num_ent, end_key, &more_entries_flag);
        ASSERT(ret >= 0);
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
            ASSERT((statbuf.st_mode & S_IFREG) > 0);
            ASSERT(memcmp(objname, realpath, len) == 0);
            metadb_readdir_iter_next(iter);
        }
        start_key = end_key;
    } while (more_entries_flag != 0);
    free(buf);
}

struct MetaDB mdb;
struct MetaDB mdb2;
char* extname;
int ret;
int dir_id = 0;
int partition_id = 0;
int new_partition_id = 1;

char filename[MAX_FILENAME_LEN];
char backup[MAX_FILENAME_LEN];
struct stat statbuf;
int num_migrated_entries = 0;
size_t num_test_entries = MAX_NUM_ENTRIES;

void test_bitmap() {
    struct giga_mapping_t mybitmap;

    ASSERT(metadb_create_dir(mdb, 0, 0, "/", &mybitmap) == 0);

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
}

void test_create_and_check_files() {

    metadb_inode_t i = 0;
    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(backup, 0, sizeof(filename));
        snprintf(backup, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb, dir_id, partition_id,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
        if (giga_file_migration_status(backup, new_partition_id)) {
            ++num_migrated_entries;
        }
    }

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_lookup(mdb, dir_id, partition_id,
                             filename, &statbuf) == 0);
        ASSERT(statbuf.st_ino == 0);
    }
    printf("finished create and check files test.\n");
    printf("moved entries: %d \n", num_migrated_entries);
}

void test_read_write_files() {
    metadb_inode_t i = 0;
    int j = 0;
    char write_buf[30] = "1234567890";
    char read_buf[30];
    int write_buf_len = 18;
    int read_buf_len;
    int state;
    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        sprintf(write_buf+10, "%08d", (int) i);
        ASSERT(metadb_write_file(mdb, dir_id, partition_id,
                      filename, write_buf, write_buf_len, 0) == 0);
    }

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(read_buf, 0, sizeof(read_buf));
        sprintf(write_buf+10, "%08d", (int) i);
        ASSERT(metadb_get_file(mdb, dir_id, partition_id,
                      filename, &state, read_buf, &read_buf_len) == 0);
        ASSERT(write_buf_len == read_buf_len);
        if (write_buf_len == read_buf_len) {
            for (j = 0; j < read_buf_len; ++j)
                ASSERT(write_buf[j] == read_buf[j]);
        }
    }
    printf("finished read and write files test\n");
}

void test_read_write_links() {
    metadb_inode_t i = 0;
    int j = 0;
    char write_buf[30] = "/linkfile/";
    char read_buf[30];
    int prefix_len = strlen(write_buf);
    int write_buf_len = prefix_len+8;
    int read_buf_len;
    int state;
    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        sprintf(write_buf+prefix_len, "%08d", (int) i);
        write_buf[write_buf_len] = '\0';
        ASSERT(metadb_write_link(mdb, dir_id, partition_id,
                      filename, write_buf) == 0);
    }

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        memset(read_buf, 0, sizeof(read_buf));
        sprintf(write_buf+10, "%08d", (int) i);
        ASSERT(metadb_get_file(mdb, dir_id, partition_id,
                      filename, &state, read_buf, &read_buf_len) == 0);
        ASSERT(write_buf_len == read_buf_len);
        if (write_buf_len == read_buf_len) {
            for (j = 0; j < read_buf_len; ++j)
                ASSERT(write_buf[j] == read_buf[j]);
        }
    }

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);
        ASSERT(metadb_write_link(mdb, dir_id, partition_id,
                      filename, filename) == 0);
    }

    printf("finished read and write links test\n");
}

void test_migration() {
    metadb_inode_t i = 0;

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

    num_print_entries = 0;
    myreaddir(mdb2, dir_id, new_partition_id);
    printf("After myreaddir %d %d\n", num_migrated_entries, num_print_entries);
    ASSERT(num_migrated_entries == num_print_entries);

    int num_found_entries = 0;
    metadb_key_t testkey;

    for (i = 0; i < num_test_entries; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);

        init_meta_obj_key(&testkey, dir_id, new_partition_id, filename);
        print_meta_obj_key(&testkey);

        ret = metadb_lookup(mdb2, dir_id, new_partition_id, filename, &statbuf);
        if (ret == 0) {
            ASSERT(statbuf.st_ino == 0);
            ++num_found_entries;
        }
    }

    printf("%d %d\n", num_migrated_entries, num_found_entries);
    ASSERT(num_migrated_entries == num_found_entries);

    for (i = num_test_entries; i < num_test_entries*2; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb2, dir_id, partition_id,
                             filename, filename) == 0);
        metadb_lookup(mdb2, dir_id, partition_id, filename, &statbuf);
    }

    for (i = num_test_entries; i < num_test_entries*4; ++i) {
        memset(filename, 0, sizeof(filename));
        snprintf(filename, MAX_FILENAME_LEN, FILE_FORMAT, i);

        ASSERT(metadb_create(mdb, dir_id, partition_id,
                             filename, filename) == 0);
        metadb_lookup(mdb, dir_id, partition_id, filename, &statbuf);
    }
}

void run_test(int nargs, char* args[]) {
    if (nargs < 4) {
        return;
    }

    char* dbname = args[1];
    char* dbname2 = args[2];
    extname = args[3];
    printf("metadb_init return value %d\n", metadb_init(&mdb, dbname));
    printf("metadb_init return value %d\n", metadb_init(&mdb2, dbname2));

    test_bitmap();
    test_create_and_check_files();
    test_read_write_files();
    test_read_write_links();
    test_migration();

    metadb_close(mdb);
    metadb_close(mdb2);
}

int main(int nargs, char* args[]) {
    run_test(nargs, args);
    return 0;
}
