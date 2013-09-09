#include "operations.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>


#define MAX_FILENAME_LEN 1024
#define MAX_NUM_ENTRIES 100000
#define FILE_FORMAT "%016lx"
#define MAX_BUF_SIZE (2 << 20)
#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); }
int num_print_entries;

static
void myreaddir(struct MetaDB *mdb,
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
            statbuf = *metadb_readdir_iter_get_stat(iter);
            (void) statbuf;
            printf("%s %s\n", objname, realpath);
            metadb_readdir_iter_next(iter);
        }
        start_key = end_key;
    } while (more_entries_flag != 0);
    free(buf);
}

static
void listmdb(struct MetaDB* mdb) {
    leveldb_iterator_t* iter =
      leveldb_create_iterator(mdb->db, mdb->scan_options);

    leveldb_iter_seek_to_first(iter);
    while (leveldb_iter_valid(iter)) {
        size_t klen;
        const char* kstr = leveldb_iter_key(iter, &klen);
        const metadb_key_t* key = (const metadb_key_t *) kstr;
        printf("%ld %ld %ld %s\n",
               key->parent_id >> 20, key->parent_id & ((1<<20)-1),
               key->partition_id, key->name_hash);
        size_t vlen;
        const char* vstr = leveldb_iter_value(iter, &vlen);
        (void) vstr;
        leveldb_iter_next(iter);
    }

    leveldb_iter_destroy(iter);
}

void run_test(int nargs, char* args[]) {
    if (nargs < 1) {
        return;
    }

    (void) myreaddir;

    char* dbname = args[1];

    struct MetaDB mdb;

    printf("metadb_init return value %d\n",
           metadb_init(&mdb, dbname, NULL, 0, 0));

    listmdb(&mdb);

    /*
    int dir_id = 1;
    int partition_id = 1;
    num_print_entries = 0;
    myreaddir(mdb, dir_id, partition_id);
    printf("After myreaddir %d \n", num_print_entries);
    struct stat statbuf;
    printf("%d\n", metadb_lookup(mdb, dir_id, partition_id,
           "h0.giga.ycsb.marmot.pdl.cmu.local_p5369_f34", &statbuf));
    */

    metadb_close(&mdb);
}

int main(int nargs, char* args[]) {
    run_test(nargs, args);
    return 0;
}
