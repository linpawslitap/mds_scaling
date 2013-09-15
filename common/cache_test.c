#include "cache.h"
#include "options.h"
#include "debugging.h"
#define ASSERT(x) \
if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

struct giga_options giga_options_t;

int Lookup(int key) {
    struct giga_directory* entry = cache_lookup(&key);
    int r = (entry == NULL) ? -1 : entry->split_flag;
    if (entry != NULL) {
        cache_release(entry);
    }
    printf("ret = %d\n", r);
    return r;
}

void Insert(int key, int val) {
    struct giga_directory* entry =
        (struct giga_directory*) malloc(sizeof(struct giga_directory));
    entry->handle = key;
    entry->split_flag = val;
    cache_insert(&key, entry);
}

void Erase(int key) {
    cache_evict(&key);
}

void TestUTHASH() {
    struct giga_directory* head = NULL;
    struct giga_directory* entry =
        (struct giga_directory*) malloc(sizeof(struct giga_directory));
    entry->handle = 100;
    entry->split_flag = 101;
    int key = 100;
    HASH_ADD_INT(head, handle, entry);
    struct giga_directory* ret;
    HASH_FIND_INT(head, &key, ret);
    if (ret != NULL) {
        printf("FIND: %d %d\n", key, ret->split_flag);
    }
    key = 200;
    HASH_FIND_INT(head, &key, ret);
    if (ret != NULL) {
        printf("FIND: %d %d\n", key, ret->split_flag);
    }

    key = 300;
    HASH_FIND_INT(head, &key, ret);
    if (ret != NULL) {
        printf("FIND: %d %d\n", key, ret->split_flag);
    }

    entry =
        (struct giga_directory*) malloc(sizeof(struct giga_directory));
    entry->handle = 200;
    entry->split_flag = 201;
    HASH_ADD_INT(head, handle, entry);

    key = 100;
    HASH_FIND_INT(head, &key, ret);
    if (ret != NULL) {
        printf("FIND: %d %d\n", key, ret->split_flag);
    }

    key = 200;
    HASH_FIND_INT(head, &key, ret);
    if (ret != NULL) {
        printf("FIND: %d %d\n", key, ret->split_flag);
    }
}

void TestHitAndMiss() {
    ASSERT(Lookup(100) == -1);

    Insert(100, 101);
    ASSERT(Lookup(100) == 101);
    ASSERT(Lookup(200) == -1);
    ASSERT(Lookup(300) == -1);
    ASSERT(Lookup(100) == 101);

    Insert(200, 201);
    ASSERT(Lookup(100) == 101);
    ASSERT(Lookup(200) == 201);
    ASSERT(Lookup(300) == -1);

    Insert(300, 301);
    ASSERT(Lookup(100) == 101);
    ASSERT(Lookup(200) == 201);
    ASSERT(Lookup(300) == 301);
}

//TODO: What is the semantics for duplicated entry?
void TestErase() {
    Erase(200);
    ASSERT(Lookup(200) == -1);

    Insert(100, 101);
    Insert(200, 201);
    Erase(100);
    ASSERT(Lookup(100) == -1);
    ASSERT(Lookup(200) == 201);

    Erase(100);
    ASSERT(Lookup(100) == -1);
    ASSERT(Lookup(200) == 201);
}

void TestEntriesArePinned() {
    Insert(100, 101);
    int key = 100;
    struct giga_directory* h1 = cache_lookup(&key);
    ASSERT(h1->split_flag == 101);

    Insert(100, 102);
    struct giga_directory* h2 = cache_lookup(&key);
    ASSERT(h2->split_flag == 102);
    ASSERT(h1->split_flag == 101);

    cache_release(h1);
    ASSERT(h2->split_flag == 102);

    Erase(100);
    ASSERT(Lookup(100) == -1);
    ASSERT(h2->split_flag == 102);
    cache_release(h2);
}

void TestEvictionPolicy() {
    Insert(100, 101);
    Insert(200, 201);

    int i = 0;
    for (i = 0; i < DEFAULT_DIR_CACHE_SIZE + 100; i++) {
        Insert(i+1000, i+2000);
        ASSERT(Lookup(i+1000) == (2000+i));
        ASSERT(Lookup(100) == 101);
    }

    ASSERT(Lookup(100) == 101);
    ASSERT(Lookup(200) == -1);
}

void TestFUSECache() {
    char key[] = "20";
    time_t tmp;
    int tmp1;
    ASSERT(fuse_cache_lookup(0, key, &tmp, &tmp1) == -1);
    fuse_cache_insert(0, key, 1000, tmp1);
    ASSERT(fuse_cache_lookup(0, key, &tmp, &tmp1) == 1000);
}

void TestDirCache(const char* filename) {
    logOpen("/tmp/cache_test.log", LOG_FATAL);
    FILE* file = fopen(filename, "r");
    char op[100];
    memset(op, 0, sizeof(op));
    int inode;
    struct giga_directory* entry;
    while ((fscanf(file, "%s %d", op, &inode))>0) {
        printf("%s %d\n", op, inode);
        if (strcmp(op, "{cache_insert}") == 0) {
          entry = new_cache_entry(&inode, 0);
          cache_insert(&inode, entry);
        }
        if (strcmp(op, "{cache_lookup}") == 0) {
          cache_lookup(&inode);
        }
        if (strcmp(op, "{cache_release}") == 0) {
          entry = cache_lookup(&inode);
          cache_release(entry);
          cache_release(entry);
        }

    }
    fclose(file);
    logClose();
}

int main(int nargs, char** args) {
    cache_init();
    /*
    TestUTHASH();
    TestHitAndMiss();
    TestErase();
    TestEntriesArePinned();
    TestEvictionPolicy();
    TestFUSECache();
    */
    if (nargs > 1)
      TestDirCache(args[1]);
    cache_destory();
    return 0;
}
