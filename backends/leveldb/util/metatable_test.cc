/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-13 13:43:40
* File Name: ./table_test.cc
* Description:
 ************************************************************************/

#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include <stdio.h>
#include <vector>

using namespace leveldb;

typedef struct MetaDB_key {
    uint64_t parent_id;
    long int partition_id;
    char name_hash[128];
} metadb_key_t;

void FindKey(std::string filename) {
    Options options;
    RandomAccessFile* file;
    uint64_t file_size;
    Status s;
    s = Env::Default()->NewRandomAccessFile(filename, &file);
    if (!s.ok()) {
      printf("Cannot open file\n");
      return;
    }
    s = Env::Default()->GetFileSize(filename, &file_size);
    if (!s.ok()) {
      printf("Cannot get file size\n");
      return;
    }
    printf("%s\n", filename.c_str());
    Table* table;
    Table::Open(options, file, file_size, &table);
    Iterator* iter = table->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      const metadb_key_t* tkey = (const metadb_key_t*) iter->key().data();
      if (tkey->parent_id == 28678) {
         printf("%ld %ld\n",
                tkey->parent_id, tkey->partition_id);
      }
    }
    delete iter;
    delete table;
    delete file;
}

int main(int argc, char** argv) {
    std::vector<std::string> result;
    Status s;
    std::string dirname(argv[1]);
    s = Env::Default()->GetChildren(dirname, &result);

    for (int i=0; i < result.size(); ++i) {
      if (result[i].find(".sst") != std::string::npos
       && result[i].find(".crc") == std::string::npos)
        FindKey(dirname+std::string("/")+result[i]);
    }
    return 0;
}
