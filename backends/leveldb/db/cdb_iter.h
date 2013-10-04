/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-10-04 16:31:16
* File Name: ./cdb_iter.h
* Description:
 ************************************************************************/

#ifndef STORAGE_LEVELDB_DB_CDB_ITER_H_
#define STORAGE_LEVELDB_DB_CDB_ITER_H_

#include <stdint.h>
#include "db/column_db.h"

namespace leveldb {

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewColumnDBIterator(
    const ReadOptions& opttions,
    ColumnDB* db,
    Iterator* internal_iter);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_CDB_ITER_H_
