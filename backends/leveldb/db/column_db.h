/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-30 17:37:43
* File Name: ./column_db.h
* Description:
 ************************************************************************/

#ifndef STORAGE_LEVELDB_DB_COLUMN_DB_H_
#define STORAGE_LEVELDB_DB_COLUMN_DB_H_

#include "db/db_impl.h"
#include "db/membuf.h"

namespace leveldb {

class ColumnDB : public DB {
 public:
  ColumnDB(const Options& options, const std::string& dbname);
  virtual ~ColumnDB();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status BulkSplit(const WriteOptions& options, uint64_t sequence,
                           const Slice* begin, const Slice* end,
                           const std::string& dname);
  virtual Status BulkInsert(const WriteOptions& options,
                            const std::string& fname,
                            uint64_t min_sequence_number,
                            uint64_t max_sequence_number);

 private:
  Env* const env_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const std::string dbname_;

  DB* indexdb;
  MemBuffer* membuf;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  WritableFile* datafile_;

  // No copying allowed
  ColumnDB(const ColumnDB&);
  void operator=(const ColumnDB&);
};

} //namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
