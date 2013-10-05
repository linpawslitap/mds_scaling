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
#include "db/data_cache.h"

namespace leveldb {

class ColumnDBIter;

class ColumnDB : public DB {
 public:
  ColumnDB(const Options& options, const std::string& dbname, Status& s);
  virtual ~ColumnDB();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Status Exists(const ReadOptions& options,
                        const Slice& key);
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

  friend class ColumnDBIter;

 private:

  Env* const env_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const std::string dbname_;

  DB* indexdb_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  WritableFile* datafile_;
  MemBuffer* membuf_;
  DataCache* data_cache_;
  uint64_t log_number_;
  uint64_t current_log_number_;

  uint64_t NewLogNumber() {
    return ++log_number_;
  }
  void SetLogNumber(uint64_t log_number) {
    current_log_number_ = log_number;
  }
  uint64_t GetLogNumber() {
    return current_log_number_;
  }

  Status NewDataFile();
  Status InternalGet(const ReadOptions& options, uint64_t file_number,
                     char* scratch, Slice* result);

  // No copying allowed
  ColumnDB(const ColumnDB&);
  void operator=(const ColumnDB&);
};

} //namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
