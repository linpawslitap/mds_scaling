/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-30 17:53:32
* File Name: column_db.cc
* Description:
 ************************************************************************/

#include "db/column_db.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "db/filename.h"
#include "db/dbformat.h"
#include "db/cdb_iter.h"
#include <stdio.h>
#include <stdlib.h>

namespace leveldb {

ColumnDB::ColumnDB(const Options& options, const std::string& dbname) :
  dbname_(dbname), env_(options.env), datafile_(NULL), indexdb_(NULL),
  membuf_(NULL), log_number_(0), current_log_number_(0) {
  //mutex_.Lock();
  Status s = DB::Open(options, dbname, &indexdb_);
  if (!s.ok()) {
    fprintf(stderr, "Open DB Error: %s\n", s.ToString().c_str());
    exit(-1);
    return;
  }
  s = NewDataFile();
  if (!s.ok()) {
    fprintf(stderr, "Open DataFile Error: %s\n", s.ToString().c_str());
    exit(-1);

    return;
  }
  membuf_ = new MemBuffer(options.write_buffer_size);
  data_cache_ = new DataCache(dbname, &options, options.max_open_files);
  //mutex_.Unlock();
}

ColumnDB::~ColumnDB() {
  if (datafile_ != NULL) {
    datafile_->Close();
    delete datafile_;
  }
  delete membuf_;
  delete data_cache_;
  delete indexdb_;
}

Status ColumnDB::NewDataFile() {
  uint64_t new_log_number = NewLogNumber();
  WritableFile* lfile;
  Status s = env_->NewWritableFile(DataFileName(dbname_, new_log_number),
                                   &lfile);
  if (s.ok()) {
    SetLogNumber(new_log_number);
    if (datafile_ != NULL) {
      datafile_->Close();
      delete datafile_;
    }
    datafile_ = lfile;
  } else {
    return s;
  }
}

Status ColumnDB::Put(const WriteOptions& opt, const Slice& key,
                     const Slice& value) {
  Status s;
  mutex_.Lock();
  if (!membuf_->HasEnough(sizeof(uint64_t)+key.size()+value.size())) {
    s = NewDataFile();
    if (!s.ok())
      return s;
    membuf_->Truncate();
  }

  //Put header
  size_t location;
  char buf[sizeof(uint64_t)];
  EncodeFixed64(buf, (key.size()<<32)+(value.size()));
  membuf_->Append(Slice(buf, sizeof(buf)), location);
  datafile_->Append(Slice(buf, sizeof(buf)));

  //Put Key Value
  size_t tmp;
  membuf_->Append(key, tmp);
  membuf_->Append(value, tmp);
  s = datafile_->Append(key);
  if (!s.ok()) return s;
  s = datafile_->Append(value);
  if (!s.ok()) return s;
  if (opt.sync)
    datafile_->Flush();
  mutex_.Unlock();

  EncodeFixed64(buf, (GetLogNumber()<<32)+location);
  indexdb_->Put(opt, key, Slice(buf, sizeof(buf)));

  return Status::OK();
}

Status ColumnDB::Delete(const WriteOptions& opt, const Slice& key) {
  return indexdb_->Delete(opt, key);
}

Status ColumnDB::Write(const WriteOptions& options, WriteBatch* updates) {
  return indexdb_->Write(options, updates);
}

Status ColumnDB::InternalGet(const ReadOptions& options,
                             uint64_t file_loc, char* buf, Slice* result) {
  Status s;

  uint64_t file_number = file_loc >> 32;
  uint64_t offset = file_loc & (0xFFFFFFFFL);
  if (file_number == GetLogNumber()) {
    mutex_.Lock();
    if (file_number == GetLogNumber()) {
      s = membuf_->Get(offset, config::kBufSize, result, buf);
      mutex_.Unlock();
    } else {
      mutex_.Unlock();
      s = data_cache_->Get(options, file_number, offset, config::kBufSize,
                           result, buf);
    }
  } else {
    s = data_cache_->Get(options, file_number, offset, config::kBufSize,
                         result, buf);
  }
  if (!s.ok())
    return s;

  uint64_t kv_size = DecodeFixed64(buf);
  uint64_t key_size = kv_size >> 32;
  uint64_t val_size = kv_size & (0xFFFFFFFFL);
  if (key_size + val_size + sizeof(kv_size) > result->size()) {
    return Status::IOError("Failed to read a full key value pair.");
  }
  *result = Slice(buf+sizeof(uint64_t)+key_size, val_size);
  return s;
}

Status ColumnDB::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  std::string location_val;
  Status s = indexdb_->Get(options, key, &location_val);
  if (!s.ok()) return s;

  uint64_t file_loc = DecodeFixed64(location_val.c_str());
  char buf[config::kBufSize];
  Slice result;
  s = InternalGet(options, file_loc, buf, &result);

  if (s.ok()) {
    value->assign(result.data(), result.size());
  }

  return s;
}

Iterator* ColumnDB::NewIterator(const ReadOptions& opt) {
  return NewColumnDBIterator(opt, this, indexdb_->NewIterator(opt));;
}

const Snapshot* ColumnDB::GetSnapshot() {
  return NULL;
}

void ColumnDB::ReleaseSnapshot(const Snapshot* snapshot) {
}

bool ColumnDB::GetProperty(const Slice& property, std::string* value) {
  return indexdb_->GetProperty(property, value);
}

void ColumnDB::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  indexdb_->GetApproximateSizes(range, n, sizes);
}

void ColumnDB::CompactRange(const Slice* begin, const Slice* end) {
  indexdb_->CompactRange(begin, end);
}

Status ColumnDB::BulkSplit(const WriteOptions& options, uint64_t sequence,
                           const Slice* begin, const Slice* end,
                           const std::string& dname) {
  return Status::OK();
}

Status ColumnDB::BulkInsert(const WriteOptions& options,
                  const std::string& fname,
                  uint64_t min_sequence_number,
                  uint64_t max_sequence_number) {
  return indexdb_->BulkInsert(options, fname,
                              min_sequence_number, max_sequence_number);;
}

} // namespace leveldb
