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
  size_t location;
  char buf[sizeof(uint64_t)];
  EncodeVarint64(buf, (key.size()<<32)+(value.size()));

  membuf_->Append(Slice(buf, sizeof(buf)), location);
  datafile_->Append(Slice(buf, sizeof(buf)));
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

  //EncodeVarint64(buf, (GetLogNumber()<<32)+location);
  //indexdb_->Put(opt, key, Slice(buf, sizeof(buf)));

  char vbuf[20];
  sprintf(vbuf, "%d,%d", (int)GetLogNumber(), (int)location);
  indexdb_->Put(opt, key, Slice(vbuf, sizeof(vbuf)));
  return Status::OK();
}

Status ColumnDB::Delete(const WriteOptions& opt, const Slice& key) {
  return indexdb_->Delete(opt, key);
}

Status ColumnDB::Write(const WriteOptions& options, WriteBatch* updates) {
  return indexdb_->Write(options, updates);
}

Status ColumnDB::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  std::string location_val;
  Status s = indexdb_->Get(options, key, &location_val);
  if (!s.ok()) {
    return s;
  }
  /*
  uint64_t file_loc_val = DecodeFixed64(location_val.c_str());
  uint64_t file_number = file_loc_val >> 32;
  uint64_t offset = file_loc_val & (0xFFFFFFFFL);
  */
  int file_number, offset;
  sscanf(location_val.c_str(), "%d,%d", &file_number, &offset);

  const size_t kBufSize = 1024;
  char buf[kBufSize];
  Slice result;

  if (file_number == GetLogNumber()) {
    mutex_.Lock();
    s = membuf_->Get(offset, kBufSize, &result, buf);
    mutex_.Unlock();
  } else {
    s = data_cache_->Get(options, file_number, offset, kBufSize,
                         &result, buf);
  }
  if (!s.ok()) {
    return s;
  }
  /*
  uint64_t kv_size = DecodeFixed64(buf);
  uint64_t key_size = kv_size >> 32;
  uint64_t val_size = kv_size & (0xFFFFFFFFL);
  if (key_size + val_size + sizeof(kv_size) > result.size())
    return Status::IOError("Failed to read a full key value pair.");
  value->assign(buf+sizeof(uint64_t)+key_size, val_size);
  */
  return s;
}

Iterator* ColumnDB::NewIterator(const ReadOptions&) {
  return NULL;
}

const Snapshot* ColumnDB::GetSnapshot() {
  return NULL;
}

void ColumnDB::ReleaseSnapshot(const Snapshot* snapshot) {
}

bool ColumnDB::GetProperty(const Slice& property, std::string* value) {
  return true;
}

void ColumnDB::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
}

void ColumnDB::CompactRange(const Slice* begin, const Slice* end) {
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
  return Status::OK();
}

} // namespace leveldb
