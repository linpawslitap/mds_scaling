/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-10-04 16:32:56
* File Name: cdb_iter.cc
* Description:
 ************************************************************************/

#include "db/cdb_iter.h"
#include "db/dbformat.h"

namespace leveldb {

class ColumnDBIter: public Iterator {
 public:
  ColumnDBIter(const ReadOptions& options, ColumnDB* db, Iterator* iter)
      : options_(options),
        db_(db),
        iter_(iter),
        is_result_loaded_(false) {
  }
  virtual ~ColumnDBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return iter_->Valid(); }
  virtual Slice internalkey() const {
    return iter_->internalkey();
  }
  virtual Slice key() const {
    assert(iter_->Valid());
    return iter_->key();
  }

  virtual Slice value() {
    assert(iter_->Valid());
    if (!is_result_loaded_)
      LoadResult();
    return saved_result_;
  }
  virtual Status status() const {
    return iter_->status();
  }

  virtual void Next() {
    iter_->Next();
    is_result_loaded_ = false;
  }

  virtual void Prev() {
    iter_->Prev();
    is_result_loaded_ = false;
  }

  virtual void Seek(const Slice& target) {
    iter_->Seek(target);
    is_result_loaded_ = false;
  }

  virtual void SeekToFirst() {
    iter_->SeekToFirst();
    is_result_loaded_ = false;
  }

  virtual void SeekToLast() {
    iter_->SeekToLast();
    is_result_loaded_ = false;
  }

 private:
  ColumnDB* const db_;
  Iterator* const iter_;
  char buf[config::kBufSize];
  ReadOptions options_;
  Slice saved_result_;
  bool is_result_loaded_;


  void LoadResult() {
    uint64_t file_loc_val = DecodeFixed64(iter_->value().data());
    Status s = db_->InternalGet(options_, file_loc_val, buf, &saved_result_);
    if (!s.ok()) {
      saved_result_ = Slice(NULL, 0);
    }
    is_result_loaded_ = true;
  }

  // No copying allowed
  ColumnDBIter(const ColumnDBIter&);
  void operator=(const ColumnDBIter&);
};

Iterator* NewColumnDBIterator(
    const ReadOptions& options,
    ColumnDB* db,
    Iterator* internal_iter) {
  return new ColumnDBIter(options, db, internal_iter);
}

}  // namespace leveldb
