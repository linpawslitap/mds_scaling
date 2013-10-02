/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-30 17:53:32
* File Name: column_db.cc
* Description:
 ************************************************************************/

#include "db/column_db.h"

namespace leveldb {

ColumnDB::ColumnDB(const Options& options, const std::string& dbname) :
  env_(options.env), datafile_(NULL), indexdb(NULL), membuf(NULL) {
  mutex_.Lock();
  Status s = DB::Open(options, dname, &indexdb);
  if (!s.ok()) {
    dbptr = NULL;
    return;
  }
  uint64_t new_log_number = NewFileNumber();
  WritableFile* lfile;
  s = options.env->NewWritableFile(DataFileName(dname, new_log_number),
                                   &lfile);
  if (s.ok()) {
    SetLogNumber(new_log_number);
    datafile_ = lfile;
  }
  membuf = new MemBuffer(options.write_buffer_size);
}

}
