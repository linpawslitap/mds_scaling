/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-08-15 22:57:50
* File Name: env_posix.h
* Description:
 ************************************************************************/

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch);
  virtual Status Skip(uint64_t n);

};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                    char* scratch) const;
};

} //namespace leveldb
