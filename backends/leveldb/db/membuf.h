/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-30 19:47:37
* File Name: membuf.h
* Description:
 ************************************************************************/

#ifndef STORAGE_LEVELDB_UTIL_MEMBUF_H_
#define STORAGE_LEVELDB_UTIL_MEMBUF_H_

namespace leveldb {

class MemBuffer {

public:
  MemBuffer(size_t write_buffer_size) :
    buffer_size(write_buffer_size), free_buffer_size(0) {
    buffer = new char[write_buffer_size];
  }

  ~MemBuffer() {
    delete [] buffer;
  }

  Status Append(const Slice& data, size_t &location) {
    if (data.size() > free_buffer_size) {
      return Status::BufferFull("Appending data to a nearly full buffer");
    }
    location = buffer_size - free_buffer_size;
    memcpy(buffer + location, data.data(), data.size());
    free_buffer_size = data.size();
  }

  Status Fetch(size_t offset, size_t size, Slice& data) {
      memcpy(data.data(), buffer+offset, size);
  }

  void Truncate() {
    free_buffer_size = buffer_size;
  }

private:
  char* buffer;
  size_t buffer_size;
  size_t free_buffer_size;

};

} //namespace leveldb

#endif
