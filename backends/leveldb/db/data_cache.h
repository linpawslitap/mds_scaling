// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_DATA_CACHE_H_
#define STORAGE_LEVELDB_DB_DATA_CACHE_H_

#include <string>
#include <stdint.h>
#include "util/env.h"
#include "port/port.h"

namespace leveldb {

class Env;

class DataCache {
 public:
  DataCache(const std::string& dbname, const Options* options, int entries);
  ~DataCache();

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t offset,
             uint64_t size,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options* options_;
  Cache* cache_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DATA_CACHE_H_
