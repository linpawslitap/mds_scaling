// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/posix_logger.h"

//#define PLATFORM_HDFS
#ifdef PLATFORM_HDFS
#include "hdfs.h"
#endif

#include <map>
#include <arpa/inet.h>
#include <sys/types.h> 
#include <sys/socket.h> 
#include <netdb.h> 
//TODO: Duplicate Code ! Copied from common/connection.c
static std::string getHostIPAddress()
{
		char ip_addr[HOST_NAME_MAX];

    char hostname[HOST_NAME_MAX] = {0};
    hostname[HOST_NAME_MAX-1] = '\0';
    gethostname(hostname, HOST_NAME_MAX-1);

    int gai_result;
    struct addrinfo hints, *info;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;
    hints.ai_protocol = 0;

    if ((gai_result = getaddrinfo(hostname, NULL, &hints, &info)) != 0) {
        fprintf(stdout, "[%s] getaddrinfo(%s) failed. [%s]\n",
                __func__, hostname, gai_strerror(gai_result));
        exit(1);
    }

    void *ptr = NULL;
    struct addrinfo *p;
    for (p = info; p != NULL; p = p->ai_next) {
        inet_ntop (p->ai_family, p->ai_addr->sa_data, ip_addr, HOST_NAME_MAX);
        switch (p->ai_family) {
            case AF_INET:
                ptr = &((struct sockaddr_in *) p->ai_addr)->sin_addr;
                break;
            case AF_INET6:
                ptr = &((struct sockaddr_in6 *) p->ai_addr)->sin6_addr;
                break;
        }

        inet_ntop (p->ai_family, ptr, ip_addr, HOST_NAME_MAX);
    }

		std::string host_ip(ip_addr);
    return host_ip;
}

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

/* Begin: HDFS Env */
//static const char *primaryNamenode = "10.1.1.74";

#define HDFS_VERBOSITY 1

void hdfsDebugLog(short verbosity, const char* format, ... ) {
    if(verbosity <= HDFS_VERBOSITY) {
			va_list args;
    	va_start( args, format );
    	vfprintf( stdout, format, args );
    	va_end( args );
		}
}

//Check if the file should be accessed from HDFS
static bool onHDFS(const std::string &fname) {
  bool on_hdfs = false;
#ifdef PLATFORM_HDFS
  static const std::string ext_sst = ".sst";
  static const std::string ext_log = ".log";
  if (fname.length() >= ext_sst.length()) {
    on_hdfs =  (!fname.compare(fname.length() - ext_sst.length(), ext_sst.length(), ext_sst))
      || (!fname.compare(fname.length() - ext_log.length(), ext_log.length(), ext_log));
  }
#endif
  return on_hdfs;
}

#ifdef PLATFORM_HDFS
static hdfsFS hdfs_primary_fs_;
static std::map<std::string, hdfsFS> hdfs_connections_;

static const std::string hdfs_prefix = "hdfs://";
static bool isRemote(const std::string& fname) {
	return fname.compare(0, hdfs_prefix.length(), hdfs_prefix) == 0;
}

std::string getHost(const std::string &fname) {
	int end;
	for(end = hdfs_prefix.length(); end < fname.length(); ++end) {
		if(fname.at(end) == '/') {
			break;
		}	
	}	
	std::string host = fname.substr(hdfs_prefix.length(), end - hdfs_prefix.length());	
	hdfsDebugLog(1, "HDFS: Resolving host from remote path[%s] host[%s] \n", 
		fname.c_str(), host.c_str());
	return host;
}

std::string getPath(const std::string &fname) {
	std::string new_path(fname);
	if(isRemote(fname)) {
		std::string host = getHost(fname);	
		new_path = fname.substr(hdfs_prefix.length() + host.length(), 
			fname.length() - hdfs_prefix.length() - host.length());	
		hdfsDebugLog(1, "HDFS: Resolving path from remote path[%s] path[%s] \n", 
			fname.c_str(), new_path.c_str());
	}
	return new_path;
}

static hdfsFS connectFS(const std::string &fname) {
	hdfsFS hdfs_fs;
	if(!isRemote(fname) || !getHost(fname).compare(getHostIPAddress())) {
		hdfsDebugLog(1, "HDFS: Resolved host from remote path[%s] to primary\n", 
			fname.c_str());
		hdfs_fs = hdfs_primary_fs_;
	} else {
		std::string secondary_host = getHost(fname); 
		if(hdfs_connections_.find(secondary_host) == hdfs_connections_.end()) {
			hdfsDebugLog(0, "HDFS: Connecting to secondary host %s\n", secondary_host.c_str());
  		hdfs_fs = hdfsConnect(secondary_host.c_str(), 8020);
		} else {
			hdfsDebugLog(1, "HDFS: Found in secondary host map %s\n", secondary_host.c_str());
			hdfs_fs = hdfs_connections_[secondary_host];
		}
	}
	return hdfs_fs;
}

static void hdfsDisconnect_all() {
	//TODO:
}

class HDFSSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  hdfsFile file_;
	hdfsFS hdfs_fs_;

 public:
  HDFSSequentialFile(const std::string& fname, hdfsFile f)
      : filename_(fname), file_(f){
		hdfs_fs_ = connectFS(fname);
		filename_ = getPath(fname);
	}
  virtual ~HDFSSequentialFile() { 
		hdfsCloseFile(hdfs_fs_,file_);
	}

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    tSize r = hdfsRead(hdfs_fs_, file_, scratch, n);
    *result = Slice(scratch, r);
    if (r < n) {
      if ( r != -1) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
   	tOffset cur = hdfsTell(hdfs_fs_, file_);
		if(cur == -1) {
      return IOError(filename_, errno);
		}

		int r = hdfsSeek(hdfs_fs_, file_, cur + static_cast<tOffset>(n)); 
    if (r != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

class HDFSRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  hdfsFile file_;
	hdfsFS hdfs_fs_;

 public:
  HDFSRandomAccessFile(const std::string& fname, hdfsFile file)
      : filename_(fname), file_(file) { 
		hdfs_fs_ = connectFS(fname);
		filename_ = getPath(fname);
	}
  virtual ~HDFSRandomAccessFile() { 
		hdfsCloseFile(hdfs_fs_, file_);
	}
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    hdfsDebugLog(2, "Read path %s\n", filename_.c_str());

    Status s;
    ssize_t r = hdfsPread(hdfs_fs_, file_, static_cast<tOffset>(offset), scratch, static_cast<tSize>(n));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      hdfsDebugLog(0, "Error in Read path %s\n", filename_.c_str());
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class HDFSWritableFile : public WritableFile {
 private:
  std::string filename_;
  hdfsFile file_;
	hdfsFS hdfs_fs_;

 public:
  HDFSWritableFile(const std::string& fname, hdfsFile file)
      : filename_(fname),
        file_(file){
		hdfs_fs_ = connectFS(fname);
		filename_ = getPath(fname);
  }

  ~HDFSWritableFile() {
    if (file_ != NULL) {
      HDFSWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    hdfsDebugLog(2, "Append path %s\n", filename_.c_str());

    const char* src = data.data();
    size_t data_size = data.size();
    if (hdfsWrite(hdfs_fs_, file_, src, data_size) < 0) {
      hdfsDebugLog(0, "Error in append path %s\n", filename_.c_str());
      return IOError(filename_, errno);
    }

    return Status::OK();
  }

  virtual Status Close() {
    hdfsDebugLog(1, "Close path %s\n", filename_.c_str());

    Status s;
    if (hdfsCloseFile(hdfs_fs_, file_) < 0) {
      hdfsDebugLog(0, "Error in close path %s\n", filename_.c_str());

      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    file_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    hdfsDebugLog(2, "Sync path %s\n", filename_.c_str());

    Status s;

    if(hdfsFlush(hdfs_fs_, file_) < 0) {
      hdfsDebugLog(0, "Error in sync path %s\n", filename_.c_str());

      s = IOError(filename_, errno);
    }
    return s;
  }
};
#endif
/* End: HDFS Env */

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
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
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  uint64_t file_offset_;  // Offset of base_ in file

 public:
  PosixWritableFile(const std::string& fname, int fd)
      : filename_(fname),
        fd_(fd),
        file_offset_(0) {
  }


  ~PosixWritableFile() {
    if (fd_ >= 0) {
      PosixWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t data_size = data.size();
    if (pwrite(fd_, src, data_size, file_offset_) < 0) {
      return IOError(filename_, errno);
    }
    file_offset_ += data_size;

    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    fd_ = -1;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (fsync(fd_) < 0) {
        s = IOError(filename_, errno);
    }

    return s;
  }
};


static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
//  return fcntl(fd, F_SETLK, &f);
  return 0;
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
};
  
class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
#ifdef PLATFORM_HDFS    
    hdfsDisconnect(hdfs_primary_fs_);
		hdfsDisconnect_all();
#endif
    exit(1);
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
			std::string filename = getPath(fname);
    	hdfsFile f = hdfsOpenFile(connectFS(fname), filename.c_str(), O_RDONLY, 0, 1, 0);
			if (f == NULL) {
				*result = NULL;
				return IOError(fname, errno);
			} else {
				*result = new HDFSSequentialFile(fname, f);
				return Status::OK();
			}
		} else {
#endif
			FILE* f = fopen(fname.c_str(), "r");
			if (f == NULL) {
				*result = NULL;
				return IOError(fname, errno);
			} else {
				*result = new PosixSequentialFile(fname, f);
				return Status::OK();
			}
#ifdef PLATFORM_HDFS    
		}
#endif
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
			std::string filename = getPath(fname);
      hdfsFile new_file = NULL;
      int tries = 0;
      while(tries++ < 3 && new_file == NULL) {
        new_file = hdfsOpenFile(connectFS(fname), filename.c_str(), O_RDONLY, 0, 1, 0);
      }

      if(new_file == NULL) {
        //hdfsFile create_file = hdfsOpenFile(hdfs_fs_, fname.c_str(), O_WRONLY|O_CREAT, 0, 1, 0);
        //if(create_file != NULL) {
        //  hdfsCloseFile(hdfs_fs_, create_file);
        //}
        new_file = hdfsOpenFile(connectFS(fname), filename.c_str(), O_RDONLY, 0, 1, 0);
      }

      if (new_file == NULL) {
        hdfsDebugLog(0, "Error on open random access file path %s\n", fname.c_str());

        s = IOError(fname, errno);
      } else {
        *result = new HDFSRandomAccessFile(fname, new_file);
      }
    } else {
#endif
      int fd = open(fname.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(fname, errno);
      } else {
        *result = new PosixRandomAccessFile(fname, fd);
      }
#ifdef PLATFORM_HDFS    
    }
#endif
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
			std::string filename = getPath(fname);
      int exists = hdfsExists(connectFS(fname), filename.c_str());
      hdfsFile new_file;
      //if (exists > -1) {
      //  new_file = hdfsOpenFile(hdfs_fs_, fname.c_str(), O_WRONLY|O_APPEND, 0, 1, 0);
      //} else{
        new_file = hdfsOpenFile(connectFS(fname), filename.c_str(), O_WRONLY|O_CREAT, 0, 1, 0);
      //}
      if (new_file == NULL) {
        hdfsDebugLog(0, "Error on writable path %s\n", fname.c_str());

        *result = NULL;
        s = IOError(fname, errno);
      } else {
        *result = new HDFSWritableFile(fname, new_file);
      }    
    } else {  
#endif
      const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
      if (fd < 0) {
        *result = NULL;
        s = IOError(fname, errno);
      } else {
        *result = new PosixWritableFile(fname, fd);
      }
#ifdef PLATFORM_HDFS    
    }
#endif
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
			std::string filename = getPath(fname);
      hdfsDebugLog(1, "Check file exists path %s\n", fname.c_str());

      return (hdfsExists(connectFS(fname), filename.c_str()) == 0);
    }
#endif
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();

#ifdef PLATFORM_HDFS    
    hdfsDebugLog(1, "Get children path %s\n", dir.c_str());
    //TODO: Directory read from both HDFS and local FS 
    int num_entries = 0;
    hdfsFileInfo* entries = hdfsListDirectory(hdfs_primary_fs_, dir.c_str(), &num_entries);
    if(entries == NULL) {
      hdfsDebugLog(0, "Error in get children path %s. Num entries=%d\n", dir.c_str(), num_entries);

      //TODO:
      //return IOError(dir, errno);
    } else {  
      for (int i = 0; i < num_entries; ++i) {
        result->push_back(entries[i].mName);
      }
      hdfsFreeFileInfo(entries, num_entries);   
    }
#endif

    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
			std::string filename = getPath(fname);
      hdfsDebugLog(1, "Delete file path %s\n", fname.c_str());

      if (hdfsDelete(connectFS(fname), filename.c_str(), 0) != 0) {
      	hdfsDebugLog(0, "Error delete file path %s\n", fname.c_str());

        result = IOError(fname, errno);
      }
    } else {
#endif
				if (unlink(fname.c_str()) != 0) {
      		result = IOError(fname, errno);
    	}
#ifdef PLATFORM_HDFS    
		}
#endif
    return result;
  };

  virtual Status CreateDir(const std::string& name) {

    Status result;
#ifdef PLATFORM_HDFS    
    hdfsDebugLog(1, "Create dir %s\n", name.c_str());
    if (hdfsCreateDirectory(hdfs_primary_fs_, name.c_str()) != 0) {
      hdfsDebugLog(0, "Error in create dir %s\n", name.c_str());

      result = IOError(name, errno);
    } else {
      hdfsChmod(hdfs_primary_fs_, name.c_str(), 0755);
    }
#endif
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) {

    Status result;
#ifdef PLATFORM_HDFS    
    hdfsDebugLog(1, "Delete dir %s\n", name.c_str());
    if (hdfsDelete(hdfs_primary_fs_, name.c_str(), 1) != 0) {
      hdfsDebugLog(0, "Error in delete dir %s\n", name.c_str());

      result = IOError(name, errno);
    }
#endif
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
      fprintf(stdout, "HDFS: Get File Size %s not implemented\n", fname.c_str());
      exit(0);
    }
#endif
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status CopyFile(const std::string& src, const std::string& target) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Copy File src:%s target:%s not implemented\n", src.c_str(), target.c_str());
      exit(0);
    }
#endif
    Status result;
    int r_fd, w_fd;
    if ((r_fd = open(src.c_str(), O_RDONLY)) < 0) {
      result = IOError(src, errno);
      return result;
    }
    if ((w_fd = open(target.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
      result = IOError(target, errno);
      return result;
    }

    int p[2];
    pipe(p);

    while(splice(p[0], 0, w_fd, 0, splice(r_fd, 0, p[1], 0, 4096, 0), 0) > 0);

    close(r_fd);
    close(w_fd);

    return result;
  }

  virtual Status SymlinkFile(const std::string& src, const std::string& target) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Symlink File src:%s target:%s. Not supported\n", src.c_str(), target.c_str());
      exit(0);
    }
#endif    
    Status result;

    if (symlink(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Rename File src:%s target:%s not implemented\n", src.c_str(), target.c_str());
      exit(0);
    }
#endif

    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(src) || onHDFS(target)) {
      fprintf(stdout, "HDFS: Link File src:%s target:%s. Not supported\n", src.c_str(), target.c_str());
      exit(0);
    }
#endif    
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
#ifdef PLATFORM_HDFS    
    if(onHDFS(fname)) {
      fprintf(stdout, "HDFS: Lock File %s. Not supported\n", fname.c_str());
      exit(0);
    }
#endif    
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      exit(1);
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;
};

PosixEnv::PosixEnv() : page_size_(getpagesize()),
                       started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));

#ifdef PLATFORM_HDFS
	std::string ip_addr = getHostIPAddress();
  fprintf(stdout, "HDFS: Connecting to %s...\n", ip_addr.c_str());
  hdfs_primary_fs_ = hdfsConnect(ip_addr.c_str(), 8020);
#endif
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
