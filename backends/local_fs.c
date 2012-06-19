
#include "common/debugging.h"

#include "operations.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define LOCALFS_LOG LOG_DEBUG

int local_getattr(const char *path, struct stat *stbuf)
{
    int ret = 0;

    if ((ret = lstat(path, stbuf)) < 0) {
        logMessage(LOCALFS_LOG, __func__,
                   "stat(%s) failed: %s", path, strerror(errno));
        ret = errno;
    }
    
    return ret;
}

int local_mkdir(const char *path, mode_t mode)
{
    int ret = 0;

    if ((ret = mkdir(path, mode)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "mkdir(%s) failed: %s", path, strerror(errno));
        ret = errno;
    }
    
    return ret;
}

int local_symlink(const char *path, const char *link)
{
    int ret = 0;

    if ((ret = symlink(path, link)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "symlink(%s,%s) failed: %s", path, link, strerror(errno));
        ret = errno;
    }
    
    return ret;
}

int local_readlink(const char *path, char *link, size_t size)
{
    int ret = 0;

    if ((ret = readlink(path, link, size-1)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "readlink(%s,%s) failed: %s", path, link, strerror(errno));
        ret = errno;
    }
    else  {
	    link[ret] = '\0';
        ret = 0;
    }
    
    return ret;
}

// NOTE: ret_fd is to return the fd from the open call (needed for fi->fh)
int local_open(const char *path, int flags, int *ret_fd)
{
    int ret = 0;
    int fd;
    
    if ((fd = open(path, flags)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "open(%s) failed: %s", path, strerror(errno));
        ret = errno;
    }

    *ret_fd = fd;

    return ret;
}

int local_mknod(const char *path, mode_t mode, dev_t dev)
{
    int ret = 0;

    /*
    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        if ((ret = open(path, O_CREAT | O_EXCL | O_WRONLY, mode) < 0)) {
            logMessage(LOG_FATAL, __func__,
                       "open(%s, CRT|EXCL|WR) failed: %s", path, strerror(errno));
            ret = (errno);
        }
        else {
            // close the opened file.
            if ((ret = close(ret)) < 0) {
                logMessage(LOG_FATAL, __func__,
                           "close(%d) failed: %s", ret, strerror(errno));
                ret = (errno);
            }
	    }
    } 
    else if (S_ISFIFO(mode)) {
	    if ((ret = mkfifo(path, mode)) < 0) {
            logMessage(LOG_FATAL, __func__,
                       "mkfifo(%s) failed: %s", path, strerror(errno));
            ret = (errno);
        }
	} else {
	    if ((ret = mknod(path, mode, dev)) < 0) {
            logMessage(LOG_FATAL, __func__,
                       "mknod(%s) failed: %s", path, strerror(errno));
            ret = (errno);
        }
	}
    */

    if ((ret = mknod(path, mode, dev)) < 0) {
        logMessage(LOG_FATAL, __func__,
                   "mknod(%s) failed: %s", path, strerror(errno));
        ret = (errno);
    }

    return ret;
}

