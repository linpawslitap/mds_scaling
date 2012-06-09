#ifndef DEBUGGING_H
#define DEBUGGING_H   

#include <stdio.h>
#include <sys/stat.h>

/* 
 * Error handling functions. 
 * */

void errMsg(const char *format, ...);

#ifdef __GNUC__

    /* This macro stops 'gcc -Wall' complaining that "control reaches
     *        end of non-void function" if we use the following functions to
     *               terminate main() or some other non-void function. */

#define NORETURN __attribute__ ((__noreturn__))
#else
#define NORETURN
#endif

void errExit(const char *format, ...) NORETURN ;
void err_exit(const char *format, ...) NORETURN ;
void errExitEN(int errnum, const char *format, ...) NORETURN ;
void fatal(const char *format, ...) NORETURN ;
void usageErr(const char *format, ...) NORETURN ;
void cmdLineErr(const char *format, ...) NORETURN ;

/* 
 * Logging related declarations and definitions. 
 * */

typedef enum log_level {
    LOG_FATAL,
    LOG_ERR,
    LOG_WARN,
    LOG_DEBUG,
    LOG_TRACE,
} log_level_t;


#define DEFAULT_LOG_LEVEL       LOG_DEBUG

#define DEFAULT_LOG_FILE_PATH   "/tmp/dbg.log"

#define MAX_ERR_BUF_SIZE    512

#define TIMESTAMP_ENABLED   1

log_level_t sys_log_level;          /* Log level */
FILE *log_fp;                       /* Log file stream */


void logOpen(const char *logFilename, log_level_t level);
void logClose(void);

void logMessage(log_level_t lev, const char *location, const char *format, ...);

/*
 * Macros for mutex debugging.
 */

#define ACQUIRE_MUTEX(lock, format, ...)  \
{                                 \
        logMessage(DEFAULT_LOG_LEVEL, "LOCK_TRY_", format, __VA_ARGS__);     \
        pthread_mutex_lock(lock);                         \
        logMessage(DEFAULT_LOG_LEVEL, "LOCK_DONE_", format, __VA_ARGS__);    \
}

#define RELEASE_MUTEX(lock, format, ...)  \
{                                 \
        logMessage(DEFAULT_LOG_LEVEL, "UNLOCK_TRY_", format, __VA_ARGS__);     \
        pthread_mutex_unlock(lock);                       \
        logMessage(DEFAULT_LOG_LEVEL, "UNLOCK_DONE_", format, __VA_ARGS__);     \
}

#define ACQUIRE_RWLOCK_READ(lock, msg)  \
{                                 \
        logMessage(DEFAULT_LOG_LEVEL, "ACQUIRE READLOCK:","[%s]", msg);    \
        pthread_rwlock_rdlock(lock);                         \
        logMessage(DEFAULT_LOG_LEVEL, "SUCCESS READLOCK:","[%s]", msg);    \
}

#define ACQUIRE_RWLOCK_WRITE(lock, msg)  \
{                                 \
        logMessage(DEFAULT_LOG_LEVEL, "ACQUIRE WRITELOCK:","[%s]", msg);    \
        pthread_rwlock_wrlock(lock);                         \
        logMessage(DEFAULT_LOG_LEVEL, "SUCCESS: WRITELOCK","[%s]", msg);    \
}

#define RELEASE_RWLOCK(lock, msg)  \
{                                 \
        logMessage(DEFAULT_LOG_LEVEL, "RELEASE RWLOCK:","[%s]", msg);    \
        pthread_rwlock_unlock(lock);                       \
        logMessage(DEFAULT_LOG_LEVEL, "SUCCESS RWLOCK:","[%s]", msg);    \
}

#endif /* DEBUGGING_H */
