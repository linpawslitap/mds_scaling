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

#define DEFAULT_LOG_FILE_LOCATIONs   "/tmp/dbg.log.s"
#define DEFAULT_LOG_FILE_LOCATIONc  "/tmp/dbg.log.c"
#define MAX_ERR_BUF_SIZE    512

#define TIMESTAMP_ENABLED   1

log_level_t sys_log_level;          /* Log level */
FILE *log_fp;                       /* Log file stream */


void logOpen(const char *logFilename, log_level_t level);
void logClose(void);

void logMessage(log_level_t lev, const char *location, const char *format, ...);

#endif /* DEBUGGING_H */
