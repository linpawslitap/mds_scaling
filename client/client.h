#ifndef CLIENT_H
#define CLIENT_H

/* The following excerpt is from the /usr/include/fuse/fuse.h
 *
 * IMPORTANT: you should define FUSE_USE_VERSION before including this
 * header.  To use the newest API define it to 26 (recommended for any
 * new application), to use the old API define it to 21 (default) 22
 * or 25, to use the even older 1.X API define it to 11.
*/
#define FUSE_USE_VERSION    26

#include "common/options.h"

struct giga_options giga_options_t;

#endif
