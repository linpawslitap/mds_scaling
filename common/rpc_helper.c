#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "rpc_giga.h"
#include "rpc_helper.h"

#include "giga_index.h"

typedef unsigned int uint_t;
typedef unsigned long ulong_t;


/* DANGER, WILL ROBINSON!: this depends on mode_t being a 32 bit int. It will
 * break otherwise
 */
bool_t xdr_mode_t(XDR *xdrs, mode_t *mode)
{
    if (!xdr_uint32_t(xdrs, (uint32_t *)mode))
        return FALSE;
    return TRUE;
}

bool_t xdr_giga_mapping_t(XDR *xdrs, struct giga_mapping_t *objp)
{
    if (!xdr_u_int(xdrs, &objp->curr_radix))
        return FALSE;
    if (!xdr_u_int(xdrs, &objp->zeroth_server))
        return FALSE;
    if (!xdr_u_int(xdrs, &objp->server_count))
        return FALSE;
    if (!(xdr_vector(xdrs, (char *)objp->bitmap, MAX_BMAP_LEN,
                     sizeof(char), (xdrproc_t) xdr_char)))
        return FALSE;

    return TRUE;
}

bool_t xdr_stat(XDR *xdrs, struct stat *objp)
{
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_dev))
    		return FALSE;
//    if(!xdr_u_short (xdrs, &objp->__pad1))
//    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_ino))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_mode))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_nlink))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_uid))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_gid))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_rdev))
    		return FALSE;
//    if(!xdr_u_short (xdrs, &objp->__pad2))
//    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_size))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_blksize))
    		return FALSE;
    if(!xdr_u_int (xdrs, (uint_t *)&objp->st_blocks))
		return FALSE;
    if(!xdr_giga_timestamp_t (xdrs, (struct giga_timestamp_t *)&objp->st_atime)) 
    		return FALSE;
    if(!xdr_giga_timestamp_t (xdrs, (struct giga_timestamp_t *)&objp->st_mtime))
    		return FALSE;
    if(!xdr_giga_timestamp_t (xdrs, (struct giga_timestamp_t *)&objp->st_ctime))
    		return FALSE;
    return TRUE;
}

bool_t xdr_statvfs(XDR *xdrs, struct statvfs *objp)
{
    if(!xdr_u_long (xdrs, &objp->f_bsize))
    		return FALSE;
    if(!xdr_u_long (xdrs, &objp->f_frsize))
    		return FALSE;
    if(!xdr_int (xdrs, (int *)&objp->f_blocks))
    		return FALSE;
    if(!xdr_int (xdrs, (int *) &objp->f_bfree))
    		return FALSE;
    if(!xdr_int (xdrs, (int *) &objp->f_bavail))
    		return FALSE;
    if(!xdr_int (xdrs, (int *)&objp->f_files))
    		return FALSE;
    if(!xdr_int (xdrs, (int *)&objp->f_ffree))
    		return FALSE;
    if(!xdr_int (xdrs, (int *)&objp->f_favail))
    		return FALSE;
    if(!xdr_u_long (xdrs, &objp->f_fsid))
    		return FALSE;
    if(!xdr_u_long (xdrs, &objp->f_flag))
    		return FALSE;
    if(!xdr_u_long (xdrs, &objp->f_namemax))
    		return FALSE;
    if(!xdr_int (xdrs, (int *)&objp->__f_spare))
    		return FALSE;
    return TRUE;
}




/*
bool_t xdr_hashMapping(XDR *xdrs, hashMapping *objp) {
	if (!xdr_u_int(xdrs, &objp->server_id))
		return FALSE;
	if (!xdr_vector(xdrs, (char *)objp->hash_val, 2*SHA1_HASH_SIZE+1, sizeof(char),
			(xdrproc_t) xdr_char))
		return FALSE;

	return TRUE;
}
*/

