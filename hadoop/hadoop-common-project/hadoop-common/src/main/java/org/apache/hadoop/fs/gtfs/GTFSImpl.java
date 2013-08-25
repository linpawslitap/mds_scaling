package org.apache.hadoop.fs.gtfs;

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 8/12/13
 * Time: 4:15 PM
 * Java Wrapper of GIGA+TableFS Client
 */

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class GTFSImpl {

    public static class Stat extends Structure {
        public int st_dev;   /* Device.  */
        public int st_ino;   /* File serial number.  */
        public int st_mode;  /* File mode.  */
        public int st_nlink; /* Link count.  */
        public int st_uid;   /* User ID of the file's owner.  */
        public int st_gid;   /* Group ID of the file's group. */
        public int st_rdev;  /* Device number, if device.  */
        public int __pad1;
        public int st_size;  /* Size of file, in bytes.  */
        public int st_blksize; /* Optimal block size for I/O.  */
        public int   __pad2;
        public int st_blocks;  /* Number 512-byte blocks allocated. */
        public int st_atime; /* Time of last access.  */
        public int st_atime_nsec;
        public int st_mtime; /* Time of last modification.  */
        public int st_mtime_nsec;
        public int st_ctime; /* Time of last status change.  */
        public int st_ctime_nsec;
        public int  __unused4;
        public int  __unused5;
    }

    public static class Info extends Structure {
        public int permission;  /* File permission.  */
        public int is_dir; /* File type */
        public int uid;   /* User ID of the file's owner.  */
        public int gid;   /* Group ID of the file's group. */
        public int size;  /* Size of file, in bytes.  */
        public int atime; /* Time of last access.  */
        public int ctime; /* Time of last status change.  */
    }

    public static class FetchReply extends Structure {
        public static class ByReference extends FetchReply implements Structure.ByReference {}
        public int state;
        public int buf_len;
    }

    public interface GIGALib extends Library {
        public int gigaInit();
        public void gigaDestroy();
        public int gigaMknod(String path, int mode);
        public int gigaMkdir(String path, int mode);
        public Pointer gigaListStatus(String path, IntByReference numEntries);
        public void gigaStatusInfo(Pointer ptr, Info info);
        public Pointer gigaStatusName(Pointer ptr, IntByReference name_len);
        public Pointer gigaNextStatus(Pointer ptr);
        public int gigaValidStatus(Pointer ptr);
        public void gigaCleanStatusList(Pointer ptr);
        public int gigaCreate(String path, int mode);
        public int gigaRmdir(String path);
        public int gigaGetAttr(String path, Stat stat);
        public int gigaGetInfo(String path, Info info);
        public int gigaOpen(String path, int flags);
        public int gigaUpdateLink(String path, String link);
        public int gigaFetch(String path, byte[] buf, FetchReply.ByReference reply);
        public int gigaGetParentID(int fd);
        public int gigaWriteLink(int fd, String link);
        public int gigaReadAll(int fd, byte[] buf, FetchReply.ByReference reply);
        public int gigaRead(int fd, byte[] buf, int size);
        public int gigaWrite(int fd, byte[] buf, int size);
        public int gigaClose(int fd);
        public int gigaUnlink(String path);
//        public int gigaRename(String src, String dst);
    }

    private GIGALib gigaclient;

    public GTFSImpl() {
       gigaclient =
         (GIGALib) Native.loadLibrary("libgiga_client.so", GIGALib.class);
       gigaclient.gigaInit();
    }

    public void destroy() {
        gigaclient.gigaDestroy();
    }

    public int mkNod(String path, int mode) {
        return gigaclient.gigaMknod(path, mode);
    }

    public int create(String path, int mode) {
        return gigaclient.gigaCreate(path, mode);
    }

    public int mkDir(String path, int mode) {
        return gigaclient.gigaMkdir(path, mode);
    }

    public int rmDir(String path) {
        return gigaclient.gigaRmdir(path);
    }

    public FileStatus[] listStatus(String path) {
        IntByReference iref = new IntByReference();
        Pointer start_ptr = gigaclient.gigaListStatus(path, iref);
        int num_entries = iref.getValue();
        FileStatus[] result = new FileStatus[num_entries];
        Pointer iter_ptr = start_ptr;
        Info info = new Info();
        for (int i = 0; i < num_entries; ++i)
            if (gigaclient.gigaValidStatus(iter_ptr) > 0) {
                gigaclient.gigaStatusInfo(iter_ptr, info);
                Pointer name = gigaclient.gigaStatusName(iter_ptr, iref);
                Path entry_name = new Path(name.getString(0));
                result[i] = new FileStatus(info.size, info.is_dir>0, 3, 1<<26,
                        info.ctime, info.atime,
                        FsPermission.createImmutable((short) info.permission),
                        Integer.toString(info.uid), Integer.toString(info.gid),
                        entry_name);
                iter_ptr = gigaclient.gigaNextStatus(iter_ptr);
            }
       gigaclient.gigaCleanStatusList(start_ptr);
    }

    public int getAttr(String path, Stat stat) {
        return gigaclient.gigaGetAttr(path, stat);
    }

    public int getInfo(String path, Info info) {
        return gigaclient.gigaGetInfo(path, info);
    }

    public int open(String path, int flags) {
        return gigaclient.gigaOpen(path, flags);
    }

    public int fetch(String path, byte[] buf, FetchReply.ByReference reply) {
        return gigaclient.gigaFetch(path, buf, reply);
    }

    public int getParentID(int fd) {
        return gigaclient.gigaGetParentID(fd);
    }

    public int readall(int fd, byte[] buf, FetchReply.ByReference reply) {
        return gigaclient.gigaReadAll(fd, buf, reply);
    }
    public int updatelink(String path, String link) {
        return gigaclient.gigaUpdateLink(path, link);
    }

    public int writelink(int fd, String link) {
        return gigaclient.gigaWriteLink(fd, link);
    }

    public int read(int fd, byte[] buf, int size) {
        return gigaclient.gigaRead(fd, buf, size);
    }

    public int write(int fd, byte[] buf, int size) {
        return gigaclient.gigaWrite(fd, buf, size);
    }

    public int close(int fd) {
        return gigaclient.gigaClose(fd);
    }

    public int unlink(String path) {
        return gigaclient.gigaUnlink(path);
    }
}
