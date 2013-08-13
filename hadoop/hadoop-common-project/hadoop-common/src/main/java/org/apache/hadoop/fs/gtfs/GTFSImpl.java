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
        public short permission;  /* File permission.  */
        public boolean is_dir; /* File type */
        public boolean __unused;
        public int uid;   /* User ID of the file's owner.  */
        public int gid;   /* Group ID of the file's group. */
        public int size;  /* Size of file, in bytes.  */
        public int atime; /* Time of last access.  */
        public int ctime; /* Time of last status change.  */
    }

    public interface GIGALib extends Library {
        public int gigaInit();
        public void gigaDestroy();
        public int gigaMknod(String path, int mode, int dev);
        public int gigaMkdir(String path, int mode);
        public int gigaRmdir(String path);
        public int gigaGetAttr(String path, Stat stat);
        public int gigaGetInfo(String path, Info info);

        public int gigaOpen(String path, int flags);
        public int gigaRead(int fd, String buf, int size);
        public int gigaWrite(int fd, String buf, int size);
        public int gigaClose(int fd);
        public int gigaUnlink(String path);
//        public int gigaRename(String src, String dst);
    }

    private GIGALib gigaclient;

    public GTFSImpl() {
       gigaclient =
         (GIGAlib) Native.loadLibrary("libgiga_client.so", CTest.class);
       gigaclient.gigaInit();
    }

    public void destroy() {
        gigaclient.gigaDestroy();
    }

    public int mkNod(String path, int mode, int dev) {
        return gigaclient.gigaMknod(path, mode, dev);
    }

    public int mkDir(String path, int mode) {
        return gigaclient.gigaMkdir(path, mode);
    }

    public int rmDir(String path) {
        return gigaclient.gigaRmdir(path);
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

    public int read(int fd, String buf, int size) {
        return gigaclient.gigaRead(fd, buf, size);
    }

    public int write(int fd, String buf, int size) {
        return gigaclient.gigaWrite(fd, buf, size);
    }

    public int close(int fd) {
        return gigaclient.gigaClose(fd);
    }

    public int unlink(String path) {
        return gigaclient.gigaUnlink(path);
    }
}