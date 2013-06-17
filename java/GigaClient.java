
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

public class GigaClient {
    public static class Stat extends Structure {
       public int st_dev;   /* Device.  */
       public int st_ino;   /* File serial number.  */
       public int st_mode;  /* File mode.  */
       public int st_nlink; /* Link count.  */
       public int  st_uid;   /* User ID of the file's owner.  */
       public int  st_gid;   /* Group ID of the file's group. */
       public int st_rdev;  /* Device number, if device.  */
       public int __pad1;
       public int st_size;  /* Size of file, in bytes.  */
       public int   st_blksize; /* Optimal block size for I/O.  */
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
       public int mode;  /* File mode.  */
       public int uid;   /* User ID of the file's owner.  */
       public int gid;   /* Group ID of the file's group. */
       public int size;  /* Size of file, in bytes.  */
       public int atime; /* Time of last access.  */
       public int mtime; /* Time of last modification.  */
       public int ctime; /* Time of last status change.  */
    }

    public interface CTest extends Library {
        public int gigaInit();
        public void gigaDestroy();
        public int gigaMknod(String path, int mode, int dev);
        public int gigaMkdir(String path, int mode);
        public int gigaRmdir(String path);
        public int gigaGetattr(String path, Stat stat);
        public int gigaGetinfo(String path, Info info);
    }

    static public void main(String argv[]) {
       CTest ctest =
         (CTest) Native.loadLibrary("libgiga_client.so", CTest.class);
       ctest.gigaInit();
       ctest.gigaMknod("/test", 777, 0);
       Info stat = new Info();
       ctest.gigaGetinfo("/test", stat);
       System.out.println("uid:"+stat.uid+"size:"+stat.size);
       ctest.gigaDestroy();
    }
}
