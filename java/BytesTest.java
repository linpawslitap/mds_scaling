import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

public class BytesTest {

    public interface CTest extends Library {
      public int getBytes(byte[] buf);
      public int getBytes(int buf);

    }

    static public void main(String argv[]) {
       CTest ctest =
         (CTest) Native.loadLibrary("bytes_test.so", CTest.class);
       byte[] buf = new byte[1024];
       int num_bytes = ctest.getBytes(buf);
       System.out.println(num_bytes);
       for (int i = 0; i < 1024; ++i)
          System.out.print((char)buf[i]);
       System.out.println();
    }
}
