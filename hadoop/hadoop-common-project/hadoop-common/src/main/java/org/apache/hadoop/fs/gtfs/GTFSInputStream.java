package org.apache.hadoop.fs.gtfs;

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 8/14/13
 * Time: 12:14 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSInputStream;

public class GTFSInputStream extends FSInputStream {


    public long getPos() throws IOException {
        return 0;
    }

    public synchronized int available() throws IOException {
        return 2;
    }

    public synchronized void seek(long targetPos) throws IOException {

    }

    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    public synchronized int read() throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        byte b[] = new byte[1];
        int res = read(b, 0, 1);
        if (res == 1) {
          if (statistics != null) {
            statistics.incrementBytesRead(1);
          }
          return b[0] & 0xff;
        }
        */
        return -1;
    }

    public synchronized int read(byte b[], int off, int len) throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        int res;

        res = kfsChannel.read(ByteBuffer.wrap(b, off, len));
        // Use -1 to signify EOF
        if (res == 0)
            return -1;
        if (statistics != null) {
          statistics.incrementBytesRead(res);
        }
        return res;
        */
        return -1;
    }

    public synchronized void close() throws IOException {

    }

    public boolean markSupported() {
        return false;
    }

    public void mark(int readLimit) {
        // Do nothing
    }

    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
