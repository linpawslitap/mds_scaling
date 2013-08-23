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
import org.apache.hadoop.fs.FSDataInputStream;

public class GTFSInputStream extends FSInputStream {

    private byte[] buf;
    private long buf_len;
    private long pos;
    private FSDataInputStream dfs_in;

    public GTFSInputStream(byte[] buf, long buf_len) {
        this.buf = buf;
        this.buf_len = buf_len;
        this.pos = 0;
        this.dfs_in = null;
    }

    public GTFSInputStream(FSDataInputStream dfs_in) {
        this.dfs_in = dfs_in;
        this.buf = null;
        this.pos = 0;
        this.buf_len = 0;
    }
    public long getPos() throws IOException {
        if (dfs_in != null) {
            return dfs_in.getPos();
        }
        return pos;
    }

    public synchronized void seek(long targetPos) throws IOException {
        if (dfs_in != null) {
            dfs_in.seek(pos);
        }
        pos = targetPos;
    }

    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    public int available() throws IOException {
        if (dfs_in != null) {
            return dfs_in.available();
        } else {
            if (buf_len > pos) {
                return (int) (buf_len - pos);
            } else {
                return 0;
            }
        }
    }

    public synchronized int read() throws IOException {
        if (dfs_in != null) {
            return dfs_in.read();
        }
        if (pos >= buf_len) {
            throw new IOException("End of File");
        }
        pos = pos + 1;
        return buf[(int)pos - 1];
    }

    public synchronized int read(byte b[], int off, int len) throws IOException {
        if (dfs_in != null) {
            return dfs_in.read(b, off, len);
        }
        if (pos >= buf_len) {
            return -1;
        }
        int res = Math.min(len, (int) (buf_len-pos));
        System.arraycopy(buf, (int) pos, b, off, res);
        pos += res;
        return res;
    }

    public synchronized void close() throws IOException {
        if (dfs_in != null) {
            dfs_in.close();
        } else {
            buf_len = 0;
            buf = null;
        }
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
