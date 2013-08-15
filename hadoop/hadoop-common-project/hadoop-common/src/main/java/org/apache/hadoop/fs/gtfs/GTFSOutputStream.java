/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Implements the Hadoop FSOutputStream interfaces to allow applications to write to
 * files in Kosmos File System (KFS).
 */

package org.apache.hadoop.fs.gtfs;

import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class GTFSOutputStream extends OutputStream {

    private FileSystem hdfs;
    private Path f;
    private FsPermission permission;
    private boolean overwrite;
    private int bufferSize;
    private short replication;
    private long blockSize;
    private boolean append;
    private Progressable progressReporter;

    static private int threshold;

    public GTFSOutputStream(Path f,
                            FsPermission permission,
                            boolean overwrite,
                            int bufferSize,
                            short replication,
                            long blockSize,
                            boolean append,
                            FileSystem fs,
                            Progressable progress) {
        this.f = f;
        this.permission = permission;
        this.overwrite = overwrite;
        this.bufferSize = bufferSize;
        this.replication = replication;
        this.blockSize = blockSize;
        this.append = append;
        this.progressReporter = progress;
    }

    public long getPos() throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        */
    }

    public void write(int v) throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        */

    }

    public void write(byte b[], int off, int len) throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        */
        // touch the progress before going into KFS since the call can block
        progressReporter.progress();
        //kfsChannel.write(ByteBuffer.wrap(b, off, len));
    }

    public void flush() throws IOException {

        progressReporter.progress();
    }

    public synchronized void close() throws IOException {

    }
}
