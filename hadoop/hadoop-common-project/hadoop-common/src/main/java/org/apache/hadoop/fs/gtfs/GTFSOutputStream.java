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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class GTFSOutputStream extends OutputStream {

    private String path;
    private GTFSImpl gtfs_impl;
    private Progressable progressReporter;
    private long pos;

    public GTFSOutputStream(GTFSImpl gtfs_impl, String path, short replication,
                            boolean append, Progressable prog) {
        this.path = path;

        /*
        if ((append) && (kfsAccess.kfs_isFile(path)))
                this.kfsChannel = kfsAccess.kfs_append(path);
        else
                this.kfsChannel = kfsAccess.kfs_create(path, replication);
        */

        this.progressReporter = prog;
    }

    public long getPos() throws IOException {
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        */
        return pos;
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
        /*
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        */
        // touch the progress before going into KFS since the call can block
        progressReporter.progress();
        //kfsChannel.sync();
    }

    public synchronized void close() throws IOException {
        /*
        if (kfsChannel == null) {
            return;
        }
        flush();
        kfsChannel.close();
        kfsChannel = null;
        */
    }
}
