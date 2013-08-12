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
 * Implements the Hadoop FS interfaces to allow applications to store
 *files in Kosmos File System (KFS).
 */

package org.apache.hadoop.fs.gtfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * A FileSystem backed by GIGA+TableFS FileSystem.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class GTFileSystem extends FileSystem {

    private URI uri;
    private Path workingDir = new Path("/");

    public GTFileSystem() {
    }

    @Override
    public URI getUri() {
	return uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      try {
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name")
                                   ).makeQualified(this);
        setConf(conf);

      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Unable to initialize GTFS");
        System.exit(-1);
      }
    }

    @Override
    public Path getWorkingDirectory() {
    	return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
    	workingDir = makeAbsolute(dir);
    }

    private Path makeAbsolute(Path path) {
	if (path.isAbsolute()) {
	    return path;
	}
	return new Path(workingDir, path);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission
        ) throws IOException {
	    Path absolute = makeAbsolute(path);
    	int res = 0;
    	return res == 0;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        /*
        if(!kfsImpl.exists(srep))
          throw new FileNotFoundException("File " + path + " does not exist.");

        if (kfsImpl.isFile(srep))
                return new FileStatus[] { getFileStatus(path) } ;

        return kfsImpl.readdirplus(absolute);
        */
        return null;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
        String srep = absolute.toUri().getPath();
        /*
        if (!kfsImpl.exists(srep)) {
          throw new FileNotFoundException("File " + path + " does not exist.");
        }
        if (kfsImpl.isDirectory(srep)) {
            // System.out.println("Status of path: " + path + " is dir");
            return new FileStatus(0, true, 1, 0, kfsImpl.getModificationTime(srep), 
                                  path.makeQualified(this));
        } else {
            // System.out.println("Status of path: " + path + " is file");
            return new FileStatus(kfsImpl.filesize(srep), false, 
                                  kfsImpl.getReplication(srep),
                                  getDefaultBlockSize(),
                                  kfsImpl.getModificationTime(srep),
                                  path.makeQualified(this));
        }
        */
        return null;
    }
    /**
     * Set permission of a path.
     * @param p
     * @param permission
     */
    public void setPermission(Path p, FsPermission permission
    ) throws IOException {
    }

    /**
     * Set owner of a path (i.e. a file or a directory).
     * The parameters username and groupname cannot both be null.
     * @param p The path
     * @param username If it is null, the original username remains unchanged.
     * @param groupname If it is null, the original groupname remains unchanged.
     */
    @Override
    public void setOwner(Path p, String username, String groupname
    ) throws IOException {
    }

    /**
     * Set access time of a file
     * @param p The path
     * @param mtime Set the modification time of this file.
     *              The number of milliseconds since Jan 1, 1970.
     *              A value of -1 means that this call should not set modification time.
     * @param atime Set the access time of this file.
     *              The number of milliseconds since Jan 1, 1970.
     *              A value of -1 means that this call should not set access time.
     */
    @Override
    public void setTimes(Path p, long mtime, long atime
    ) throws IOException {
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream create(Path file, FsPermission permission,
                                     boolean overwrite, int bufferSize,
				     short replication, long blockSize, Progressable progress)
	throws IOException {

        return null;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if (!exists(path))
            throw new IOException("File does not exist: " + path);
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return true;
    }

    // recursively delete the directory and its contents
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return true;
    }
    
    @Override
    public short getDefaultReplication() {
    	return 3;
    }

    @Override
    public boolean setReplication(Path path, short replication)
	throws IOException {
        return true;
    }

    // 64MB is the KFS block size

    @Override
    public long getDefaultBlockSize() {
    	return 1 << 26;
    }

    @Deprecated            
    public void lock(Path path, boolean shared) throws IOException {

    }

    @Deprecated            
    public void release(Path path) throws IOException {

    }

    /**
     * Return null if the file doesn't exist; otherwise, get the
     * locations of the various chunks of the file file from KFS.
     */
    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
        long len) throws IOException {

        if (file == null) {
            return null;
        }
        return null;
    }
}
