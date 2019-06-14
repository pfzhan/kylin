/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.common.persistence.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.HadoopUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Stream;

@Slf4j
public class HDFSMetadataStore extends MetadataStore {

    public static final String HDFS_SCHEME = "hdfs";

    private final Path rootPath;
    private final FileSystem fs;

    public HDFSMetadataStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        try {
            val storageUrl = kylinConfig.getMetadataUrl();
            Preconditions.checkState(HDFS_SCHEME.equals(storageUrl.getScheme()));

            String path = storageUrl.getParameter("path");
            if (path == null) {
                path = HadoopUtil.getBackupFolder(kylinConfig);
                fs = HadoopUtil.getFileSystem(path);
                if (!fs.exists(new Path(path))) {
                    fs.mkdirs(new Path(path));
                }
                rootPath = Stream.of(fs.listStatus(new Path(path)))
                        .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                        .orElse(new Path(path + "/backup_0/"));
                if (!fs.exists(rootPath)) {
                    fs.mkdirs(rootPath);
                }
            } else {
                fs = HadoopUtil.getFileSystem(path);
                rootPath = fs.makeQualified(new Path(path));
            }

            if (!fs.exists(rootPath)) {
                log.warn("Path not exist in HDFS, create it: {}", path);
                createMetaFolder(rootPath);
            }

            log.info("The FileSystem location is {}, hdfs root path : {}", fs.getUri().toString(), rootPath.toString());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void save(String resPath, ByteSource bs, long ts, long mvcc) throws Exception {
        log.trace("res path : {}", resPath);
        Path p = getRealHDFSPath(resPath);
        if (bs == null) {
            fs.delete(p, true);
            return;
        }
        FSDataOutputStream out = null;
        try {
            out = fs.create(p, true);
            IOUtils.copy(bs.openStream(), out);
            fs.setTimes(p, ts, -1);
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    @Override
    public NavigableSet<String> list(String resPath) {
        try {
            Path p = getRealHDFSPath(resPath);
            if (!fs.exists(p) || !fs.isDirectory(p)) {
                log.warn("path {} does not exist in HDFS", resPath);
                return new TreeSet<>();
            }
            TreeSet<String> r;

            r = getAllFilePath(p);
            return r.isEmpty() ? new TreeSet<>() : r;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public RawResource load(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        if (fs.exists(p) && fs.isFile(p)) {
            if (fs.getFileStatus(p).getLen() == 0) {
                log.warn("Zero length file: " + p.toString());
            }
            try (val in = fs.open(p)) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(in));
                long t = fs.getFileStatus(p).getModificationTime();
                return new RawResource(resPath, bs, t, 0);
            }
        } else {
            throw new IOException("path " + p + " not found");
        }
    }

    private Path getRealHDFSPath(String resourcePath) {
        if (resourcePath.equals("/"))
            return this.rootPath;
        if (resourcePath.startsWith("/") && resourcePath.length() > 1)
            resourcePath = resourcePath.substring(1);
        return new Path(this.rootPath, resourcePath);
    }

    TreeSet<String> getAllFilePath(Path filePath) {
        try {
            TreeSet<String> fileList = new TreeSet<>();
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, true);
            while (it.hasNext()) {
                fileList.add(it.next().getPath().toString().replace(filePath.toString(), ""));
            }
            return fileList;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void createMetaFolder(Path metaDirName) {
        //create hdfs meta path
        try {
            if (!fs.exists(metaDirName)) {
                fs.mkdirs(metaDirName);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
