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
package org.apache.kylin.common.persistence.image;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.HadoopUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HDFSImageStore extends ImageStore {

    public static final String HDFS_SCHEME = "hdfs";

    private final Path rootPath;
    private final FileSystem fs;

    public HDFSImageStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        try {
            val storageUrl = kylinConfig.getMetadataUrl();
            Preconditions.checkState(HDFS_SCHEME.equals(storageUrl.getScheme()));

            String path = storageUrl.getParameter("path");
            if (path == null) {
                path = HadoopUtil.getLatestImagePath(kylinConfig);
            }

            fs = HadoopUtil.getFileSystem(path);
            rootPath = new Path(path);
            if (!fs.exists(rootPath)) {
                log.warn("Path not exist in HDFS, create it: {}", path);
                createMetaFolder(rootPath);
            }

            log.info("hdfs root path : {}", rootPath.toString());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void saveFile(String path, ByteSource bs, long ts) throws Exception {
        log.trace("res path : {}", path);
        Path p = getRealHDFSPath(path);
        FSDataOutputStream out = null;
        try {
            out = fs.create(p, true);
            IOUtils.copy(bs.openStream(), out);
            fs.setTimes(p, ts, ts);
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    @Override
    protected NavigableSet<String> listFiles(String rootFilepath) {
        try {
            Path p = getRealHDFSPath(rootFilepath);
            if (!fs.exists(p) || !fs.isDirectory(p)) {
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
    protected RawResource loadFile(String filepath) throws IOException {
        Path p = getRealHDFSPath(filepath);
        if (fs.exists(p) && fs.isFile(p)) {
            if (fs.getFileStatus(p).getLen() == 0) {
                log.warn("Zero length file: " + p.toString());
            }
            val bs = ByteStreams.asByteSource(IOUtils.toByteArray(fs.open(p)));
            long t = fs.getFileStatus(p).getModificationTime();
            return new RawResource(filepath, bs, t, getMvcc(bs));
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

    private String createMetaStoreUUID() {
        return String.valueOf(UUID.randomUUID());
    }
}
