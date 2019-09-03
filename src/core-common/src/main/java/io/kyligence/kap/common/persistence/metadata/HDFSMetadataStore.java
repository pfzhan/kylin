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

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HDFSMetadataStore extends MetadataStore {

    public static final String HDFS_SCHEME = "hdfs";
    private static final String COMPRESSED_FILE = "metadata.zip";

    private final Path rootPath;
    private final FileSystem fs;

    private enum Type {
        DIR, ZIP
    }

    private final Type type;

    public HDFSMetadataStore(KylinConfig kylinConfig) throws IOException {
        super(kylinConfig);
        try {
            val storageUrl = kylinConfig.getMetadataUrl();
            Preconditions.checkState(HDFS_SCHEME.equals(storageUrl.getScheme()));
            type = storageUrl.getParameter("zip") != null ? Type.ZIP : Type.DIR;
            String path = storageUrl.getParameter("path");
            if (path == null) {
                path = HadoopUtil.getBackupFolder(kylinConfig);
                fs = HadoopUtil.getWorkingFileSystem();
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
                fs = HadoopUtil.getWorkingFileSystem();
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
            if (compressedFilesContains(resPath)) {
                return Sets.newTreeSet(getAllFilePathFromCompressedFiles(resPath));
            }
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
        if (getCompressedFiles().containsKey(resPath)) {
            return getCompressedFiles().get(resPath);
        }
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

    @Override
    public void dump(ResourceStore store, String rootPath) throws Exception {
        if (type == Type.DIR) {
            super.dump(store, rootPath);
            return;
        }
        val resources = store.listResourcesRecursively(rootPath);
        if (resources == null || resources.isEmpty()) {
            log.info("there is no resources in rootPath ({}),please check the rootPath.", rootPath);
            return;
        }
        val compressedFile = new Path(this.rootPath, COMPRESSED_FILE);
        try (FSDataOutputStream out = fs.create(compressedFile, true);
                ZipOutputStream zipOut = new ZipOutputStream(new CheckedOutputStream(out, new CRC32()))) {
            for (String resPath : resources) {
                val raw = store.getResource(resPath);
                compress(zipOut, raw);
            }
        } catch (Exception e) {
            throw new IOException("Put compressed resource fail", e);
        }
    }

    @Override
    public void restore(ResourceStore store) throws IOException {
        val compressedFile = getRealHDFSPath(COMPRESSED_FILE);
        if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
            super.restore(store);
            return;
        }
        log.info("restore from metadata.zip");
        getCompressedFiles().forEach((name, raw) -> store.putResourceWithoutCheck(name, raw.getByteSource(),
                raw.getTimestamp(), raw.getMvcc()));
    }

    private void compress(ZipOutputStream out, RawResource raw) throws IOException {
        ZipEntry entry = new ZipEntry(raw.getResPath());
        entry.setTime(raw.getTimestamp());
        out.putNextEntry(entry);
        IOUtils.copy(raw.getByteSource().openStream(), out);
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

    private List<String> getAllFilePathFromCompressedFiles(String path) {
        if (File.separator.equals(path)) {
            return Lists.newArrayList(getCompressedFiles().keySet());
        }
        return getCompressedFiles().keySet().stream()
                .filter(file -> file.startsWith(path + File.separator) || file.equals(path))
                .map(file -> file.substring(path.length())).collect(Collectors.toList());
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

    @Getter(lazy = true)
    private final Map<String, RawResource> compressedFiles = getFilesFromCompressedFile();

    private Map<String, RawResource> getFilesFromCompressedFile() {
        val res = Maps.<String, RawResource> newHashMap();
        val compressedFile = getRealHDFSPath(COMPRESSED_FILE);
        try {
            if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
                return Maps.newHashMap();
            }
        } catch (IOException ignored) {
        }
        try (FSDataInputStream in = fs.open(compressedFile); ZipInputStream zipIn = new ZipInputStream(in);) {
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                if (!zipEntry.getName().startsWith("/")) {
                    continue;
                }
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipIn));
                long t = zipEntry.getTime();
                val raw = new RawResource(zipEntry.getName(), bs, t, 0);
                res.put(zipEntry.getName(), raw);
            }
            return res;
        } catch (Exception e) {
            log.warn("get file from compressed file error", e);
        }
        return Maps.newHashMap();
    }

    private boolean compressedFilesContains(String path) {
        if (File.separator.equals(path)) {
            return !getCompressedFiles().isEmpty();
        }
        return getCompressedFiles().keySet().stream()
                .anyMatch(file -> file.startsWith(path + "/") || file.equals(path));
    }
}
