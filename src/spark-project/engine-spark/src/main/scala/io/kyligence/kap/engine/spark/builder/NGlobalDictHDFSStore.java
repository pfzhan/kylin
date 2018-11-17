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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.engine.spark.builder;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeSet;

public class NGlobalDictHDFSStore extends NGlobalDictStore {

    static final Logger logger = LoggerFactory.getLogger(NGlobalDictHDFSStore.class);

    private static final String VERSION_PREFIX = "version_";
    private static final String DICT_METADATA_NAME = "meta";
    private static final String DICT_CURR_PREFIX = "CURR_";
    private static final String DICT_PREV_PREFIX = "PREV_";

    private final Path basePath;
    private final FileSystem fileSystem;

    public NGlobalDictHDFSStore(String baseDir) throws IOException {
        this.basePath = new Path(baseDir);
        this.fileSystem = HadoopUtil.getFileSystem(baseDir);
    }

    @Override
    void prepareForWrite(String workingDir) throws IOException {
        if (!fileSystem.exists(basePath)) {
            logger.info("Global dict store at {} doesn't exist, create a new one", basePath);
            fileSystem.mkdirs(basePath);
        }

        logger.trace("Prepare to write Global dict store at {}", workingDir);
        Path working = new Path(workingDir);

        if (fileSystem.exists(working)) {
            fileSystem.delete(working, true);
            logger.trace("Working directory {} exits, delete it first", working);
        }

        fileSystem.mkdirs(working);
    }

    @Override
    public Long[] listAllVersions() throws IOException {
        if (!fileSystem.exists(basePath)) {
            return new Long[0];
        }

        FileStatus[] versionDirs = fileSystem.listStatus(basePath, path -> path.getName().startsWith(VERSION_PREFIX));
        TreeSet<Long> versions = new TreeSet<>();
        for (FileStatus versionDir : versionDirs) {
            Path path = versionDir.getPath();
            versions.add(Long.parseLong(path.getName().substring(VERSION_PREFIX.length())));
        }
        return versions.toArray(new Long[versions.size()]);
    }

    @Override
    public Path getVersionDir(long version) {
        return new Path(basePath, VERSION_PREFIX + version);
    }

    @Override
    public NGlobalDictMetadata getMetadata(long version) throws IOException {
        Path versionDir = getVersionDir(version);
        FileStatus[] metaFiles = fileSystem.listStatus(versionDir, path -> path.getName().startsWith(DICT_METADATA_NAME));

        if (metaFiles.length == 0) {
            return null;
        }

        String metaFile = metaFiles[0].getPath().getName();
        Path metaPath = new Path(versionDir, metaFile);
        if (!fileSystem.exists(metaPath)) return null;

        NGlobalDictMetadata metadata;
        try (FSDataInputStream is = fileSystem.open(metaPath)) {
            int bucketSize = is.readInt();
            int[] offset = new int[bucketSize];
            int dictCount = is.readInt();
            for (int i = 0; i < bucketSize; i++) {
                offset[i] = is.readInt();
            }
            metadata = new NGlobalDictMetadata(bucketSize, offset, dictCount);
        }

        return metadata;
    }

    @Override
    public Object2IntMap<String> getBucketDict(long version, NGlobalDictMetadata metadata, int bucketId) throws IOException {
        Object2IntMap<String> object2IntMap = new Object2IntOpenHashMap<>();
        Path versionDir = getVersionDir(version);
        FileStatus[] dictCurrFiles = fileSystem.listStatus(versionDir, path -> path.getName().endsWith("_" + String.valueOf(bucketId)));

        for (FileStatus file : dictCurrFiles) {
            if (file.getPath().getName().startsWith(DICT_CURR_PREFIX)) {
                object2IntMap.putAll(getBucketDict(file.getPath(), metadata.getPointOffset(bucketId)));
            }
            if (file.getPath().getName().startsWith(DICT_PREV_PREFIX)) {
                object2IntMap.putAll(getBucketDict(file.getPath(), 0));
            }
        }

        return object2IntMap;
    }

    private Object2IntMap<String> getBucketDict(Path dictPath, int offset) throws IOException {
        Object2IntMap<String> object2IntMap = new Object2IntOpenHashMap<>();
        try (FSDataInputStream is = fileSystem.open(dictPath)) {
            int elementCnt = is.readInt();
            for (int i = 0; i < elementCnt; i++) {
                int value = is.readInt();
                int bytesLength = is.readInt();
                byte[] bytes = new byte[bytesLength];
                IOUtils.readFully(is, bytes, 0, bytes.length);
                object2IntMap.put(new String(bytes), value + offset);
            }
        }

        return object2IntMap;
    }

    @Override
    public void writeBucketCurrDict(String workingPath, int bucketId, Object2IntMap<String> openHashMap) throws IOException {
        Path dictPath = new Path(workingPath, DICT_CURR_PREFIX + bucketId);
        writeBucketDict(dictPath, openHashMap);
    }

    @Override
    public void writeBucketPrevDict(String workingPath, int bucketId, Object2IntMap<String> openHashMap) throws IOException {
        Path dictPath = new Path(workingPath, DICT_PREV_PREFIX + bucketId);
        writeBucketDict(dictPath, openHashMap);
    }

    private void writeBucketDict(Path dictPath, Object2IntMap<String> openHashMap) throws IOException {
        if (fileSystem.exists(dictPath)) {
            fileSystem.delete(dictPath, true);
        }
        logger.info("write dict path: {}", dictPath);
        try (FSDataOutputStream dos = fileSystem.create(dictPath)) {
            dos.writeInt(openHashMap.size());
            for (Object2IntMap.Entry<String> entry : openHashMap.object2IntEntrySet()) {
                dos.writeInt(entry.getIntValue());
                dos.writeInt(entry.getKey().length());
                dos.writeBytes(entry.getKey());
            }
            dos.flush();
        }

        logger.info("write dict path: {} , dict num: {} success", dictPath, openHashMap.size());
    }

    public void writeMetaDict(String workingPath) throws IOException {
        Path metaPath = new Path(workingPath, DICT_METADATA_NAME);
        if (fileSystem.exists(metaPath)) {
            fileSystem.delete(metaPath, true);
        }
        logger.info("write dict meta path: {}", metaPath);

        Path workPath = new Path(workingPath);
        FileStatus[] dictPrevFiles = fileSystem.listStatus(workPath, path -> StringUtils.contains(path.getName(), DICT_PREV_PREFIX));
        FileStatus[] dictCurrFiles = fileSystem.listStatus(workPath, path -> StringUtils.contains(path.getName(), DICT_CURR_PREFIX));

        int dictCount = 1;
        for (FileStatus fileStatus : dictPrevFiles) {
            try (FSDataInputStream is = fileSystem.open(fileStatus.getPath())) {
                dictCount = dictCount + is.readInt();
            }
        }

        int[] offset = new int[dictCurrFiles.length];

        try (FSDataOutputStream dos = fileSystem.create(metaPath)) {
            dos.writeInt(dictCurrFiles.length);
            dos.writeInt(dictCount);
            for (FileStatus fileStatus : dictCurrFiles) {
                try (FSDataInputStream is = fileSystem.open(fileStatus.getPath())) {
                    String bucketId = fileStatus.getPath().getName().replaceAll(DICT_CURR_PREFIX, "");
                    offset[Integer.parseInt(bucketId)] = is.readInt();
                }
            }

            for (int i : offset) {
                dos.writeInt(dictCount);
                dictCount = dictCount + i;
            }
            dos.flush();
        }
    }

    @Override
    public void commit(String workingDir, int maxVersions, int versionTTL) throws IOException {
        Path workingPath = new Path(workingDir);
        // copy working dir to newVersion dir
        Path newVersionPath = new Path(basePath, VERSION_PREFIX + System.currentTimeMillis());
        fileSystem.rename(workingPath, newVersionPath);
        logger.info("commit from {} to {}", workingPath, newVersionPath);
        cleanUp(maxVersions, versionTTL);
    }

    // Check versions count, delete expired versions
    private void cleanUp(int maxVersions, int versionTTL) throws IOException {
        long timestamp = System.currentTimeMillis();
        Long[] versions = listAllVersions();
        for (int i = 0; i < versions.length - maxVersions; i++) {
            if (versions[i] + versionTTL < timestamp) {
                fileSystem.delete(getVersionDir(versions[i]), true);
            }
        }
    }
}
