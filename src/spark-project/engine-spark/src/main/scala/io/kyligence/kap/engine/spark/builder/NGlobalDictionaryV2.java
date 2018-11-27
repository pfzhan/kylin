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
package io.kyligence.kap.engine.spark.builder;

import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class NGlobalDictionaryV2 implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionaryV2.class);

    private String baseDir;
    private NGlobalDictMetadata metadata;

    private final static String WORKING_DIR = "working";

    private String sourceTable;
    private String sourceColumn;

    public String getResourceDir() {
        return ResourceStore.GLOBAL_DICT_RESOURCE_ROOT + "/" + sourceTable + "/" + sourceColumn + "/";
    }

    private String getWorkingDir() {
        return baseDir + WORKING_DIR;
    }

    public NGlobalDictionaryV2(String sourceTable, String sourceColumn, String baseDir) throws IOException {
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.baseDir = baseDir + getResourceDir();
        this.metadata = getMetaDict();
    }

    public NBucketDictionary createBucketDictionary(int bucketId) throws IOException {
        if (null == metadata) {
            metadata = getMetaDict();
        }
        return new NBucketDictionary(baseDir, getWorkingDir(), bucketId, metadata);
    }

    public void prepareWrite() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.prepareForWrite(getWorkingDir());
    }

    public void writeMetaDict(int maxVersions, int versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.writeMetaDict(getWorkingDir());
        commit(maxVersions, versionTTL);
    }

    public NGlobalDictMetadata getMetaDict() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        NGlobalDictMetadata metadata;
        Long[] versions = globalDictStore.listAllVersions();

        if (versions.length == 0) {
            return null;
        } else {
            metadata = globalDictStore.getMetadata(versions[versions.length - 1]);
        }
        return metadata;
    }

    public int getBucketSize(int defaultSize) {
        int bucketPartitionSize;
        if (metadata == null) {
            bucketPartitionSize = defaultSize;
        } else {
            bucketPartitionSize = metadata.getBucketSize();
        }

        return bucketPartitionSize;
    }

    private void commit(int maxVersions, int versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.commit(getWorkingDir(), maxVersions, versionTTL);
    }

    private static NGlobalDictStore getResourceStore(String baseDir) throws IOException {
        return new NGlobalDictHDFSStore(baseDir);
    }
}
