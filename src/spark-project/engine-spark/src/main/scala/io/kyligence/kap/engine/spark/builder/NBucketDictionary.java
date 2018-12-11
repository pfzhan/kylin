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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class NBucketDictionary {

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionaryV2.class);

    private String workingDir;

    private Object2IntMap<String> prevObject2IntMap;
    private Object2IntMap<String> currObject2IntMap;

    NBucketDictionary(String baseDir, String workingDir, int bucketId, NGlobalDictMetadata metadata)
            throws IOException {
        this.workingDir = workingDir;
        final NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(baseDir);
        Long[] versions = globalDictStore.listAllVersions();
        if (versions.length == 0) {
            this.prevObject2IntMap = new Object2IntOpenHashMap<>();
        } else {
            this.prevObject2IntMap = globalDictStore.getBucketDict(versions[versions.length - 1], metadata, bucketId);
        }
        this.currObject2IntMap = new Object2IntOpenHashMap<>();
    }

    public void addValue(String value) {
        if (null == value) {
            return;
        }
        if (prevObject2IntMap.containsKey(value)) {
            return;
        }
        currObject2IntMap.put(value, currObject2IntMap.size() + 1);
    }

    public int encode(Object value) {
        return prevObject2IntMap.getInt(value.toString());
    }

    public void saveBucketDict(int bucketId) throws IOException {
        writeBucketCurrDict(bucketId);
        writeBucketPrevDict(bucketId);
    }

    private void writeBucketPrevDict(int bucketId) throws IOException {
        if (prevObject2IntMap.isEmpty())
            return;
        NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(workingDir);
        globalDictStore.writeBucketPrevDict(workingDir, bucketId, prevObject2IntMap);
    }

    private void writeBucketCurrDict(int bucketId) throws IOException {
        NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(workingDir);
        globalDictStore.writeBucketCurrDict(workingDir, bucketId, currObject2IntMap);
    }

    public Object2IntMap<String> getPrevObject2IntMap() {
        return prevObject2IntMap;
    }

}
