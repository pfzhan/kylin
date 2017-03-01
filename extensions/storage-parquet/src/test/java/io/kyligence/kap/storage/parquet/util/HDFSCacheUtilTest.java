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
package io.kyligence.kap.storage.parquet.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

@Ignore
public class HDFSCacheUtilTest extends HBaseMetadataTestCase {

    private static final String TEST_POOL_NAME = "test_pool";

    private static final String CACHE_DIR = "hdfs://kylin/test_hdfs_cache";

    private static final String CACHE_DIR_SUB = CACHE_DIR + "/sub";

    private static final int RETRY_TIMES = 5;

    private DistributedFileSystem dfs;

    private HDFSCacheUtil hdfsCacheUtil;

    @Before
    public void before() throws Exception {
        super.createTestMetadata();
        hdfsCacheUtil = new HDFSCacheUtil();
        dfs = hdfsCacheUtil.getDfs();
        dfs.mkdirs(new Path(CACHE_DIR));
        dfs.mkdirs(new Path(CACHE_DIR_SUB));
        writeSthToFile(new Path(CACHE_DIR, "file1"), dfs);
        writeSthToFile(new Path(CACHE_DIR_SUB, "file2"), dfs);
        writeSthToFile(new Path(CACHE_DIR_SUB, "file3"), dfs);
        if (hdfsCacheUtil.existPool(dfs, TEST_POOL_NAME))
            hdfsCacheUtil.removePool(TEST_POOL_NAME, dfs);
        hdfsCacheUtil.createPool(TEST_POOL_NAME, dfs);
    }

    @After
    public void after() throws Exception {
        dfs.delete(new Path(CACHE_DIR), true);
        hdfsCacheUtil.removePool(TEST_POOL_NAME, dfs);
        super.cleanupTestMetadata();
    }

    @Test
    public void testCacheFiles() throws Exception {
        hdfsCacheUtil.cacheFilesRecursivly(new Path(CACHE_DIR), dfs, TEST_POOL_NAME);
        int t = RETRY_TIMES;
        while (t > 0) {
            t--;
            CachePoolEntry entry = hdfsCacheUtil.getPoolEntry(dfs, TEST_POOL_NAME);
            CachePoolStats stats = entry.getStats();
            if (stats.getBytesCached() == stats.getBytesNeeded()) {
                return;
            }
            Thread.currentThread().sleep(10000);
        }
        fail("Files not fully cached");
    }

    private void writeSthToFile(Path file, FileSystem fs) throws IOException {
        FSDataOutputStream os = null;
        try {
            os = fs.create(file);
            os.writeBytes("something");
        } finally {
            IOUtils.closeQuietly(os);
        }

    }
}
