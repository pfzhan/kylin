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

package io.kyligence.kap.metadata.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.query.KylinTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.filter.MassinFilterManager;

public class ITMassinFilterManagerTest extends KylinTestBase {
    private MassinFilterManager manager;
    private static final String[] testStrings = new String[] { "Apple", "Banana", "Kylin" };

    @BeforeClass
    public static void setupAll() throws Exception {
        KylinTestBase.setupAll();
    }

    @Before
    public void setup() {
        manager = MassinFilterManager.getInstance(config);
    }

    @Test
    public void testSaveHBase() throws IOException {
        try {
            manager.save(Functions.FilterTableType.HBASE_TABLE, null);
        } catch (RuntimeException e) {
            // This exception is expected.
            Assert.assertTrue(true);
            return;
        }
        Assert.assertTrue(false);
    }

    @Test
    public void testLoadHBase() throws IOException {
        try {
            manager.load(Functions.FilterTableType.HBASE_TABLE, null);
        } catch (RuntimeException e) {
            // This exception is expected.
            Assert.assertTrue(true);
            return;
        }
        Assert.assertTrue(false);
    }

    @Test
    public void testSaveHDFS() throws IOException {
        List<List<String>> result = Lists.newArrayList();
        for (String s : testStrings) {
            result.add(Lists.newArrayList(s));
        }
        String filterName = manager.save(Functions.FilterTableType.HDFS, result);
        String resourceIdentifier = MassinFilterManager.getResourceIdentifier(KapConfig.wrap(config), filterName);

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path identifierPath = new Path(config.getHdfsWorkingDirectory(), resourceIdentifier.substring(1));

        Assert.assertTrue(fs.exists(identifierPath));

        fs.deleteOnExit(identifierPath);
    }

    @Test
    public void testLoadHDFS() throws IOException {
        List<List<String>> result = Lists.newArrayList();
        for (String s : testStrings) {
            result.add(Lists.newArrayList(s));
        }
        String filterName = manager.save(Functions.FilterTableType.HDFS, result);
        String resourceIdentifier = MassinFilterManager.getResourceIdentifier(KapConfig.wrap(config), filterName);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        Set<ByteArray> loadedData = manager.load(Functions.FilterTableType.HDFS, resourceIdentifier);
        Assert.assertEquals(result.size(), loadedData.size());

        for (String s : testStrings) {
            Assert.assertTrue(loadedData.contains(new ByteArray(s.getBytes())));
        }
        fs.deleteOnExit(new Path(resourceIdentifier));
    }
}
