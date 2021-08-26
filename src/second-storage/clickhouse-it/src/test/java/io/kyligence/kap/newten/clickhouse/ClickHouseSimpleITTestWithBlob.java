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
package io.kyligence.kap.newten.clickhouse;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.sparkproject.guava.collect.Sets;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.extern.slf4j.Slf4j;

@Ignore("disable this suite, since it only works on hadoop 3.2")
@Slf4j
@RunWith(JUnit4.class)
public class ClickHouseSimpleITTestWithBlob extends ClickHouseSimpleITTest {
    public static final String TEST_BLOB_META = "src/test/resources/blob_metadata";
    private AzuriteContainer azuriteContainer;

    @Override
    protected void doSetup() throws Exception {
        File tempRoot = FileUtils.getTempDirectory();
        File tempDir = new File(tempRoot, RandomUtil.randomUUIDStr());
        if (!tempDir.mkdir()) {
            throw new IllegalStateException("temp blob directory create failed");
        }
        log.info("use {} as temp blob dir", tempDir.getAbsolutePath());
        FileUtils.copyDirectory(new File(TEST_BLOB_META), tempDir);

        azuriteContainer = new AzuriteContainer(10000, tempDir.getAbsolutePath());
        azuriteContainer.start();

        Map<String, String> overrideConf = Maps.newHashMap();
        overrideConf.put("fs.azure.account.key.devstoreaccount1.localhost:10000",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        overrideConf.put("fs.azure.skip.metrics", "true");
        // resolve dns problem
        overrideConf.put("fs.azure.storage.emulator.account.name", "devstoreaccount1.localhost:10000");
        overrideConf.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
        overrideConf.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.hdfs-working-dir", "wasb://test@devstoreaccount1.localhost:10000/kylin");
        SparkContext sc = ss.sparkContext();
        overrideConf.forEach(sc.hadoopConfiguration()::set);
    }

    @After
    public void tearDown() throws Exception {
        if (azuriteContainer != null) {
            azuriteContainer.stop();
            azuriteContainer = null;
        }
        super.tearDown();
    }

    @Test
    public void testSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardBlob", false, clickhouse);
        }
    }

    @Test
    public void testTwoShards() throws Exception {
        // TODO: make sure splitting data into two shards
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardsBlob", false, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testIncrementalSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalSingleShardBlob", true, clickhouse);
        }
    }

    @Test
    public void testIncrementalTwoShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardBlob", true, clickhouse1, clickhouse2);
        }
    }

    @Override
    protected String getSourceUrl() {
        return "host.docker.internal";
    }

    protected void fullBuildCube(String dfName, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("wasb"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        buildCuboid(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1),
                prj, true);
    }


    @Override
    protected void checkHttpServer() throws IOException {
    }
}
