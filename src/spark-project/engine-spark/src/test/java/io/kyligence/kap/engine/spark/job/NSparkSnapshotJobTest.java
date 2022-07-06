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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.parquet.Strings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.execution.NSparkSnapshotJob;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;

public class NSparkSnapshotJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        config = getTestConfig();

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(config);
    }

    @After
    public void after() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSnapshotByPartitionJob() throws Exception {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-01", "2012-01-02");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        Assert.assertNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, false, null, null, null);
        setPartitions(job, partitions);
        execMgr.addJob(job);

        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(2, list(snapshotPath).length);
        val fs = HadoopUtil.getWorkingFileSystem();
        Assert.assertEquals(Stream.of(list(snapshotPath)).mapToLong(s -> {
            try {
                return HadoopUtil.getContentSummary(fs, s.getPath()).getLength();
            } catch (IOException e) {
                return 0L;
            }
        }).sum(), tableManager.getTableDesc(tableName).getLastSnapshotSize());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getTempSnapshotPath());
        Assert.assertEquals(partitionCol, tableManager.getTableDesc(tableName).getSnapshotPartitionCol());
        Assert.assertTrue(
                tableManager.getTableDesc(tableName).getSnapshotLastModified() > table.getSnapshotLastModified());
    }

    @Test
    public void testBuildSnapshotByPartitionRefreshPart() throws Exception {
        testBuildSnapshotByPartitionJob();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-03", "2012-01-04");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, true, null, null, null);
        setPartitions(job, partitions);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(4, list(snapshotPath).length);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        Assert.assertNotNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertEquals(partitionCol, tableManager.getTableDesc(tableName).getSnapshotPartitionCol());
        Assert.assertTrue(
                tableManager.getTableDesc(tableName).getSnapshotLastModified() > table.getSnapshotLastModified());
    }

    @Test
    public void testBuildSnapshotByPartitionRefreshChoosePartition() throws Exception {
        testBuildSnapshotByPartitionJob();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-03", "2012-01-04");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Set<String> partitionToBuild = ImmutableSet.of("2012-01-03");
        TableDesc tableDesc = tableManager.getTableDesc(tableName);
        tableDesc.setRangePartition(true);
        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, true, partitionToBuild, null,
                null);
        setPartitions(job, partitions);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(3, list(snapshotPath).length);

    }

    private FileStatus[] list(String path) {

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String baseDir = KapConfig.getInstanceFromEnv().getMetadataWorkingDirectory();
        String resourcePath = baseDir + "/" + path;
        try {
            return fs.listStatus(new Path(resourcePath));
        } catch (IOException e) {
            return null; // on IOException, skip the checking
        }
    }

    private void setPartitions(NSparkSnapshotJob job, Set<String> partitions) {
        job.setParam("partitions", Strings.join(partitions, ","));
        job.getSnapshotBuildingStep().setParam("partitions", Strings.join(partitions, ","));
    }

    @Test
    public void testBuildSnapshotJob() throws Exception {
        String tableName = "SSB.PART";
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        Assert.assertNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN", false, null);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        Assert.assertNotNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotEquals(0L, tableManager.getTableDesc(tableName).getLastSnapshotSize());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotEquals(0L, tableManager.getTableDesc(tableName).getSnapshotTotalRows());
    }

}
