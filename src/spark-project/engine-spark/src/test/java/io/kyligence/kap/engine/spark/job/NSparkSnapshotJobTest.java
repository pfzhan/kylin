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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkSnapshotJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() {
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        config = getTestConfig();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSnapshotJob() throws Exception {
        String tableName = "SSB.PART";
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        Assert.assertNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), getProject(), "ADMIN",
                false);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        Assert.assertNotNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getLastSnapshotPath());
    }

}
