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
package io.kyligence.kap.event;

import static io.kyligence.kap.event.DataflowJobTest.prepareSegment;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ITStorageCleanerTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_PROJECT = "default";
    private NDefaultScheduler scheduler;

    @Before
    public void setUp() throws Exception {
        System.setProperty("kylin.job.event.poll-interval-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "2");
        System.setProperty("kylin.engine.spark.build-class-name", "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");
        System.setProperty("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "0s");
        this.createTestMetadata();

        val projectMgr = NProjectManager.getInstance(getTestConfig());
        for (String project : Arrays.asList("bad_query_test", "broken_test", "demo", "match", "newten", "smart", "ssb",
                "top_n")) {
            projectMgr.forceDropProject(project);
        }
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        EventOrchestratorManager.getInstance(getTestConfig());
        NDefaultScheduler.destroyInstance();
        scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val table = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        table.setIncrementLoading(true);
        tableMgr.updateTableDesc(table);
    }

    @After
    public void tearDown() throws Exception {
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        this.cleanupTestMetadata();
        System.clearProperty("kylin.job.event.poll-interval-second");
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.engine.spark.build-class-name");
        System.clearProperty("kylin.garbage.storage.cuboid-layout-survival-time-threshold");
    }

    @Test
    public void testStorageCleanWithJob_MultiThread() throws InterruptedException {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val MAX_WAIT = 500 * 1000;
        val start = System.currentTimeMillis() + MAX_WAIT;
        val finished = new AtomicBoolean(false);
        new Thread(() -> {
            while (System.currentTimeMillis() < start && !finished.get()) {
                try {
                    val cleaner = new StorageCleaner();
                    cleaner.execute();
                    Thread.sleep(1000);
                } catch (Exception e) {
                    log.warn("gc failed", e);
                }
            }
        }).start();
        prepareSegment(df.getUuid(), "2012-01-01", "2012-06-01", true);
        prepareSegment(df.getUuid(), "2012-06-01", "2012-09-01", false);

        indexManager.updateIndexPlan(df.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(30001L, 20001L), LayoutEntity::equals, true, true);
        });
        val df2 = dataflowManager.getDataflow(df.getUuid());

        Thread.sleep(3000);
        val root = getTestConfig().getHdfsWorkingDirectory().substring(7) + "default/parquet/";
        val layoutFolders = FileUtils.listFiles(new File(root), new String[] { "parquet" }, true).stream()
                .map(File::getParent).distinct().sorted().collect(Collectors.toList());
        val expected = Sets.<String> newTreeSet();
        for (NDataSegment segment : df2.getSegments()) {
            for (Map.Entry<Long, NDataLayout> entry : segment.getLayoutsMap().entrySet()) {
                expected.add(root + df2.getId() + "/" + segment.getId() + "/" + entry.getKey());
            }
        }
        finished.set(true);
        Assert.assertEquals(String.join(";\n", expected), String.join(";\n", layoutFolders));

    }

}
