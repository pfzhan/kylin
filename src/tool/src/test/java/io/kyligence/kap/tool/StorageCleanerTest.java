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
package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleanerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws IOException {
        createTestMetadata();
        prepare();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupGarbage() throws IOException {
        val cleaner = new StorageCleaner();

        val baseDir = new File(getTestConfig().getMetadataUrl().getIdentifier()).getParentFile();
        val files = FileUtils.listFiles(new File(baseDir, "working-dir"), null, true);
        val garbageFiles = files.stream().filter(f -> f.getAbsolutePath().contains("invalid"))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());

        cleaner.execute();
        log.debug("outdated items are {}", cleaner.getOutdatedItems());

        Assert.assertEquals(garbageFiles.size(), cleaner.getOutdatedItems().size());
        for (StorageCleaner.StorageItem item : cleaner.getOutdatedItems()) {
            Assert.assertTrue(item.getPath() + " not in garbageFiles",
                    garbageFiles.contains(item.getPath().replace("file:", "")));
        }
    }

    @Test
    public void testCleanupAfterTruncate() throws IOException {
        val cleaner = new StorageCleaner();

        val projects = NProjectManager.getInstance(getTestConfig()).listAllProjects();
        for (ProjectInstance project : projects) {
            NProjectManager.getInstance(getTestConfig()).forceDropProject(project.getName());
        }

        cleaner.execute();
        val files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")),
                null, true);
        val existFiles = files.stream().filter(f -> !f.getName().startsWith("."))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());
        Assert.assertEquals(0, existFiles.size());
    }

    @Test
    public void testCleanupAfterRemoveTable() throws IOException {
        val cleaner = new StorageCleaner();

        NTableMetadataManager.getInstance(getTestConfig(), "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        cleaner.execute();
        val files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/default" + ResourceStore.GLOBAL_DICT_RESOURCE_ROOT), null, true);
        Assert.assertEquals(0, files.size());

    }

    private void prepare() throws IOException {
        val config = getTestConfig();
        config.setProperty("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "0s");
        String metaId = config.getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
        val workingDir1 = new Path(config.getHdfsWorkingDirectory()).getParent().toString() + "/working-dir1";
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir"),
                new File(config.getHdfsWorkingDirectory().replace("file://", "")));
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir1"),
                new File(workingDir1.replace("file:", "") + "/" + metaId));

        val indexMgr = NIndexPlanManager.getInstance(config, "default");
        val inner = indexMgr.getIndexPlanByModelAlias("nmodel_basic_inner");
        indexMgr.updateIndexPlan(inner.getId(), copyForWrite -> {
            val map = Maps.<String, String> newLinkedHashMap();
            map.put("kylin.env.hdfs-working-dir", workingDir1);
            copyForWrite.setOverrideProps(map);
        });

        val dfMgr = NDataflowManager.getInstance(config, "default");
        val df = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        dfMgr.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setCreateTimeUTC(System.currentTimeMillis());
        });

        val execMgr = NExecutableManager.getInstance(config, "default");
        val job1 = new DefaultChainedExecutable();
        job1.setId("job1");
        execMgr.addJob(job1);
        val job2 = new DefaultChainedExecutable();
        job2.setId("job2");
        execMgr.addJob(job2);
    }

}
