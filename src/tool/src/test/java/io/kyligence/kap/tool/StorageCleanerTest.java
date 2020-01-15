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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflow;
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
    @Ignore
    public void testCleanupGarbage() throws Exception {
        val cleaner = new StorageCleaner();

        val baseDir = new File(getTestConfig().getMetadataUrl().getIdentifier()).getParentFile();
        val files = FileUtils.listFiles(new File(baseDir, "working-dir"), null, true);
        val garbageFiles = files.stream().filter(f -> f.getAbsolutePath().contains("invalid"))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());

        cleaner.execute();

        val outdatedItems = normalizeGarbages(cleaner.getOutdatedItems());
        Assert.assertEquals(garbageFiles.size(), outdatedItems.size());
        for (String item : outdatedItems) {
            Assert.assertTrue(item + " not in garbageFiles", garbageFiles.contains(item));
        }
    }

    @Test
    public void testCleanupAfterTruncate() throws Exception {
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
    public void testCleanupAfterRemoveTable() throws Exception {
        val cleaner = new StorageCleaner();

        NTableMetadataManager.getInstance(getTestConfig(), "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        cleaner.execute();
        val files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/default" + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);
        Assert.assertEquals(0, files.size());

    }

    @Test
    public void testCleanup_WithRunningJobs() throws Exception {
        val jobMgr = NExecutableManager.getInstance(getTestConfig(), "default");
        val job1 = new DefaultChainedExecutable();
        job1.setProject("default");
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);
        Map<String, String> extra = Maps.newHashMap();
        val dependFiles = new String[] { "/default/dict/global_dict/invalid", "/default/parquet/invalid",
                "/default/parquet/abe3bf1a-c4bc-458d-8278-7ea8b00f5e96/invalid",
                "/default/table_snapshot/DEFAULT.TEST_COUNTRY",
                "/default/dict/global_dict/DEFAULT.TEST_KYLIN_FACT/invalid/keep" };

        extra.put(AbstractExecutable.DEPENDENT_FILES, StringUtils.join(dependFiles, ","));
        jobMgr.updateJobOutput(job1.getId(), ExecutableState.RUNNING, extra, null, null);

        val cleaner = new StorageCleaner();
        cleaner.execute();
        val garbagePaths = normalizeGarbages(cleaner.getOutdatedItems());
        for (String file : dependFiles) {
            Assert.assertFalse(garbagePaths.stream().anyMatch(p -> p.startsWith(file)));
            Assert.assertFalse(garbagePaths.stream().anyMatch(file::startsWith));
        }
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

    private Set<String> normalizeGarbages(Set<StorageCleaner.StorageItem> items) {
        return items.stream().map(i -> i.getPath().replaceAll("file:", "").replaceAll("/keep", ""))
                .collect(Collectors.toSet());
    }
}
