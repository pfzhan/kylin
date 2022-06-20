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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.execution.ShellExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
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
    public void testTrashRecord() throws Exception {
        val config = getTestConfig();
        val kapConfig = KapConfig.getInstanceFromEnv();
        config.setProperty("kylin.storage.time-machine-enabled", "true");
        val cleaner = new StorageCleaner();
        val workingDir = config.getHdfsWorkingDirectory();
        val beforeProtectionTime = System.currentTimeMillis() - config.getStorageResourceSurvivalTimeThreshold();
        val keys = cleaner.getTrashRecord().keySet().stream().collect(Collectors.toSet());

        keys.stream().forEach(k -> {
            cleaner.getTrashRecord().remove(k);
            // default/dict/global_dict/DEFAULT.TEST_KYLIN_FACT -> 1584689333538
            if (k.equals("default/dict/global_dict/DEFAULT.TEST_KYLIN_FACT/invalid")) {
                cleaner.getTrashRecord().put(new Path(workingDir, k).toString(), String.valueOf(beforeProtectionTime));
            } else {
                cleaner.getTrashRecord().put(new Path(workingDir, k).toString(),
                        String.valueOf(System.currentTimeMillis()));
            }
        });
        NTableMetadataManager.getInstance(config, "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(config, "default").listAllDataflows()) {
            NDataflowManager.getInstance(config, "default").dropDataflow(dataflow.getId());
        }
        cleaner.execute();
        val files = FileUtils.listFiles(new File(config.getHdfsWorkingDirectory().replace("file://", "") + "/default"
                + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);
        Assert.assertEquals(2, files.size());
    }

    @Test
    public void testCleanup_WithRunningJobs() throws Exception {
        val jobMgr = ExecutableManager.getInstance(getTestConfig(), "default");
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

    @Test
    public void testCleanup_withProjects() throws Exception {
        val cleaner = new StorageCleaner(true, Collections.singletonList("default"));

        NTableMetadataManager.getInstance(getTestConfig(), "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        cleaner.execute();
        Collection<File> files = FileUtils
                .listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "") + "/default"
                        + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);
        Assert.assertEquals(0, files.size());

        files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/table_index" + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);

        Assert.assertNotEquals(0, files);
    }

    @Test
    public void testCleanupPreserveTableDescLastSnapshot() throws Exception {
        val cleaner = new StorageCleaner(true, Collections.singletonList("default"));

        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        val countryTableSnapshotPath = "default/table_snapshot/DEFAULT.TEST_COUNTRY/b4849638-4eb5-44f5-b776-0619813fb676";
        val countryTableDesc = NTableMetadataManager.getInstance(getTestConfig(), "default").getTableDesc("DEFAULT.TEST_COUNTRY");
        countryTableDesc.setLastSnapshotPath(countryTableSnapshotPath);
        NTableMetadataManager.getInstance(getTestConfig(), "default").updateTableDesc(countryTableDesc);

        cleaner.execute();

        // test if table snapshot is preserved
        val snapshots = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/default" + HadoopUtil.SNAPSHOT_STORAGE_ROOT), null, true);
        Assert.assertEquals(1, snapshots.size());
        Assert.assertTrue(snapshots.iterator().next().getAbsolutePath().contains(countryTableSnapshotPath));
    }

    @Test
    public void testStorageCleanerWithRateLimiter() throws Exception {
        boolean cleanup = true;
        Collection<String> projects = Collections.emptyList();
        double requestFSRate = 10.0;
        int tRetryTimes = 10;

        val cleaner = new StorageCleaner(cleanup, projects, requestFSRate, tRetryTimes);
        val fsd = StorageCleaner.FileSystemDecorator.getInstance(HadoopUtil.getWorkingFileSystem());
        val filePath = new Path(getTestConfig().getHdfsWorkingDirectory());

        int totalRequestTimes = 30;
        long start = System.currentTimeMillis();
        for (int i = 0; i < totalRequestTimes; i++) {
            fsd.listStatus(filePath);
        }
        long duration = System.currentTimeMillis() - start;
        double expectTime = 1000 * (totalRequestTimes - 2 * requestFSRate) / requestFSRate;

        Assert.assertTrue(duration > expectTime);
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
        val execMgr = ExecutableManager.getInstance(config, "default");
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
