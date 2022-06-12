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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.sparkproject.guava.collect.Sets;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRule.AbstractCondition;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.general.RollbackStatusEnum;
import lombok.val;
import lombok.var;

public class RollbackToolTest extends NLocalFileMetadataTestCase {

    DateTimeFormatter DATE_TIME_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss",
            Locale.getDefault(Locale.Category.FORMAT));

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
        val jdbcTemplate = getJdbcTemplate();
        getStore().getMetadataStore().setAuditLogStore(new JdbcAuditLogStore(getTestConfig(), jdbcTemplate,
                new DataSourceTransactionManager(jdbcTemplate.getDataSource()), "test_audit_log"));
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRestoreFail() throws Exception {

        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project12", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project12", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project12");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project12", 1);

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t1.format(DATE_TIME_FORMATTER) });

        val currentResourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        val projectItems = currentResourceStore.listResources("/_global/project");
        //Time travel successfully, does not include project12
        Assert.assertFalse(projectItems.contains("/_global/project/project12.json"));

        Thread.sleep(1000);
        val t2 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project13", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project13", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project13");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project13", 1);

        Thread.sleep(1000);
        val t3 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project14", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project14", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project14");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project14", 1);

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t2.format(DATE_TIME_FORMATTER) });

        // Time travel successfully, does not include project13  does not include project14
        val projectItems1 = currentResourceStore.listResources("/_global/project");
        Assert.assertFalse(projectItems1.contains("/_global/project/project13.json"));
        Assert.assertFalse(projectItems1.contains("/_global/project/project14.json"));

        Thread.sleep(1000);
        tool.execute(new String[] { "-skipCheckData", "true", "-time", t3.format(DATE_TIME_FORMATTER) });

        // Time travel successfully, include project13  does not include project14
        val projectItems2 = currentResourceStore.listResources("/_global/project");
        Assert.assertTrue(projectItems2.contains("/_global/project/project13.json"));
        Assert.assertFalse(projectItems2.contains("/_global/project/project14.json"));

        Thread.sleep(1000);
        val t4 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project15", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project15", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project15");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project15", 1);

        Mockito.doReturn(false).when(tool).restoreMirror(Mockito.any(), Mockito.any(), Mockito.any());

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t4.format(DATE_TIME_FORMATTER) });
        // Time travel failed, include project15
        val projectItems3 = currentResourceStore.listResources("/_global/project");
        Assert.assertTrue(projectItems3.contains("/_global/project/project13.json"));
        Assert.assertTrue(projectItems3.contains("/_global/project/project15.json"));
    }

    @Test
    public void testDeleteProject() throws Exception {
        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectMgr.forceDropProject("default");
            return 0;
        }, "default", 1);

        tool.execute(new String[] { "-time", t1.format(DATE_TIME_FORMATTER), "-project", "default" });
        Assert.assertEquals(tool.rollbackStatus, RollbackStatusEnum.WAIT_USER_CONFIRM_SUCCESS);
    }

    @Test
    public void testJobRollback() throws Exception {
        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);
        val jobId = RandomUtil.randomUUIDStr();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            mockJob("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", jobId, SegmentRange.dateToLong("2012-01-01"),
                    SegmentRange.dateToLong("2012-09-01"));
            return 0;
        }, "default", 1);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            executableManager.updateJobOutput(jobId, ExecutableState.RUNNING);
            return 0;
        }, "default", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            executableManager.updateJobOutput(jobId, ExecutableState.SUCCEED);
            return 0;
        }, "default", 1);
        tool.execute(new String[] { "-skipCheckData", "true", "-time", t1.format(DATE_TIME_FORMATTER), "-project",
                "default" });
        Assert.assertSame(ExecutableState.READY, NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default").getAllExecutables().get(0).getStatus());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = StorageURL.valueOf(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    private void mockJob(String project, String dataflowId, String jobId, long start, long end) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var dataflow = dataflowManager.getDataflow(dataflowId);
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val segment = dataflow.getSegment("ef5e0663-feba-4ed2-b71c-21958122bbff");
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId, null, null, null);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).addJob(job);
    }

    private void prepare() throws IOException {

        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir"),
                new File(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().replace("file://", "")));
    }

}
