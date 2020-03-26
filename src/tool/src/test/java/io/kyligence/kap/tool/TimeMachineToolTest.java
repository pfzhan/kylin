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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRule.AbstractCondition;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.general.TimeTravelStausEnum;
import lombok.val;

public class TimeMachineToolTest extends NLocalFileMetadataTestCase {

    DateTimeFormatter DATE_TIME_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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

        val tool = Mockito.spy(new TimeMachineTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now();
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project12", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
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
        Assert.assertTrue(!projectItems.contains("/_global/project/project12.json"));

        Thread.sleep(1000);
        val t2 = LocalDateTime.now();
        Thread.sleep(1000);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project13", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
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
        val t3 = LocalDateTime.now();
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project14", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
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
        Assert.assertTrue(!projectItems1.contains("/_global/project/project13.json"));
        Assert.assertTrue(!projectItems1.contains("/_global/project/project14.json"));

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t3.format(DATE_TIME_FORMATTER) });

        // Time travel successfully, include project13  does not include project14
        val projectItems2 = currentResourceStore.listResources("/_global/project");
        Assert.assertTrue(projectItems2.contains("/_global/project/project13.json"));
        Assert.assertTrue(!projectItems2.contains("/_global/project/project14.json"));

        Thread.sleep(1000);
        val t4 = LocalDateTime.now();
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project15", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
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
    public void testBackCurrentMetadata() throws Exception {
        val tool = Mockito.spy(new TimeMachineTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        val currentResourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project12", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
            return 0;
        }, "project12", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project13", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
            return 0;
        }, "project13", 1);

        MetadataTool.backup(kylinConfig);

        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            val copy = dfMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").copy();
            val seg = copy.getFirstSegment();
            seg.setStatus(SegmentStatusEnum.NEW);
            val dfUpdate = new NDataflowUpdate("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            dfUpdate.setToUpdateSegs(seg);
            dfMgr.updateDataflow(dfUpdate);
            return 0;
        }, "default", 1);

        val timeStamp = System.currentTimeMillis();

        Thread.sleep(100);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectMgr.forceDropProject("broken_test");

            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setName("project11");
            return 0;
        }, "broken_test", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project11", "", "", Maps.newLinkedHashMap(), MaintainModelType.AUTO_MAINTAIN);
            return 0;
        }, "project11", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            val modelDesc = modelMgr.getDataModelDescByAlias("all_fixed_length");
            modelMgr.dropModel(modelDesc);
            return 0;
        }, "default", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "table_index");
            val modelDesc = modelMgr.getDataModelDescByAlias("test_table_index");
            modelMgr.dropModel(modelDesc);
            return 0;
        }, "table_index", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "gc_test");
            val copy = dfMgr.getDataflow("e0e90065-e7c3-49a0-a801-20465ca64799").copy();
            val seg = copy.getFirstSegment();
            val dfUpdate = new NDataflowUpdate("e0e90065-e7c3-49a0-a801-20465ca64799");
            dfUpdate.setToRemoveSegs(seg);
            dfMgr.updateDataflow(dfUpdate);
            return 0;
        }, "gc_test", 1);

        tool.execute(new String[] { "-project", "table_index", "-time", "2020-03-26-12-13-14" });
        Assert.assertEquals(tool.timeTravelStatus, TimeTravelStausEnum.START);
        tool.execute(
                new String[] { "-project", "table_index", "-time", LocalDateTime.now().format(DATE_TIME_FORMATTER) });
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = StorageURL.valueOf(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    private void prepare() throws IOException {

        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir"),
                new File(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().replace("file://", "")));
    }

}
