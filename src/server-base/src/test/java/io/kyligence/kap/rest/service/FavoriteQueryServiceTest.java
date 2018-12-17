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
package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import lombok.var;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.Comparator;

public class FavoriteQueryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final String[] sqls = new String[] { //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where cal_dt = '2012-01-03' group by cal_dt, lstg_format_name", //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where lstg_format_name = 'ABIN' group by cal_dt, lstg_format_name", //
            "select sum(price) from test_kylin_fact where cal_dt = '2012-01-03'", //
            "select lstg_format_name, sum(item_count), count(*) from test_kylin_fact group by lstg_format_name" //
    };

    @InjectMocks
    private FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    private void createTestFavoriteQuery() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery(sqls[0]);
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setSuccessCount(1);
        favoriteQuery1.setLastQueryTime(10001);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery(sqls[1]);
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setSuccessCount(1);
        favoriteQuery2.setLastQueryTime(10002);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery(sqls[2]);
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setSuccessCount(1);
        favoriteQuery3.setLastQueryTime(10003);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        favoriteQueryManager.create(Lists.newArrayList(favoriteQuery1, favoriteQuery2, favoriteQuery3));
    }

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        ReflectionTestUtils.setField(favoriteQueryService, "favoriteRuleService",
                Mockito.spy(new FavoriteRuleService()));
        for (ProjectInstance projectInstance : NProjectManager.getInstance(getTestConfig()).listAllProjects()) {
            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(projectInstance.getName());
            favoriteScheduler.init();
            Assert.assertTrue(favoriteScheduler.hasStarted());
        }
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    private void stubUnAcceleratedSqlPatterns(List<String> sqls, String project) {
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(sqls).when(favoriteQueryManager).getUnAcceleratedSqlPattern();
        Mockito.doReturn(favoriteQueryManager).when(favoriteQueryService).getFavoriteQueryManager(project);
    }

    @Test
    public void testGetAccelerateTips() throws IOException {
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        // case of not reached threshold
        Map<String, Object> newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(false, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));

        // case of no model
        System.setProperty("kylin.favorite.query-accelerate-threshold", "1");

        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJECT_NEWTEN, sqls);
        smartMaster.runAll();

        String[] sqlsForAddCuboidTest = new String[] { "select order_id from test_kylin_fact" };

        // case of adding a new cuboid
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsForAddCuboidTest), PROJECT_NEWTEN);
        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(1, newten_data.get("size"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));
        System.clearProperty("kylin.favorite.query-accelerate-threshold");

        // when unaccelerated sql patterns list is empty
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(), PROJECT);
        Map<String, Object> data = favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertEquals(0, data.get("size"));
        Assert.assertEquals(false, data.get("reach_threshold"));
        Assert.assertEquals(0, data.get("optimized_model_num"));
    }

    @Test
    public void testGetAccelerateTipsInManualTypeProject() throws IOException {
        val projectManager = NProjectManager.getInstance(getTestConfig());
        val manualProject = projectManager.copyForWrite(projectManager.getProject(PROJECT_NEWTEN));
        manualProject.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(manualProject);

        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        Map<String, Object> manualProjData = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(0, manualProjData.get("optimized_model_num"));

        // change project type back to auto
        manualProject.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(manualProject);
    }

    @Test
    public void testAcceptAccelerate() throws PersistentException, IOException {
        getTestConfig().setProperty("kylin.server.mode", "query");

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
        } catch (Throwable ex) {
            Assert.assertEquals(ex.getMessage(),
                    String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10));
        }

        // when there is no origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);
        favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, 4);
        EventDao eventDaoOfNewtenProj = EventDao.getInstance(getTestConfig(), PROJECT_NEWTEN);
        var events = eventDaoOfNewtenProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(2, events.size());
        Assert.assertEquals(4, ((AddCuboidEvent) events.get(0)).getSqlPatterns().size());

        // when there is origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 4);
        EventDao eventDaoOfDefaultProj = EventDao.getInstance(getTestConfig(), PROJECT);
        events = eventDaoOfNewtenProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(2, events.size());
        Assert.assertEquals(4, ((AddCuboidEvent) events.get(0)).getSqlPatterns().size());

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10),
                    ex.getMessage());
        }

        // when models are reconstructing
        try {
            getTestConfig().setProperty("kylin.favorite.batch-accelerate-size", "4");
            favoriteQueryService.acceptAccelerate(PROJECT, 4);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalStateException.class, ex.getClass());
            Assert.assertEquals("model all_fixed_length is reconstructing", ex.getMessage());
        }

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testIgnoreAccelerateTips() {
        Assert.assertFalse(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
        favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertTrue(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
        favoriteQueryService.ignoreAccelerate(PROJECT);
        Assert.assertEquals(2, (int) favoriteQueryService.getIgnoreCountMap().get(PROJECT));
    }

    @Test
    public void testManualFavorite() throws NoSuchFieldException, IllegalAccessException, IOException {
        createTestFavoriteQuery();
        getTestConfig().setProperty("kylin.server.mode", "query");
        // when sql pattern not exists
        String sqlPattern = "select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME";
        favoriteQueryService.markFavoriteAndAccelerate(new HashSet<String>(){{add(sqlPattern);}}, PROJECT, "ADMIN");

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        List<FavoriteQuery> favoriteQueries = favoriteQueryManager.getAll();
        favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        FavoriteQuery newCreated = favoriteQueries.get(0);
        Assert.assertEquals(sqlPattern, newCreated.getSqlPattern());
        Assert.assertEquals(FavoriteQuery.CHANNEL_FROM_WHITE_LIST, newCreated.getChannel());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, newCreated.getStatus());

        // sql pattern exists but not accelerating
        favoriteQueryService.markFavoriteAndAccelerate(new HashSet<String>(){{add(sqls[0]);}}, PROJECT, "ADMIN");
        favoriteQueries = favoriteQueryManager.getAll();
        favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        FavoriteQuery favoriteQueryWithSql0 = favoriteQueries.get(favoriteQueries.size() - 1);
        Assert.assertEquals(favoriteQueries.size(), favoriteQueries.size());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, favoriteQueryWithSql0.getStatus());

        // sql pattern exists and is accelerating
        favoriteQueryService.markFavoriteAndAccelerate(new HashSet<String>() {
            {
                add(sqls[2]);
            }
        }, PROJECT, "ADMIN");

        // if accelerate sql is blocked
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        projectInstance = projectManager.copyForWrite(projectInstance);
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstance);

        NDataModel dataModel = NDataModelManager.getInstance(getTestConfig(), PROJECT).getDataModelDesc("all_fixed_length");

        for (NDataModel.NamedColumn namedColumn : dataModel.getAllNamedColumns()) {
            if (namedColumn.getId() == 16) {
                Class<?> clazz = namedColumn.getClass();
                Field field = clazz.getDeclaredField("status");
                field.setAccessible(true);
                field.set(namedColumn, NDataModel.ColumnStatus.EXIST);
            }
        }

        NDataModelManager.getInstance(getTestConfig(), PROJECT).updateDataModelDesc(dataModel.copy());


        String blockedSql = "select CAL_DT, LSTG_FORMAT_NAME, max(SLR_SEGMENT_CD) " +
                "from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME";
        favoriteQueryService.markFavoriteAndAccelerate(new HashSet<String>(){{add(blockedSql);}}, PROJECT, "ADMIN");
        Assert.assertEquals(5, favoriteQueryManager.getAll().size());
        favoriteQueries = favoriteQueryManager.getAll();
        favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        newCreated = favoriteQueries.get(0);
        Assert.assertEquals(blockedSql, newCreated.getSqlPattern());
        Assert.assertEquals(FavoriteQueryStatusEnum.BLOCKED, newCreated.getStatus());

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(getTestConfig(), PROJECT);
        FavoriteRule whitelist = favoriteRuleManager.getByName(FavoriteRule.WHITELIST_NAME);
        int originSqlSize = whitelist.getConds().size();

        // when query history is failed
        FavoriteRequest request = new FavoriteRequest(PROJECT, "test_sql", "test_sql_pattern", 1000,
                QueryHistory.QUERY_HISTORY_FAILED);
        favoriteQueryService.manualFavorite(PROJECT, request);
        // not saved to whitelist
        Assert.assertEquals(originSqlSize, whitelist.getConds().size());

        // when query history is succeeded
        request.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        favoriteQueryService.manualFavorite(PROJECT, request);
        // saved to whitelist
        whitelist = favoriteRuleManager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(originSqlSize + 1, whitelist.getConds().size());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testFilterFavoriteQuery() {
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1");
        favoriteQuery1.setLastQueryTime(1000);
        favoriteQuery1.setTotalCount(3);
        favoriteQuery1.setAverageDuration(100);
        favoriteQuery1.setSuccessRate(0.2f);
        favoriteQuery1.setStatus(FavoriteQueryStatusEnum.WAITING);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2");
        favoriteQuery2.setLastQueryTime(2000);
        favoriteQuery2.setTotalCount(2);
        favoriteQuery2.setAverageDuration(200);
        favoriteQuery2.setSuccessRate(0.1f);
        favoriteQuery2.setStatus(FavoriteQueryStatusEnum.ACCELERATING);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3");
        favoriteQuery3.setLastQueryTime(3000);
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setAverageDuration(300);
        favoriteQuery3.setSuccessRate(0.3f);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        favoriteQueryManager.create(Lists.newArrayList(favoriteQuery1, favoriteQuery2, favoriteQuery3));

        // filter status
        List<FavoriteQuery> filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, null, false,
                Lists.newArrayList(FavoriteQueryStatusEnum.WAITING.toString(), FavoriteQueryStatusEnum.ACCELERATING.toString()));
        Assert.assertEquals(2, filteredFavoriteQueries.size());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(1).getSqlPattern());

        // sort by frequency
        filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "total_count", true, null);
        Assert.assertEquals(3, filteredFavoriteQueries.size());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(1).getSqlPattern());
        Assert.assertEquals("sql3", filteredFavoriteQueries.get(2).getSqlPattern());

        // sort by success rate
        filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "success_rate", true, null);
        Assert.assertEquals(3, filteredFavoriteQueries.size());
        Assert.assertEquals("sql3", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(1).getSqlPattern());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(2).getSqlPattern());

        // sort by average duration
        filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "average_duration", true, null);
        Assert.assertEquals(3, filteredFavoriteQueries.size());
        Assert.assertEquals("sql3", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(1).getSqlPattern());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(2).getSqlPattern());
    }
}
