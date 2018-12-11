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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class FavoriteQueryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final String[] sqlsToAnalyze = new String[] { //
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
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1");
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setSuccessCount(1);
        favoriteQuery1.setLastQueryTime(10001);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2");
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setSuccessCount(1);
        favoriteQuery2.setLastQueryTime(10002);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3");
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
        ReflectionTestUtils.setField(favoriteQueryService, "favoriteRuleService", Mockito.spy(new FavoriteRuleService()));
        for (ProjectInstance projectInstance : NProjectManager.getInstance(getTestConfig()).listAllProjects()) {
            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(projectInstance.getName());
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
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);

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

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJECT_NEWTEN, sqlsToAnalyze);
        smartMaster.runAll();

        String[] sqlsForAddCuboidTest = new String[] { "select order_id from test_kylin_fact" };

        // case of adding a new cuboid
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsForAddCuboidTest), PROJECT_NEWTEN);
        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(1, newten_data.get("size"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);

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

        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);

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
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);
        favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, 4);
        EventDao eventDaoOfNewtenProj = EventDao.getInstance(getTestConfig(), PROJECT_NEWTEN);
        var events = eventDaoOfNewtenProj.getEvents();
        events.sort(Comparator.comparing(Event::getCreateTimeNanosecond));
        Assert.assertEquals(2, events.size());
        Assert.assertEquals(4, ((AddCuboidEvent) events.get(0)).getSqlPatterns().size());

        // when there is origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 4);
        EventDao eventDaoOfDefaultProj = EventDao.getInstance(getTestConfig(), PROJECT);
        events = eventDaoOfNewtenProj.getEvents();
        events.sort(Comparator.comparing(Event::getCreateTimeNanosecond));
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
    @Ignore("no valid sqls")
    public void testManualFavorite() throws IOException, PersistentException {
        createTestFavoriteQuery();
        getTestConfig().setProperty("kylin.server.mode", "query");
        // when sql pattern not exists
        favoriteQueryService.insertToDaoAndAccelerateForWhitelistChannel(new HashSet<String>(){{add("sql_pattern_not_exists");}}, PROJECT);

        List<FavoriteQuery> favoriteQueries = favoriteQueryService.getFavoriteQueries(PROJECT);
        FavoriteQuery newInsertedRow = favoriteQueries.get(0);
        Assert.assertEquals("sql_pattern_not_exists", newInsertedRow.getSqlPattern());
        Assert.assertEquals(FavoriteQuery.CHANNEL_FROM_WHITE_LIST, newInsertedRow.getChannel());
        // triggered accelerate event, no new layouts no new event
        EventDao eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(0, eventDao.getEvents().size());
//        AccelerateEvent accelerateEvent = (AccelerateEvent) eventDao.getEvents().get(0);
//        Assert.assertEquals("sql_pattern_not_exists", accelerateEvent.getSqlPatterns().get(0));

        // sql pattern exists but not accelerating
        favoriteQueryService.insertToDaoAndAccelerateForWhitelistChannel(new HashSet<String>(){{add("sql1");}}, PROJECT);
        // triggered another accelerate event
        // sql_pattern_not_exists, sql1
        Assert.assertEquals(2, eventDao.getEvents().size());

        // sql pattern exists and is accelerating
        favoriteQueryService.insertToDaoAndAccelerateForWhitelistChannel(new HashSet<String>() {
            {
                add("sql3");
            }
        }, PROJECT);
        // assert this sql pattern did not post out
        Assert.assertEquals(2, eventDao.getEvents().size());

        // when query history is failed
        FavoriteRequest request = new FavoriteRequest(PROJECT, "test_sql", "test_sql_pattern", 1000,
                QueryHistory.QUERY_HISTORY_FAILED);
        favoriteQueryService.manualFavorite(PROJECT, request);
        Mockito.verify(favoriteQueryService, Mockito.never()).favoriteForWhitelistChannel(Mockito.anySet(),
                Mockito.anyString());

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(getTestConfig(), PROJECT);
        FavoriteRule whitelist = favoriteRuleManager.getByName(FavoriteRule.WHITELIST_NAME);
        int originSqlSize = whitelist.getConds().size();
        // when query history is succeeded
        request.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        favoriteQueryService.manualFavorite(PROJECT, request);
        // saved to whitelist
        whitelist = favoriteRuleManager.getByName(FavoriteRule.WHITELIST_NAME);
        Assert.assertEquals(originSqlSize + 1, whitelist.getConds().size());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }
}
