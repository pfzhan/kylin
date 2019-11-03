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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.favorite.CheckAccelerateSqlListResult;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;
import lombok.var;

public class FavoriteQueryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final String[] sqls = new String[] { //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where cal_dt = '2012-01-03' group by cal_dt, lstg_format_name", //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where lstg_format_name = 'ABIN' group by cal_dt, lstg_format_name", //
            "select sum(price) from test_kylin_fact where cal_dt = '2012-01-03'", //
            "select lstg_format_name, sum(item_count), count(*) from test_kylin_fact group by lstg_format_name", //
            "select lstg_format_name, seller_id, sum(price)\n"
                    + "from test_account inner join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id\n"
                    + "group by lstg_format_name, seller_id order by lstg_format_name, seller_id\n" //
    };

    private final String constantSql = "select * from test_kylin_fact where 1 <> 1";
    private final String blockedSql = "select sum(lstg_format_name) from test_kylin_fact";
    private final String tableMissingSql = "select count(*) from test_kylin_table";
    private final String blockedSqlForCircleJoin = "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
            + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
            + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n" + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
            + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
            + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
            + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID AND SELLER_ACCOUNT.ACCOUNT_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
            + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"";

    @InjectMocks
    private FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    private void createTestFavoriteQuery() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery(QueryPatternUtil.normalizeSQLPattern(sqls[0]));
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setLastQueryTime(10001);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery(QueryPatternUtil.normalizeSQLPattern(sqls[1]));
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setLastQueryTime(10002);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery(QueryPatternUtil.normalizeSQLPattern(sqls[2]));
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setLastQueryTime(10003);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        favoriteQueryManager.create(Sets.newHashSet(favoriteQuery1, favoriteQuery2, favoriteQuery3));
    }

    @Before
    public void setup() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(favoriteQueryService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        for (ProjectInstance projectInstance : NProjectManager.getInstance(getTestConfig()).listAllProjects()) {
            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(projectInstance.getName());
            favoriteScheduler.init();
            Assert.assertTrue(favoriteScheduler.hasStarted());
        }
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    private void stubUnAcceleratedSqlPatterns(List<String> sqls, String project) {
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(sqls).when(favoriteQueryManager).getAccelerableSqlPattern();
        Mockito.doReturn(favoriteQueryManager).when(favoriteQueryService).getFavoriteQueryManager(project);
    }

    private void stubWaitingAccelerateSqlPatterns(List<String> sqls, String project) {
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(sqls).when(favoriteQueryManager).getToBeAcceleratedSqlPattern();
        Mockito.doReturn(favoriteQueryManager).when(favoriteQueryService).getFavoriteQueryManager(project);
    }

    @Test
    public void testGetAccelerateTips() {
        stubWaitingAccelerateSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        // case of not reached threshold
        Map<String, Object> newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(5, newten_data.get("size"));
        Assert.assertEquals(false, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));

        // case of no model
        System.setProperty("kylin.favorite.query-accelerate-threshold", "1");

        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(5, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(2, newten_data.get("optimized_model_num"));

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJECT_NEWTEN, sqls);
        smartMaster.runAll();

        String[] sqlsForAddCuboidTest = new String[] { "select order_id from test_kylin_fact" };

        // case of adding a new cuboid
        stubWaitingAccelerateSqlPatterns(Lists.newArrayList(sqlsForAddCuboidTest), PROJECT_NEWTEN);
        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(1, newten_data.get("size"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        stubWaitingAccelerateSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(5, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));
        System.clearProperty("kylin.favorite.query-accelerate-threshold");

        // when unaccelerated sql patterns list is empty
        stubWaitingAccelerateSqlPatterns(Lists.newArrayList(), PROJECT);
        Map<String, Object> data = favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertEquals(0, data.get("size"));
        Assert.assertEquals(false, data.get("reach_threshold"));
        Assert.assertEquals(0, data.get("optimized_model_num"));
    }

    @Test
    public void testGetAccelerateTipsInManualTypeProject() {
        val projectManager = NProjectManager.getInstance(getTestConfig());
        val manualProject = projectManager.copyForWrite(projectManager.getProject(PROJECT_NEWTEN));
        manualProject.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(manualProject);

        stubWaitingAccelerateSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);

        Map<String, Object> manualProjData = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(0, manualProjData.get("optimized_model_num"));

        // change project type back to auto
        manualProject.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(manualProject);
    }

    @Test
    public void testAcceptAccelerate_sqls() {
        getTestConfig().setProperty("kylin.server.mode", "query");

        List<String> sqlList = Lists.newArrayList(sqls);

        Mockito.doReturn(new CheckAccelerateSqlListResult(Lists.newArrayList(sqls), Lists.newArrayList()))
                .when(favoriteQueryService).checkAccelerateSqlList(PROJECT_NEWTEN, sqlList);

        // when there is no origin model
        Map<String, List<String>> resultNoModel = favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, sqlList);
        EventDao eventDaoOfNewtenProj = EventDao.getInstance(getTestConfig(), PROJECT_NEWTEN);
        var events = eventDaoOfNewtenProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(2, resultNoModel.get("job_list").size());

        Mockito.doReturn(new CheckAccelerateSqlListResult(Lists.newArrayList(sqls), Lists.newArrayList()))
                .when(favoriteQueryService).checkAccelerateSqlList(PROJECT, sqlList);
        // when there is origin model
        favoriteQueryService.acceptAccelerate(PROJECT, sqlList);
        EventDao eventDaoOfDefaultProj = EventDao.getInstance(getTestConfig(), PROJECT);
        events = eventDaoOfDefaultProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(2, resultNoModel.get("job_list").size());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testAcceptAccelerate() {
        getTestConfig().setProperty("kylin.server.mode", "query");

        // when there is no origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT_NEWTEN);
        favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, 4);
        EventDao eventDaoOfNewtenProj = EventDao.getInstance(getTestConfig(), PROJECT_NEWTEN);
        var events = eventDaoOfNewtenProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(4, events.size());

        // when there is origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqls), PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 4);
        EventDao eventDaoOfDefaultProj = EventDao.getInstance(getTestConfig(), PROJECT);
        events = eventDaoOfDefaultProj.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(4, events.size());

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10),
                    ex.getMessage());
        }

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testAccelerateWithNullJob() {
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), "newten");
        val fq1 = new FavoriteQuery("select count(*) from test_kylin_fact");
        fqManager.create(Sets.newHashSet(fq1));

        favoriteQueryService.acceptAccelerate("newten", 1);

        // no new index will be proposed
        val fq2 = new FavoriteQuery("select count(*) from test_kylin_fact limit 500");
        fqManager.create(Sets.newHashSet(fq2));
        favoriteQueryService.acceptAccelerate("newten", 1);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), "newten");
        Assert.assertEquals(4, eventDao.getEvents().size());
        Assert.assertEquals(2, eventDao.getJobRelatedEvents().size());
    }

    @Test
    public void testAcceptAccelerateWithPendingSqlPattern() {
        getTestConfig().setProperty("kylin.server.mode", "query");

        // 1. create favorite queries
        List<String> sqlPatterns = Arrays.stream(sqls).map(QueryPatternUtil::normalizeSQLPattern)
                .collect(Collectors.toList());
        FavoriteRequest request = new FavoriteRequest(PROJECT, Lists.newArrayList(sqlPatterns));
        favoriteQueryService.createFavoriteQuery(PROJECT, request);

        // 2. change status of some favorite queries to FAILED
        FavoriteQueryManager favoriteQueryManager = favoriteQueryService.getFavoriteQueryManager(PROJECT);
        Map<String, FavoriteQuery> fqMap = Maps.newHashMap();
        favoriteQueryManager.getAll().forEach(fq -> fqMap.putIfAbsent(fq.getSqlPattern(), fq));

        fqMap.get(sqlPatterns.get(0)).setStatus(FavoriteQueryStatusEnum.PENDING);
        fqMap.get(sqlPatterns.get(1)).setStatus(FavoriteQueryStatusEnum.FAILED);
        fqMap.get(sqlPatterns.get(2)).setStatus(FavoriteQueryStatusEnum.ACCELERATED);
        fqMap.get(sqlPatterns.get(3)).setStatus(FavoriteQueryStatusEnum.TO_BE_ACCELERATED);
        fqMap.get(sqlPatterns.get(4)).setStatus(FavoriteQueryStatusEnum.PENDING);
        favoriteQueryManager.updateFavoriteQueryMap(fqMap.get(sqlPatterns.get(0)));
        favoriteQueryManager.updateFavoriteQueryMap(fqMap.get(sqlPatterns.get(1)));
        favoriteQueryManager.updateFavoriteQueryMap(fqMap.get(sqlPatterns.get(2)));
        favoriteQueryManager.updateFavoriteQueryMap(fqMap.get(sqlPatterns.get(3)));
        favoriteQueryManager.updateFavoriteQueryMap(fqMap.get(sqlPatterns.get(4)));

        // 3. assert status update successfully
        Map<String, FavoriteQuery> fqChanged = Maps.newHashMap();
        favoriteQueryManager.getAll().forEach(fq -> fqChanged.putIfAbsent(fq.getSqlPattern(), fq));
        Assert.assertEquals(FavoriteQueryStatusEnum.PENDING, fqChanged.get(sqlPatterns.get(0)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, fqChanged.get(sqlPatterns.get(1)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, fqChanged.get(sqlPatterns.get(2)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fqChanged.get(sqlPatterns.get(3)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.PENDING, fqChanged.get(sqlPatterns.get(4)).getStatus());

        // 4. accelerate and validate
        favoriteQueryService.acceptAccelerate(PROJECT, 3);
        EventDao eventDaoOfDefaultProject = EventDao.getInstance(getTestConfig(), PROJECT);
        var events = eventDaoOfDefaultProject.getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(4, events.size());

        Map<String, FavoriteQuery> accFQ = Maps.newHashMap();
        favoriteQueryManager.getAll().forEach(fq -> accFQ.putIfAbsent(fq.getSqlPattern(), fq));
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, accFQ.get(sqlPatterns.get(0)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, accFQ.get(sqlPatterns.get(1)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, accFQ.get(sqlPatterns.get(2)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, accFQ.get(sqlPatterns.get(3)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, accFQ.get(sqlPatterns.get(4)).getStatus());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testAcceptAccelerateWithConstantAndBlockedPattern() {

        getTestConfig().setProperty("kylin.server.mode", "query");
        List<String> sqlPatterns = Lists.newArrayList(constantSql, blockedSql, tableMissingSql, blockedSqlForCircleJoin)
                .stream().map(QueryPatternUtil::normalizeSQLPattern) //
                .collect(Collectors.toList());

        FavoriteRequest request = new FavoriteRequest(PROJECT, sqlPatterns);
        favoriteQueryService.createFavoriteQuery(PROJECT, request);

        FavoriteQueryManager favoriteQueryManager = favoriteQueryService.getFavoriteQueryManager(PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 4);

        Map<String, FavoriteQuery> fqMap = Maps.newHashMap();
        favoriteQueryManager.getAll().forEach(fq -> fqMap.putIfAbsent(fq.getSqlPattern(), fq));

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, fqMap.get(sqlPatterns.get(0)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, fqMap.get(sqlPatterns.get(1)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.PENDING, fqMap.get(sqlPatterns.get(2)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, fqMap.get(sqlPatterns.get(3)).getStatus());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testAcceptAccelerateWithNormalAndBlockedPattern() {

        getTestConfig().setProperty("kylin.server.mode", "query");
        List<String> sqlPatterns = Lists.newArrayList(sqls[0], blockedSql) //
                .stream().map(QueryPatternUtil::normalizeSQLPattern) //
                .collect(Collectors.toList());
        FavoriteRequest request = new FavoriteRequest(PROJECT, sqlPatterns);
        favoriteQueryService.createFavoriteQuery(PROJECT, request);

        FavoriteQueryManager favoriteQueryManager = favoriteQueryService.getFavoriteQueryManager(PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 2);

        Map<String, FavoriteQuery> fqMap = Maps.newHashMap();
        favoriteQueryManager.getAll().forEach(fq -> fqMap.putIfAbsent(fq.getSqlPattern(), fq));

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, fqMap.get(sqlPatterns.get(0)).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, fqMap.get(sqlPatterns.get(1)).getStatus());

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testIgnoreAccelerateTips() {
        var sqlPatterns = Mockito.mock(List.class);
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(40).when(sqlPatterns).size();
        Mockito.doReturn(true).when(sqlPatterns).isEmpty();
        Mockito.doReturn(sqlPatterns).when(favoriteQueryManager).getToBeAcceleratedSqlPattern();
        Mockito.doReturn(favoriteQueryManager).when(favoriteQueryService).getFavoriteQueryManager(PROJECT);

        Assert.assertTrue((boolean) favoriteQueryService.getAccelerateTips(PROJECT).get("reach_threshold"));

        // ignore tips
        favoriteQueryService.ignoreAccelerate(PROJECT, 40);
        Assert.assertFalse((boolean) favoriteQueryService.getAccelerateTips(PROJECT).get("reach_threshold"));
    }

    @Test
    public void testCreateFavoriteQuery() {
        createTestFavoriteQuery();
        // when sql pattern not exists
        String sqlPattern = "select count(*) from test_kylin_fact";
        FavoriteRequest request = new FavoriteRequest(PROJECT, Lists.newArrayList(sqls[0], sqls[1], sqlPattern));
        var result = favoriteQueryService.createFavoriteQuery(PROJECT, request);
        Assert.assertEquals(1, (int) result.get("imported"));
        Assert.assertEquals(2, (int) result.get("waiting"));
        Assert.assertEquals(0, (int) result.get("not_accelerated"));
        Assert.assertEquals(0, (int) result.get("accelerated"));
        Assert.assertEquals(0, (int) result.get("blacklist"));

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        List<FavoriteQuery> favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(4, favoriteQueries.size());
        favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
        FavoriteQuery newCreated = favoriteQueries.get(0);
        Assert.assertEquals(QueryPatternUtil.normalizeSQLPattern(sqlPattern), newCreated.getSqlPattern());
        Assert.assertEquals(FavoriteQuery.CHANNEL_FROM_IMPORTED, newCreated.getChannel());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, newCreated.getStatus());

        // sql pattern in blaclist
        sqlPattern = "select * from test_kylin_fact";
        request.setSqls(Lists.newArrayList(sqlPattern));
        result = favoriteQueryService.createFavoriteQuery(PROJECT, request);
        Assert.assertEquals(1, (int) result.get("blacklist"));
        Assert.assertEquals(0, (int) result.get("waiting"));
        Assert.assertEquals(0, (int) result.get("not_accelerated"));
        Assert.assertEquals(0, (int) result.get("accelerated"));
        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(4, favoriteQueries.size());

        favoriteQueryManager.updateStatus(QueryPatternUtil.normalizeSQLPattern(sqls[0]),
                FavoriteQueryStatusEnum.ACCELERATING, "");
        favoriteQueryManager.updateStatus(QueryPatternUtil.normalizeSQLPattern(sqls[1]),
                FavoriteQueryStatusEnum.PENDING, "");
        favoriteQueryManager.updateStatus(QueryPatternUtil.normalizeSQLPattern(sqls[2]),
                FavoriteQueryStatusEnum.ACCELERATED, "");

        // create fqs which are already in fq list
        request.setSqls(Lists.newArrayList(sqls[0], sqls[1], sqls[2], "select count(*) from kylin_sales"));
        result = favoriteQueryService.createFavoriteQuery(PROJECT, request);
        Assert.assertEquals(1, (int) result.get("imported"));
        Assert.assertEquals(1, (int) result.get("waiting"));
        Assert.assertEquals(1, (int) result.get("not_accelerated"));
        Assert.assertEquals(1, (int) result.get("accelerated"));

        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(5, favoriteQueries.size());

        String updateSql = "update test_table set columnA='1' where columnB='1'";
        request.setSqls(Lists.newArrayList(updateSql));
        result = favoriteQueryService.createFavoriteQuery(PROJECT, request);
        Assert.assertEquals(1, (int) result.get("not_supported_sql"));

        favoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(5, favoriteQueries.size());
    }

    @Test
    public void testFilterFavoriteQuery() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        favoriteQueryManager.create(mockFavoriteQuery());

        // filter status
        List<FavoriteQuery> filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, null,
                false, Lists.newArrayList(FavoriteQueryStatusEnum.TO_BE_ACCELERATED.toString(),
                        FavoriteQueryStatusEnum.ACCELERATING.toString()));
        Assert.assertEquals(2, filteredFavoriteQueries.size());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(1).getSqlPattern());

        // sort by frequency
        filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "total_count", true, null);
        Assert.assertEquals(5, filteredFavoriteQueries.size());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(1).getSqlPattern());
        Assert.assertEquals("sql3", filteredFavoriteQueries.get(2).getSqlPattern());

        // sort by average duration
        filteredFavoriteQueries = favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "average_duration", true,
                null);
        Assert.assertEquals(5, filteredFavoriteQueries.size());
        Assert.assertEquals("sql3", filteredFavoriteQueries.get(0).getSqlPattern());
        Assert.assertEquals("sql2", filteredFavoriteQueries.get(1).getSqlPattern());
        Assert.assertEquals("sql1", filteredFavoriteQueries.get(2).getSqlPattern());
    }

    private Set<FavoriteQuery> mockFavoriteQuery() {
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1");
        favoriteQuery1.setLastQueryTime(1000);
        favoriteQuery1.setTotalCount(3);
        favoriteQuery1.setAverageDuration(100);
        favoriteQuery1.setStatus(FavoriteQueryStatusEnum.TO_BE_ACCELERATED);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2");
        favoriteQuery2.setLastQueryTime(2000);
        favoriteQuery2.setTotalCount(2);
        favoriteQuery2.setAverageDuration(200);
        favoriteQuery2.setStatus(FavoriteQueryStatusEnum.ACCELERATING);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3");
        favoriteQuery3.setLastQueryTime(3000);
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setAverageDuration(300);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.ACCELERATED);

        FavoriteQuery favoriteQuery4 = new FavoriteQuery("sql4");
        favoriteQuery4.setLastQueryTime(4000);
        favoriteQuery4.setAverageDuration(50);
        favoriteQuery4.setStatus(FavoriteQueryStatusEnum.PENDING);

        FavoriteQuery favoriteQuery5 = new FavoriteQuery("sql5");
        favoriteQuery5.setLastQueryTime(5000);
        favoriteQuery5.setAverageDuration(80);
        favoriteQuery5.setStatus(FavoriteQueryStatusEnum.FAILED);

        return Sets.newHashSet(favoriteQuery1, favoriteQuery2, favoriteQuery3, favoriteQuery4, favoriteQuery5);
    }

    @Test
    public void testGetFQSizeInDifferentStatus() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        favoriteQueryManager.create(mockFavoriteQuery());

        val result = favoriteQueryService.getFQSizeInDifferentStatus(PROJECT);
        Assert.assertEquals(2, (int) result.get("can_be_accelerated"));
        Assert.assertEquals(2, (int) result.get("waiting"));
        Assert.assertEquals(2, (int) result.get("not_accelerated"));
        Assert.assertEquals(1, (int) result.get("accelerated"));
    }

    @Test
    public void testAutoCheckAccelerationInfo() {
        KylinConfig config = getTestConfig();
        String sql = "select count(*) from TEST_KYLIN_FACT where CAL_DT = '2012-01-05' and TRANS_ID > 100 limit 10";

        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(config, PROJECT);
        FavoriteQuery favoriteQuery = new FavoriteQuery(sql);
        favoriteQuery.setTotalCount(1);
        favoriteQuery.setLastQueryTime(10001);
        favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
        favoriteQueryManager.create(Sets.newHashSet(favoriteQuery));

        // no accelerated query by default
        FavoriteQueryManager manager = FavoriteQueryManager.getInstance(config, PROJECT);
        Assert.assertTrue(manager.getAcceleratedSqlPattern().isEmpty());

        // accelerate
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJECT, new String[] { sql });
        smartMaster
                .runAllAndForContext(ctx -> FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                        .updateStatus(sql, FavoriteQueryStatusEnum.ACCELERATED, ""));
        manager.reloadSqlPatternMap();
        Assert.assertFalse(manager.getAcceleratedSqlPattern().isEmpty());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, manager.get(sql).getStatus());

        // change model version
        NDataModelManager modelManager = NDataModelManager.getInstance(config, PROJECT);
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setSemanticVersion(2);
        modelManager.updateDataModelDesc(model);

        // run check, not accelerated
        favoriteQueryService.adjustFalseAcceleratedFQ();
        manager.reloadSqlPatternMap();
        Assert.assertTrue(manager.getAcceleratedSqlPattern().isEmpty());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, manager.get(sql).getStatus());

        // accelerate again
        NSmartMaster smartMasterAfter = new NSmartMaster(getTestConfig(), PROJECT, new String[] { sql });
        smartMasterAfter
                .runAllAndForContext(ctx -> FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                        .updateStatus(sql, FavoriteQueryStatusEnum.ACCELERATED, ""));
        manager.reloadSqlPatternMap();
        Assert.assertFalse(manager.getAcceleratedSqlPattern().isEmpty());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, manager.get(sql).getStatus());

        // run check again, accelerated
        favoriteQueryService.adjustFalseAcceleratedFQ();
        Assert.assertFalse(manager.getAcceleratedSqlPattern().isEmpty());
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, manager.get(sql).getStatus());

        // ------------ case when table not loaded --------------
        tableService.unloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT", false);
        favoriteQueryService.adjustFalseAcceleratedFQ();
        manager.reloadSqlPatternMap();
        Assert.assertTrue(manager.getAcceleratedSqlPattern().isEmpty());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, manager.get(sql).getStatus());
    }

    @Test
    public void testUpdateNotAcceleratedSqlStatus() {

        KylinConfig mockConfig = KylinConfig.createKylinConfig(getTestConfig());
        val fqMgr = FavoriteQueryManager.getInstance(mockConfig, PROJECT_NEWTEN);
        Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();
        Set<FavoriteQuery> fqs = Sets.newHashSet();
        for (int i = 0; i < 2; i++) {
            final String sql = "sql_" + i;
            FavoriteQuery fq = new FavoriteQuery();
            fq.setSqlPattern(sql);
            fqs.add(fq);

            AccelerateInfo accelerateInfo = new AccelerateInfo();
            if (i % 2 == 0) {
                accelerateInfo.setFailedCause(new IllegalArgumentException());
            } else {
                accelerateInfo.setPendingMsg(new IllegalStateException().toString());
            }
            accelerateInfoMap.put(sql, accelerateInfo);
        }
        fqMgr.create(fqs);

        favoriteQueryService.updateNotAcceleratedSqlStatusForTest(accelerateInfoMap, mockConfig, PROJECT_NEWTEN);
        List<FavoriteQuery> fqList = fqMgr.getAll();
        fqList.sort(Comparator.comparing(FavoriteQuery::getSqlPattern));
        Assert.assertEquals(FavoriteQueryStatusEnum.FAILED, fqList.get(0).getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.PENDING, fqList.get(1).getStatus());
    }
}
