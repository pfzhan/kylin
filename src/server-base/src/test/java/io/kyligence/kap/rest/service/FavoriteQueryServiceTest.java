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
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.smart.NSmartMaster;
import org.apache.kylin.rest.msg.MsgPicker;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@Ignore
public class FavoriteQueryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static long now = System.currentTimeMillis();

    @Mock
    private QueryHistoryService queryHistoryService = Mockito.spy(QueryHistoryService.class);
    @InjectMocks
    private final FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        ReflectionTestUtils.setField(favoriteQueryService, "queryHistoryService", queryHistoryService);
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("HADOOP_USER_NAME");
        staticCleanupTestMetadata();
    }

    private void stubQueryFilterRule(List<QueryFilterRule> rules) {
        final QueryFilterRuleManager manager = Mockito.mock(QueryFilterRuleManager.class);
        Mockito.when(manager.getAll()).thenReturn(rules);

        Mockito.when(favoriteQueryService.getQueryFilterRuleManager(PROJECT)).thenReturn(manager);
    }

    private void stubUnAcceleratedFavorites(String[] sqls, String project) {
        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        for (String sql : sqls) {
            favoriteQueries.add(new FavoriteQuery(sql, sql.hashCode(), project));
        }
    }

//    private List<QueryHistory> queriesForTest() {
//        QueryHistory queryHistory1 = new QueryHistory();
//        queryHistory1.setUuid("query-0");
//        queryHistory1.setSql("select * from existing_table_1");
//        queryHistory1.setStartTime(now);
//        queryHistory1.setDuration(200);
//        queryHistory1.setQueryStatus(QueryHistoryStatusEnum.FAILED);
//        QueryHistory queryHistory2 = new QueryHistory();
//        queryHistory2.setUuid("query-1");
//        queryHistory2.setSql("select * from existing_table_2");
//        queryHistory2.setStartTime(now);
//        queryHistory2.setDuration(30);
//        queryHistory2.setQueryStatus(QueryHistoryStatusEnum.FAILED);
//        QueryHistory queryHistory3 = new QueryHistory();
//        queryHistory3.setStartTime(now + 1);
//        queryHistory3.setUuid("query-2");
//        queryHistory3.setDuration(70);
//        queryHistory3.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
//        queryHistory3.setSql("select * from existing_table_2");
//
//        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3);
//    }

    private List<QueryFilterRule> rulesForTest() {
        QueryFilterRule queryFilterRule1 = new QueryFilterRule();
        queryFilterRule1.setUuid("rule-0");
        queryFilterRule1.setEnabled(true);

        QueryFilterRule queryFilterRule2 = new QueryFilterRule();
        queryFilterRule2.setUuid("rule-1");
        queryFilterRule2.setEnabled(false);

        return Lists.newArrayList(queryFilterRule1, queryFilterRule2);
    }

    @Test
    public void testGetAccelerateTips() throws IOException {
        String[] sqls = new String[] { //
                "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where cal_dt = '2012-01-03' group by cal_dt, lstg_format_name", //
                "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where lstg_format_name = 'ABIN' group by cal_dt, lstg_format_name", //
                "select sum(price) from test_kylin_fact where cal_dt = '2012-01-03'", //
                "select lstg_format_name, sum(item_count), count(*) from test_kylin_fact group by lstg_format_name" //
        };

        stubUnAcceleratedFavorites(sqls, PROJECT);

        // case of no model
        HashMap<String, Object> newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(false, newten_data.get("reach_threshold"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", sqls);
        smartMaster.runAll();

        String[] sqlsForAddCuboidTest = new String[] {
                "select order_id from test_kylin_fact"
        };

        // case of adding a new cuboid
        stubUnAcceleratedFavorites(sqlsForAddCuboidTest, PROJECT);
        newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(1, newten_data.get("size"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        // if there is no unAcceleratedQueries
//        Mockito.when(favoriteQueryService.getUnAcceleratedQueries(PROJECT)).thenReturn(new ArrayList<FavoriteQuery>());
        HashMap<String, Object> default_data = favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertEquals(0, default_data.get("size"));
        Assert.assertEquals(false, default_data.get("reach_threshold"));
        Assert.assertEquals(0, default_data.get("optimized_model_num"));

        stubUnAcceleratedFavorites(sqls, PROJECT);

        System.setProperty("kylin.favorite.query-accelerate-threshold", "1");
        newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));
        System.clearProperty("kylin.favorite.query-accelerate-threshold");
    }

    @Test
    public void testAcceptAccelerate() {
        getTestConfig().setProperty("kylin.server.mode", "query");
//        Mockito.when(favoriteQueryService.getUnAcceleratedQueries(PROJECT)).thenReturn(favoritesForTest());
        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
        } catch (Throwable ex) {
            Assert.assertEquals(ex.getMessage(), String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10));
        }

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 2);
        } catch (Throwable ex) {
            // ignore
        }
        getTestConfig().setProperty("kylin.server.mode", "all");

    }

    @Test
    public void testIgnoreAccelerateTips() {
        Assert.assertFalse(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
//        Mockito.when(favoriteQueryService.getUnAcceleratedQueries(PROJECT)).thenReturn(Lists.<FavoriteQuery>newArrayList());
        favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertTrue(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
        favoriteQueryService.ignoreAccelerate(PROJECT);
        Assert.assertEquals(2, (int) favoriteQueryService.getIgnoreCountMap().get(PROJECT));
    }
}
