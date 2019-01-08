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
import java.util.HashMap;

import io.kyligence.kap.cube.model.IndexEntity;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.model.NIndexPlanManager;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.cube.model.NRuleBasedIndex;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.TableIndexResponse;
import lombok.val;
import lombok.var;

public class indexPlanServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private indexPlanService indexPlanService = Mockito.spy(new indexPlanService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        indexPlanService.setSemanticUpater(semanticService);

    }

    @Test
    public void testUpdateIndexPlan() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .dimensions(Arrays.asList(1, 2, 3, 4))
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 1, saved.getAllLayouts().size());
    }

    @Test
    public void testUpdateEmptyRule() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .dimensions(Arrays.asList(1, 2, 3, 4))
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 1, saved.getAllLayouts().size());
    }

    @Test
    public void testCreateTableIndex() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val originLayoutSize = origin.getAllLayouts().size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout == null) {
                newLayout = layout;
            } else {
                if (newLayout.getId() < layout.getId()) {
                    newLayout = layout;
                }
            }
        }
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(1, 2, 3, 4)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(1)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList(2)));

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        allEvents.sort(Event::compareTo);

        Assert.assertEquals(2, allEvents.size());
        val newLayoutEvent = allEvents.get(0);
        Assert.assertTrue(newLayoutEvent instanceof AddCuboidEvent);
    }

    @Test
    public void testCreateTableIndexIsAuto() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val originLayoutSize = origin.getAllLayouts().size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_SITES.SITE_NAME",
                                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_SITE_ID", "TEST_KYLIN_FACT.PRICE"))
                        .sortByColumns(Arrays.asList("TEST_SITES.SITE_NAME")).build());
        var saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(originLayoutSize, saved.getAllLayouts().size());
        var layout = saved.getCuboidLayout(20000000001L);
        Assert.assertTrue(layout.isManual());
        Assert.assertTrue(layout.isAuto());

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        Assert.assertEquals(0, allEvents.size());

        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000000001L);
        saved = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        layout = saved.getCuboidLayout(20000000001L);
        Assert.assertEquals(originLayoutSize, saved.getAllLayouts().size());
        Assert.assertFalse(layout.isManual());
        Assert.assertTrue(layout.isAuto());
    }

    @Test
    public void testCreateDuplicateTableIndex() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Already exists same layout");
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
    }

    @Test
    public void testRemoveTableIndex() {
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000030001L);

        Assert.assertFalse(indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .getAllLayouts().stream().anyMatch(l -> l.getId() == 20000030001L));
    }

    @Test
    public void testRemoveWrongId() {
        thrown.expect(IllegalStateException.class);
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000020001L);
    }

    @Test
    public void testUpdateTableIndex() {
        long prevMaxId = 20000030001L;
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.updateTableIndex("default",
                CreateTableIndexRequest.builder().id(prevMaxId).project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());

        Assert.assertFalse(indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .getAllLayouts().stream().anyMatch(l -> l.getId() == prevMaxId));
        Assert.assertTrue(indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getAllLayouts()
                .stream().anyMatch(l -> l.getId() == prevMaxId + IndexEntity.INDEX_ID_STEP));
        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        allEvents.sort(Event::compareTo);

        Assert.assertEquals(4, allEvents.size());
    }

    @Test
    public void testGetTableIndex() {
        val originSize = indexPlanService.getTableIndexs("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa").size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").name("ti1")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME", "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val result = indexPlanService.getTableIndexs("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(3 + originSize, result.size());
        val first = result.get(originSize);
        Assert.assertThat(first.getColOrder(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID",
                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID")));
        Assert.assertThat(first.getShardByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertEquals(20000030001L, first.getId().longValue());
        Assert.assertEquals("default", first.getProject());
        Assert.assertEquals("ti1", first.getName());
        Assert.assertEquals("ADMIN", first.getOwner());
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", first.getModel());
        Assert.assertEquals(TableIndexResponse.Status.EMPTY, first.getStatus());
        Assert.assertTrue(first.isManual());
        Assert.assertFalse(first.isAuto());
    }

    @Test
    public void testGetRule() throws Exception {
        Assert.assertNull(indexPlanService.getRule("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        val rule = JsonUtil.deepCopy(indexPlanService.getRule("default", "741ca86a-1f13-46da-a59f-95fb68615e3a"),
                NRuleBasedIndex.class);
        Assert.assertNotNull(rule);
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        indePlanManager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copy -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Lists.newArrayList(1, 2, 3));
            newRule.setMeasures(Lists.newArrayList(1001, 1002));
            copy.setRuleBasedIndex(newRule);
        });
        val rule2 = indexPlanService.getRule("default", "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertNotEquals(rule2, rule);
    }
}
