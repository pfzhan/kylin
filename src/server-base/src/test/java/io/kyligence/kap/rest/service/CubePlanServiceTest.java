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
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.TableIndexResponse;
import lombok.val;
import lombok.var;

public class CubePlanServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private CubePlanService cubePlanService = Mockito.spy(new CubePlanService());

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
        cubePlanService.setSemanticUpater(semanticService);

    }

    @Test
    public void testUpdateCubePlan() {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val saved = cubePlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").model("nmodel_basic")
                        .dimensions(Arrays.asList(1, 2, 3, 4))
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testUpdateEmptyRule() {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val saved = cubePlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").model("nmodel_basic")
                        .dimensions(Arrays.asList(1, 2, 3, 4))
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testCreateTableIndex() {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val originLayoutSize = origin.getAllCuboidLayouts().size();
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .layoutOverrideIndices(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val saved = cubePlanManager.getCubePlan("ncube_basic");
        Assert.assertEquals(originLayoutSize + 1, saved.getAllCuboidLayouts().size());
        NCuboidLayout newLayout = null;
        for (NCuboidLayout layout : saved.getAllCuboidLayouts()) {
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
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val originLayoutSize = origin.getAllCuboidLayouts().size();
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_SITES.SITE_NAME",
                                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_SITE_ID", "TEST_KYLIN_FACT.PRICE"))
                        .sortByColumns(Arrays.asList("TEST_SITES.SITE_NAME")).build());
        var saved = cubePlanManager.getCubePlan("ncube_basic");
        Assert.assertEquals(originLayoutSize, saved.getAllCuboidLayouts().size());
        var layout = saved.getCuboidLayout(20000000001L);
        Assert.assertTrue(layout.isManual());
        Assert.assertTrue(layout.isAuto());

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        Assert.assertEquals(0, allEvents.size());

        cubePlanService.removeTableIndex("default", "nmodel_basic", 20000000001L);
        saved = cubePlanManager.getCubePlan("ncube_basic");
        layout = saved.getCuboidLayout(20000000001L);
        Assert.assertEquals(originLayoutSize, saved.getAllCuboidLayouts().size());
        Assert.assertFalse(layout.isManual());
        Assert.assertTrue(layout.isAuto());
    }

    @Test
    public void testCreateDuplicateTableIndex() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Already exists same layout");
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
    }

    @Test
    public void testRemoveTableIndex() {
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.removeTableIndex("default", "nmodel_basic", 20000003001L);

        Assert.assertFalse(cubePlanService.getCubePlanManager("default").getCubePlan("ncube_basic")
                .getAllCuboidLayouts().stream().anyMatch(l -> l.getId() == 20000003001L));
    }

    @Test
    public void testRemoveWrongId() {
        thrown.expect(IllegalStateException.class);
        cubePlanService.removeTableIndex("default", "nmodel_basic", 20000002001L);
    }

    @Test
    public void testUpdateTableIndex() {
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.updateTableIndex("default",
                CreateTableIndexRequest.builder().id(20000003001L).project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());

        Assert.assertFalse(cubePlanService.getCubePlanManager("default").getCubePlan("ncube_basic")
                .getAllCuboidLayouts().stream().anyMatch(l -> l.getId() == 20000003001L));
        Assert.assertTrue(cubePlanService.getCubePlanManager("default").getCubePlan("ncube_basic").getAllCuboidLayouts()
                .stream().anyMatch(l -> l.getId() == 20000004001L));
        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        allEvents.sort(Event::compareTo);

        Assert.assertEquals(4, allEvents.size());
    }

    @Test
    public void testGetTableIndex() {
        val originSize = cubePlanService.getTableIndexs("default", "nmodel_basic").size();
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic").name("ti1")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME", "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                                "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val result = cubePlanService.getTableIndexs("default", "nmodel_basic");
        Assert.assertEquals(3 + originSize, result.size());
        val first = result.get(originSize);
        Assert.assertThat(first.getColOrder(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID",
                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID")));
        Assert.assertThat(first.getShardByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertEquals(20000003001L, first.getId().longValue());
        Assert.assertEquals("default", first.getProject());
        Assert.assertEquals("ti1", first.getName());
        Assert.assertEquals("ADMIN", first.getOwner());
        Assert.assertEquals("nmodel_basic", first.getModel());
        Assert.assertEquals(TableIndexResponse.Status.EMPTY, first.getStatus());
        Assert.assertTrue(first.isManual());
        Assert.assertFalse(first.isAuto());
    }

    @Test
    public void testGetRule() throws Exception {
        Assert.assertNull(cubePlanService.getRule("default", "nmodel_basic"));
        val rule = JsonUtil.deepCopy(cubePlanService.getRule("default", "nmodel_basic_inner"),
                NRuleBasedCuboidsDesc.class);
        Assert.assertNotNull(rule);
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), "default");
        cubeMgr.updateCubePlan("ncube_basic_inner", copy -> {
            val newRule = new NRuleBasedCuboidsDesc();
            newRule.setDimensions(Lists.newArrayList(1, 2, 3));
            newRule.setMeasures(Lists.newArrayList(1001, 1002));
            rule.setNewRuleBasedCuboid(newRule);
            copy.setRuleBasedCuboidsDesc(rule);
        });
        val rule2 = cubePlanService.getRule("default", "nmodel_basic_inner");
        Assert.assertNotEquals(rule2, rule);
    }
}
