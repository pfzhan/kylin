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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
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
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.TableIndexResponse;
import io.kylingence.kap.event.manager.EventDao;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.RemoveCuboidByIdEvent;
import lombok.val;
import lombok.var;

public class CubePlanServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private CubePlanService cubePlanService = Mockito.spy(new CubePlanService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.dropDataflow("all_fixed_length");
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val fixedPlan = cubePlanManager.getCubePlan("all_fixed_length");
        cubePlanManager.removeCubePlan(fixedPlan);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @Test
    public void testUpdateCubePlan() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val saved = cubePlanService.updateRuleBasedCuboid(UpdateRuleBasedCuboidRequest.builder().project("default")
                .model("nmodel_basic").dimensions(Arrays.asList(1, 2, 3, 4)).measures(Arrays.asList(1001, 1002))
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testUpdateEmptyRule() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val saved = cubePlanService.updateRuleBasedCuboid(UpdateRuleBasedCuboidRequest.builder().project("default")
                .model("nmodel_basic").dimensions(Arrays.asList(1, 2, 3, 4)).measures(Arrays.asList(1001, 1002))
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testUpdateEmptyCube() throws IOException, PersistentException {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.dropDataflow("ncube_basic");
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val fixedPlan = cubePlanManager.getCubePlan("ncube_basic");
        cubePlanManager.removeCubePlan(fixedPlan);
        val saved = cubePlanService.updateRuleBasedCuboid(UpdateRuleBasedCuboidRequest.builder().project("default")
                .model("nmodel_basic").dimensions(Arrays.asList(1, 2, 3, 4)).measures(Arrays.asList(1001, 1002))
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList()).build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testCreateTableIndex() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val originLayoutSize = origin.getAllCuboidLayouts().size();
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .layoutOverrideIndices(new HashMap<String, String>() {{
                    put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                }})
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
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
        Assert.assertEquals(1, allEvents.size());
        val newLayoutEvent = allEvents.get(0);
        Assert.assertTrue(newLayoutEvent instanceof AddCuboidEvent);
        Assert.assertThat(((AddCuboidEvent) newLayoutEvent).getLayoutIds(),
                CoreMatchers.is(Arrays.asList(newLayout.getId())));
    }

    @Test
    public void testCreateDuplicateTableIndex() throws IOException, PersistentException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Already exists same layout");
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
    }


    @Test
    public void testCreateTableIndexWithNewColumns() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val originLayoutSize = origin.getAllCuboidLayouts().size();
        val originModelSize = origin.getModel().getAllNamedColumns().size();
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                        "TEST_SELLER_TYPE_DIM.DIM_UPD_USER", "TEST_SELLER_TYPE_DIM.DIM_UPD_DATE"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val saved = cubePlanManager.getCubePlan("ncube_basic");
        Assert.assertEquals(originLayoutSize + 1, saved.getAllCuboidLayouts().size());
        val newLayout = saved.getCuboidLayout(30_000_000_001L);
        Assert.assertEquals(originModelSize + 2, saved.getModel().getAllNamedColumns().size());
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(1, 2, 3, 4, 37, 38)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(1)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList(2)));
    }

    @Test
    public void testRemoveTableIndex() throws IOException, PersistentException {
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.removeTableIndex("default", "nmodel_basic", 30_000_000_001L);

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        var allEvents = eventDao.getEvents();
        Collections.sort(allEvents, new Comparator<Event>() {
            @Override
            public int compare(Event o1, Event o2) {
                return (int) (o1.getCreateTime() - o2.getCreateTime());
            }
        });
        Assert.assertEquals(2, allEvents.size());
        val event = allEvents.get(1);
        Assert.assertThat(((RemoveCuboidByIdEvent) event).getLayoutIds(), CoreMatchers.is(Arrays.asList(30_000_000_001L)));
    }

    @Test
    public void testRemoveWrongId() throws IOException, PersistentException {
        thrown.expect(IllegalStateException.class);
        cubePlanService.removeTableIndex("default", "nmodel_basic", 20000002001L);
    }
    @Test
    public void testUpdateTableIndex() throws IOException, PersistentException {
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.updateTableIndex(
                CreateTableIndexRequest.builder().id(30_000_000_001L).project("default").model("nmodel_basic")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        Collections.sort(allEvents, new Comparator<Event>() {
            @Override
            public int compare(Event o1, Event o2) {
                return (int) (o1.getCreateTime() - o2.getCreateTime());
            }
        });
        Assert.assertEquals(3, allEvents.size());
        val event = allEvents.get(1);
        Assert.assertThat(((RemoveCuboidByIdEvent) event).getLayoutIds(), CoreMatchers.is(Arrays.asList(30_000_000_001L)));
        val event2 = allEvents.get(2);
        Assert.assertThat(((AddCuboidEvent) event2).getLayoutIds(), CoreMatchers.is(Arrays.asList(30_000_001_001L)));
    }

    @Test
    public void testGetTableIndex() throws IOException, PersistentException {
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .name("ti1")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                        "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME", "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        cubePlanService.createTableIndex(CreateTableIndexRequest.builder().project("default").model("nmodel_basic")
                .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                        "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID",
                        "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"))
                .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val result = cubePlanService.getTableIndexs("default", "nmodel_basic");
        Assert.assertEquals(3, result.size());
        val first = result.get(0);
        Assert.assertThat(first.getColOrder(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID",
                "TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID")));
        Assert.assertThat(first.getShardByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertThat(first.getSortByColumns(), CoreMatchers.is(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")));
        Assert.assertEquals(30_000_000_001L, first.getId().longValue());
        Assert.assertEquals("default", first.getProject());
        Assert.assertEquals("ti1", first.getName());
        Assert.assertEquals("ADMIN", first.getOwner());
        Assert.assertEquals("nmodel_basic", first.getModel());
        Assert.assertEquals(TableIndexResponse.Status.EMPTY, first.getStatus());
    }
}
