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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.rest.response.AggIndexCombResult;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.TableIndexResponse;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexPlanServiceTest extends CSVSourceTestCase {

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void tearDown() {
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        super.setup();
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
    }

    private AtomicBoolean prepare(String modelId) throws NoSuchFieldException, IllegalAccessException {
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "true");
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);
        AtomicBoolean clean = new AtomicBoolean(false);
        val recommendationManager = spyOptimizeRecommendationManager();
        Mockito.doAnswer(invocation -> {
            String id = invocation.getArgument(0);
            if (modelId.equals(id)) {
                clean.set(true);
            }
            return null;
        }).when(recommendationManager).cleanInEffective(Mockito.anyString());
        Assert.assertFalse(clean.get());
        return clean;
    }

    @Test
    public void testUpdateSingleRuleBasedCuboid() throws NoSuchFieldException, IllegalAccessException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(modelId);
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        val saved = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default")
                                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());
        Assert.assertTrue(clean.get());
    }

    @Test
    public void testUpdateRuleBasedSortDimension() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup1.setSelectRule(selectRule);

        val aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 4, 3, 5 });
        aggregationGroup2.setSelectRule(selectRule);

        var saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                        .build())
                .getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(5, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 3, 4, 5]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3 });
        aggregationGroup2.setIncludes(new Integer[] { 4, 3 });

        val aggregationGroup3 = new NAggregationGroup();
        aggregationGroup3.setIncludes(new Integer[] { 2, 4 });
        aggregationGroup3.setSelectRule(selectRule);

        val aggregationGroup4 = new NAggregationGroup();
        aggregationGroup4.setIncludes(new Integer[] { 5, 4 });
        aggregationGroup4.setSelectRule(selectRule);

        saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId).aggregationGroups(
                        Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3, aggregationGroup4))
                        .build())
                .getFirst();

        Assert.assertEquals(5, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 5, 4, 3]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup2.setIncludes(new Integer[] { 2, 5, 6, 4 });
        aggregationGroup3.setIncludes(new Integer[] { 5, 3 });

        saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId)
                        .aggregationGroups(Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3))
                        .build())
                .getFirst();

        Assert.assertEquals(6, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 5, 6, 3, 4]", saved.getRuleBasedIndex().getDimensions().toString());

        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3 });
        aggregationGroup2.setIncludes(new Integer[] { 2, 4 });
        aggregationGroup3.setIncludes(new Integer[] { 4, 3 });
        aggregationGroup4.setIncludes(new Integer[] { 3, 2 });

        saved = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId).aggregationGroups(
                        Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3, aggregationGroup4))
                        .build())
                .getFirst();

        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals("[1, 2, 3, 4]", saved.getRuleBasedIndex().getDimensions().toString());
    }

    @Test
    public void testUpdateIndexPlanDuplicate() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        val saved = indexPlanService
                .updateRuleBasedCuboid("default",
                        UpdateRuleBasedCuboidRequest.builder().project("default")
                                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build())
                .getFirst();
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());

        long lastModifiedTime = saved.getRuleBasedIndex().getLastModifiedTime();

        val res = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build());
        long lastModifiedTime2 = res.getFirst().getRuleBasedIndex().getLastModifiedTime();
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_LAYOUT, res.getSecond().getType());
        Assert.assertTrue(lastModifiedTime2 > lastModifiedTime);
    }

    @Test
    public void testUpdateIndexPlanWithNoSegment() {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(new Integer[] { 1, 2, 3, 4 });
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);

        val res = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup)).build());
        val saved = res.getFirst();
        val response = res.getSecond();
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_SEGMENT, response.getType());
        Assert.assertNotNull(saved.getRuleBasedIndex());
        Assert.assertEquals(4, saved.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 15, saved.getAllLayouts().size());
    }

    @Test
    public void testCreateTableIndex() throws NoSuchFieldException, IllegalAccessException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        val response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
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
        Assert.assertTrue(clean.get());
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
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(true)
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());

        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_LAYOUT, response.getType());
    }

    @Test
    public void testRemoveTableIndex() {
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000040001L);

        Assert.assertFalse(
                indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getAllLayouts().stream().anyMatch(l -> l.getId() == 20000040001L));
    }

    @Test
    public void testRemoveWrongId() {
        thrown.expect(IllegalStateException.class);
        indexPlanService.removeTableIndex("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 20000020001L);
    }

    @Test
    public void testUpdateTableIndex() throws NoSuchFieldException, IllegalAccessException {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        long prevMaxId = 20000040001L;
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId(modelId)
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        val response = indexPlanService.updateTableIndex("default",
                CreateTableIndexRequest.builder().id(prevMaxId).project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).build());

        Assert.assertFalse(
                indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getAllLayouts().stream().anyMatch(l -> l.getId() == prevMaxId));
        Assert.assertTrue(
                indexPlanService.getIndexPlanManager("default").getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getAllLayouts().stream().anyMatch(l -> l.getId() == prevMaxId + IndexEntity.INDEX_ID_STEP));
        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val allEvents = eventDao.getEvents();
        allEvents.sort(Event::compareTo);

        Assert.assertEquals(4, allEvents.size());
        Assert.assertTrue(clean.get());
    }

    @Test
    public void testGetTableIndex() {
        val originSize = indexPlanService.getTableIndexs("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa").size();
        indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .name("ti1")
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
        Assert.assertEquals(20000040001L, first.getId().longValue());
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

    @Test
    public void testCalculateAggIndexCountEmpty() throws Exception {
        String aggGroupStr = "{\"includes\":[],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup));
        AggIndexCombResult aggIndexCombResult = ret.getAggIndexCounts().get(0);
        Assert.assertEquals(0L, aggIndexCombResult.getResult());
        Assert.assertEquals(0L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCalculateAggIndexCount() throws Exception {
        String aggGroupStr = "{\"includes\":[0, 1, 2, 3],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[[1, 3]],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup));
        AggIndexCombResult aggIndexCombResult = ret.getAggIndexCounts().get(0);
        Assert.assertEquals(11L, aggIndexCombResult.getResult());
        Assert.assertEquals(11L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCalculateAggIndexCountFail() throws Exception {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11,12],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        var response = calculateCount(Lists.newArrayList(aggGroup));
        Assert.assertEquals(1, response.getAggIndexCounts().size());
        Assert.assertEquals("FAIL", response.getAggIndexCounts().get(0).getStatus());
        Assert.assertEquals("FAIL", response.getTotalCount().getStatus());
    }

    @Test
    public void testCalculateAggIndexCountTwoGroups() throws Exception {
        String aggGroupStr1 = "{\"includes\":[0,1,2,3,4,5],\"select_rule\":{\"mandatory_dims\":[0, 1, 2],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        String aggGroupStr2 = "{\"includes\":[2,3,4,5,6,7],\"select_rule\":{\"mandatory_dims\":[2, 4],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        val aggGroup1 = JsonUtil.readValue(aggGroupStr1, NAggregationGroup.class);
        val aggGroup2 = JsonUtil.readValue(aggGroupStr2, NAggregationGroup.class);
        val ret = calculateCount(Lists.newArrayList(aggGroup1, aggGroup2));
        Assert.assertEquals(8L, ret.getAggIndexCounts().get(0).getResult());
        Assert.assertEquals(4096L, ret.getAggrgroupMaxCombination().longValue());
        Assert.assertEquals(16L, ret.getAggIndexCounts().get(1).getResult());
        Assert.assertEquals(25L, ret.getTotalCount().getResult());
    }

    @Test
    public void testCheckIndexCountWithinLimit() {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        NAggregationGroup aggGroup = null;
        try {
            aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        } catch (IOException e) {
            log.error("Read value fail ", e);
        }
        var request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(Lists.newArrayList(aggGroup))
                .build();
        indexPlanService.checkIndexCountWithinLimit(request);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIndexCountWithinLimitFail() {
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11,12],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[]}}";
        NAggregationGroup aggGroup = null;
        try {
            aggGroup = JsonUtil.readValue(aggGroupStr, NAggregationGroup.class);
        } catch (IOException e) {
            log.error("Read value fail ", e);
        }
        var request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(Lists.newArrayList(aggGroup))
                .build();
        indexPlanService.checkIndexCountWithinLimit(request);
    }

    private AggIndexResponse calculateCount(List<NAggregationGroup> aggGroups) {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(aggGroups).build();

        return indexPlanService.calculateAggIndexCount(request);
    }

}
