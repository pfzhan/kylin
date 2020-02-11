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

import static io.kyligence.kap.rest.response.IndexResponse.Source.AUTO_TABLE;
import static io.kyligence.kap.rest.response.IndexResponse.Source.MANUAL_TABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.kyligence.kap.event.model.EventContext;
import org.apache.commons.collections.ListUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.exception.BadRequestException;
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
import com.google.common.collect.Sets;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.IndexResponse;
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

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId(modelId)
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();
        val diff1 = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertTrue(diff1.getIncreaseLayouts() > 0);

        var saved = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();
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

        request = UpdateRuleBasedCuboidRequest.builder().project("default").modelId(modelId)
                .aggregationGroups(
                        Lists.newArrayList(aggregationGroup1, aggregationGroup2, aggregationGroup3, aggregationGroup4))
                .build();
        val diff2 = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertTrue(diff2.getDecreaseLayouts() > 0 && diff2.getIncreaseLayouts() > 0);

        saved = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();

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
    public void testUpdateRuleBasedIndexWithDifferentMeasure() {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = indexPlanManager.getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup1.setMeasures(new Integer[] { 100000, 100001, 100002, 100003 });
        val selectRule1 = new SelectRule();
        selectRule1.mandatoryDims = new Integer[] { 1 };
        selectRule1.hierarchyDims = new Integer[][] { { 2, 3 } };
        selectRule1.jointDims = new Integer[0][0];
        aggregationGroup1.setSelectRule(selectRule1);
        val aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 1, 3, 4, 5 });
        aggregationGroup2.setMeasures(new Integer[] { 100001, 100003, 100004, 100005 });
        val selectRule2 = new SelectRule();
        selectRule2.mandatoryDims = new Integer[] { 3 };
        selectRule2.hierarchyDims = new Integer[0][0];
        selectRule2.jointDims = new Integer[][] { { 4, 5 } };
        aggregationGroup2.setSelectRule(selectRule2);

        val revertedBefore = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                        .build())
                .getFirst();
        Assert.assertNotNull(revertedBefore.getRuleBasedIndex());
        Assert.assertEquals(5, revertedBefore.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(6, revertedBefore.getRuleBasedIndex().getMeasures().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 11, revertedBefore.getAllLayouts().size());

        // revert agg groups order
        val reverted = indexPlanService.updateRuleBasedCuboid("default",
                UpdateRuleBasedCuboidRequest.builder().project("default")
                        .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").isLoadData(true)
                        .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup2, aggregationGroup1))
                        .build())
                .getFirst();

        Assert.assertEquals(5, revertedBefore.getRuleBasedIndex().getDimensions().size());
        Assert.assertEquals(6, revertedBefore.getRuleBasedIndex().getMeasures().size());
        Assert.assertEquals(origin.getAllLayouts().size() + 11, revertedBefore.getAllLayouts().size());

        Assert.assertEquals(revertedBefore.getRuleBasedIndex().getMeasures(),
                reverted.getRuleBasedIndex().getMeasures());
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
        var response = indexPlanService.createTableIndex("default",
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

        Assert.assertEquals(1, allEvents.size());
        val newLayoutEvent = allEvents.get(0);
        Assert.assertTrue(newLayoutEvent instanceof AddCuboidEvent);
        Assert.assertTrue(clean.get());

        int before = origin.getIndexes().size();
        response = indexPlanService.createTableIndex("default",
                CreateTableIndexRequest.builder().project("default").modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).isLoadData(true)
                        .layoutOverrideIndexes(new HashMap<String, String>() {
                            {
                                put("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "eq");
                            }
                        }).sortByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT")).build());
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        int after = origin.getIndexes().size();
        Assert.assertTrue(before == after);

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

        Assert.assertEquals(2, allEvents.size());
        Assert.assertTrue(clean.get());
    }

    @Test
    public void testUpdateTableIndex_markToBeDeleted() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        long existLayoutId = 20000010001L;
        long maxTableLayoutId = indexPlanService.getIndexPlanManager(project).getIndexPlan(modelId)
                .getWhitelistLayouts().stream().map(LayoutEntity::getId).max(Long::compare).get();
        indexPlanService.updateTableIndex(project,
                CreateTableIndexRequest.builder().id(existLayoutId).project(project).modelId(modelId)
                        .colOrder(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.CAL_DT",
                                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LSTG_SITE_ID"))
                        .shardByColumns(Arrays.asList("TEST_KYLIN_FACT.CAL_DT"))
                        .sortByColumns(Arrays.asList("TEST_KYLIN_FACT.TRANS_ID")).isLoadData(false).build());

        Assert.assertTrue(indexPlanService.getIndexPlanManager(project).getIndexPlan(modelId).getAllLayouts().stream()
                .anyMatch(l -> l.getId() == existLayoutId));
        Assert.assertTrue(indexPlanService.getIndexPlanManager(project).getIndexPlan(modelId).getToBeDeletedIndexes()
                .stream().map(IndexEntity::getLayouts).flatMap(List::stream).anyMatch(l -> l.getId() == existLayoutId));
        Assert.assertTrue(indexPlanService.getIndexPlanManager(project).getIndexPlan(modelId).getAllLayouts().stream()
                .anyMatch(l -> l.getId() == maxTableLayoutId + IndexEntity.INDEX_ID_STEP));
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        NDataSegment segment = df.getLatestReadySegment();
        Assert.assertNotNull(segment.getLayout(existLayoutId));
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
        val newRule = new NRuleBasedIndex();
        newRule.setDimensions(Lists.newArrayList(1, 2, 3));
        newRule.setMeasures(Lists.newArrayList(1001, 1002));

        indePlanManager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copy -> {

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
        String aggGroupStr = "{\"includes\":[0,1,2,3,4,5,6,7,8,9,10,11],\"select_rule\":{\"mandatory_dims\":[],\"hierarchy_dims\":[],\"joint_dims\":[],\"dim_cap\":2}}";
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

    @Test
    public void testUpdateAggShardByColumns() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val request = new AggShardByColumnsRequest();
        request.setModelId(modelId);
        request.setProject("default");
        request.setLoadData(true);
        request.setShardByColumns(Lists.newArrayList("TEST_KYLIN_FACT.CAL_DT", "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        indexPlanService.updateShardByColumns("default", request);

        var indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        var layouts = indexPlan.getRuleBaseLayouts();
        for (LayoutEntity layout : layouts) {
            if (layout.getColOrder().containsAll(Lists.newArrayList(2, 3))) {
                Assert.assertEquals(2, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else {
                Assert.assertEquals(1, layout.getId() % IndexEntity.INDEX_ID_STEP);
            }

        }

        val response = indexPlanService.getShardByColumns("default", modelId);
        Assert.assertArrayEquals(request.getShardByColumns().toArray(new String[0]),
                response.getShardByColumns().toArray(new String[0]));

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        val events = eventDao.getEventsOrdered();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(AddCuboidEvent.class, events.get(0).getClass());

        // change shard by columns
        request.setShardByColumns(Lists.newArrayList("TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        request.setLoadData(false);
        indexPlanService.updateShardByColumns("default", request);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(modelId);
        layouts = indexPlan.getRuleBaseLayouts();
        for (LayoutEntity layout : layouts) {
            if (layout.getColOrder().containsAll(Lists.newArrayList(2, 3))) {
                Assert.assertEquals(3, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else if (layout.getColOrder().contains(3)) {
                Assert.assertEquals(2, layout.getId() % IndexEntity.INDEX_ID_STEP);
            } else {
                Assert.assertEquals(1, layout.getId() % IndexEntity.INDEX_ID_STEP);
            }
        }
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void testUpdateAggShard_WithInvalidColumn() {
        val wrongColumn = "TEST_CAL_DT.WEEK_BEG_DT";
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Column " + wrongColumn + " is not dimension");
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val request = new AggShardByColumnsRequest();
        request.setModelId(modelId);
        request.setProject("default");
        request.setLoadData(true);
        request.setShardByColumns(
                Lists.newArrayList("TEST_KYLIN_FACT.CAL_DT", wrongColumn, "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"));
        indexPlanService.updateShardByColumns("default", request);
    }

    @Test
    public void testExtendPartitionColumns() {
        NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel("741ca86a-1f13-46da-a59f-95fb68615e3a", modeDesc -> modeDesc.setStorageType(2));
        NIndexPlanManager instance = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        IndexPlan indexPlan = instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        checkLayoutEntityPartitionCoulumns(instance, indexPlan);
        instance.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", index -> {
            List<Integer> extendPartitionColumns = new ArrayList<>(index.getExtendPartitionColumns());
            extendPartitionColumns.add(1);
            index.setExtendPartitionColumns(extendPartitionColumns);
        });
        indexPlan = instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        checkLayoutEntityPartitionCoulumns(instance, indexPlan);

    }

    private void checkLayoutEntityPartitionCoulumns(NIndexPlanManager instance, IndexPlan indexPlan) {
        ArrayList<Integer> partitionColumns = new ArrayList<>(indexPlan.getExtendPartitionColumns());
        if (indexPlan.getModel().getPartitionDesc().getPartitionDateColumnRef() != null) {
            Integer colId = indexPlan.getModel()
                    .getColId(indexPlan.getModel().getPartitionDesc().getPartitionDateColumnRef());
            partitionColumns.add(colId);
        }
        instance.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getAllIndexes().stream()
                .flatMap(indexEntity -> indexEntity.getLayouts().stream()).filter(LayoutEntity::isManual)
                .filter(layoutEntity -> !layoutEntity.isAuto()).forEach(layoutEntity -> {
                    if (layoutEntity.getOrderedDimensions().keySet().containsAll(partitionColumns)) {
                        if (!ListUtils.isEqualList(layoutEntity.getPartitionByColumns(), partitionColumns)) {
                            throw new RuntimeException("Partition column is not match.");
                        }
                    }
                });
    }

    private AggIndexResponse calculateCount(List<NAggregationGroup> aggGroups) {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("741ca86a-1f13-46da-a59f-95fb68615e3a").aggregationGroups(aggGroups).build();

        return indexPlanService.calculateAggIndexCount(request);
    }

    @Test
    public void testRemoveIndex() throws NoSuchFieldException, IllegalAccessException {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val clean = prepare(modelId);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        val manualAgg = indexPlan.getCuboidLayout(1010001L);
        Assert.assertNotNull(manualAgg);
        Assert.assertTrue(manualAgg.isManual());
        indexPlanService.removeIndex(getProject(), modelId, manualAgg.getId());
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getCuboidLayout(1010001L));
        val autoTable = indexPlan.getCuboidLayout(20000000001L);
        Assert.assertNotNull(autoTable);
        Assert.assertTrue(autoTable.isAuto());
        indexPlanService.removeIndex(getProject(), modelId, autoTable.getId());
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getCuboidLayout(20000000001L));

        testUpdateTableIndex_markToBeDeleted();
        indexPlanService.removeIndex(getProject(), modelId, 20000010001L);
        indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNull(indexPlan.getCuboidLayout(20000010001L));
        Assert.assertTrue(clean.get());
    }

    @Test
    public void testGetIndexes() throws NoSuchFieldException, IllegalAccessException {
        testUpdateSingleRuleBasedCuboid();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), "data_size",
                true, Lists.newArrayList());
        Assert.assertEquals(25, indexResponses.size());
        Assert.assertEquals(1000001L, indexResponses.get(0).getId().longValue());

        indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), "data_size", true,
                Lists.newArrayList(AUTO_TABLE, MANUAL_TABLE));

        Assert.assertTrue(indexResponses.stream().allMatch(indexResponse -> indexResponse.getSource().equals(AUTO_TABLE)
                || indexResponse.getSource().equals(MANUAL_TABLE)));

        // test default order by
        indexResponses = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList(), null, false,
                null);
        Assert.assertSame(indexResponses.get(0).getStatus(), IndexResponse.Status.EMPTY);
        IndexResponse prev = null;
        for (IndexResponse current : indexResponses) {
            if (prev == null) {
                prev = current;
                continue;
            }
            if (current.getStatus() == IndexResponse.Status.AVAILABLE) {
                Assert.assertTrue(current.getDataSize() >= prev.getDataSize());
            }
            prev = current;
        }
    }

    @Test
    public void testGetIndexes_WithKey() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        // find normal column, and measure parameter
        var response = indexPlanService.getIndexes(getProject(), modelId, "PRICE", Lists.newArrayList(), "data_size",
                false, null);
        var ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(20, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(10001L));

        // find CC column as dimension
        response = indexPlanService.getIndexes(getProject(), modelId, "NEST3", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(2, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(1000001L));

        // find CC column as measure
        response = indexPlanService.getIndexes(getProject(), modelId, "NEST4", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(14, response.size());
        for (IndexResponse res : response) {
            Assert.assertTrue(res.getColOrder().stream().map(IndexResponse.ColOrderPair::getKey)
                    .anyMatch(col -> col.equals("TEST_KYLIN_FACT.NEST4") || col.equals("SUM_NEST4")));
        }

        response = indexPlanService.getIndexes(getProject(), modelId, "nest4", Lists.newArrayList(), "data_size", false,
                null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(14, response.size());
        for (IndexResponse res : response) {
            Assert.assertTrue(res.getColOrder().stream().map(IndexResponse.ColOrderPair::getKey)
                    .anyMatch(col -> col.equals("TEST_KYLIN_FACT.NEST4") || col.equals("SUM_NEST4")));
        }
    }

    @Test
    public void testGetIndexes_WithStatus() throws Exception {
        testUpdateTableIndex_markToBeDeleted();

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        // find normal column, and measure parameter
        var response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList("EMPTY"), "data_size",
                false, null);
        var ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(3, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
        Assert.assertTrue(ids.contains(20000030001L));

        response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList("AVAILABLE"), "data_size",
                false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(7, response.size());
        Assert.assertFalse(ids.contains(20000010001L));
        Assert.assertTrue(ids.contains(10001L));

        response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList("TO_BE_DELETED"),
                "data_size", false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(1, response.size());
        Assert.assertTrue(ids.contains(20000010001L));

        Mockito.doReturn(Sets.newHashSet(20000020001L)).when(indexPlanService).getLayoutsByRunningJobs(getProject());
        response = indexPlanService.getIndexes(getProject(), modelId, "", Lists.newArrayList("BUILDING"), "data_size",
                false, null);
        ids = response.stream().map(IndexResponse::getId).collect(Collectors.toSet());
        Assert.assertEquals(1, response.size());
        Assert.assertTrue(ids.contains(20000020001L));
    }

    @Test
    public void testGetIndexGraph_EmptyFullLoad() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setPartitionDesc(null);
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        dataflowManager.dropDataflow(modelId);
        indexManager.dropIndexPlan(modelId);
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(modelId);
        indexManager.createIndexPlan(indexPlan);
        val dataflow = new NDataflow();
        dataflow.setUuid(modelId);
        dataflowManager.createDataflow(indexPlan, "ADMIN");
        val df = dataflowManager.getDataflow(modelId);
        dataflowManager.fillDfManually(df,
                Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));

        val response = indexPlanService.getIndexGraph(getProject(), modelId, 100);
        Assert.assertEquals(0, response.getStartTime());
        Assert.assertEquals(0, response.getEndTime());
        Assert.assertFalse(response.isFullLoaded());

    }

    @Test
    public void testUpdateIndexPlanWithMDC() throws Exception {
        // aggregationGroup1 will generate [1,2,10000] [1,3,10000] [1,4,10000] [1,10000] total 4 layout
        NAggregationGroup aggregationGroup1 = new NAggregationGroup();
        aggregationGroup1.setIncludes(new Integer[] { 1, 2, 3, 4 });
        aggregationGroup1.setMeasures(new Integer[] { 10000 });
        SelectRule selectRule1 = new SelectRule();
        selectRule1.setMandatoryDims(new Integer[] { 1 });
        selectRule1.setDimCap(1);
        aggregationGroup1.setSelectRule(selectRule1);

        // aggregationGroup2 will generate [5,10000,10001] [5,6,7,10000,10001] total 2 layout
        NAggregationGroup aggregationGroup2 = new NAggregationGroup();
        aggregationGroup2.setIncludes(new Integer[] { 5, 6, 7 });
        aggregationGroup2.setMeasures(new Integer[] { 10000, 10001 });
        SelectRule selectRule2 = new SelectRule();
        selectRule2.setMandatoryDims(new Integer[] { 5 });
        selectRule2.setJointDims(new Integer[][] { { 6, 7 } });
        aggregationGroup2.setSelectRule(selectRule2);

        UpdateRuleBasedCuboidRequest request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .aggregationGroups(Lists.<NAggregationGroup> newArrayList(aggregationGroup1, aggregationGroup2))
                .build();
        request.setGlobalDimCap(2);

        AggIndexResponse aggIndexResponse = indexPlanService.calculateAggIndexCount(request);
        List<AggIndexCombResult> aggIndexCounts = aggIndexResponse.getAggIndexCounts();
        Assert.assertEquals(4L, aggIndexCounts.get(0).getResult());
        Assert.assertEquals(2L, aggIndexCounts.get(1).getResult());
        // 4 + 2 + baseCuboid
        Assert.assertEquals(7L, aggIndexResponse.getTotalCount().getResult());

        val diff = indexPlanService.calculateDiffRuleBasedIndex(request);
        Assert.assertTrue(diff.getIncreaseLayouts().equals(7));

        IndexPlan indexPlan = indexPlanService.updateRuleBasedCuboid("default", request).getFirst();
        Assert.assertEquals(7, indexPlan.getRuleBaseLayouts().size());

        EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<Event> events = eventDao.getEvents();
        Event event = events.get(0);
        EventContext eventContext = new EventContext(event, KylinConfig.getInstanceFromEnv(), "default");
        event.getEventHandler().handle(eventContext);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        List<LayoutEntity> ruleBaseLayouts = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .getIndexPlan().getRuleBaseLayouts();
        Assert.assertEquals(7L, ruleBaseLayouts.size());
        Assert.assertEquals("[1, 2, 10000]", ruleBaseLayouts.get(0).getColOrder().toString());
        Assert.assertEquals("[5, 6, 7, 10000, 10001]", ruleBaseLayouts.get(1).getColOrder().toString());
        Assert.assertEquals("[1, 3, 10000]", ruleBaseLayouts.get(2).getColOrder().toString());
        Assert.assertEquals("[1, 4, 10000]", ruleBaseLayouts.get(3).getColOrder().toString());
        Assert.assertEquals("[1, 10000]", ruleBaseLayouts.get(4).getColOrder().toString());
        Assert.assertEquals("[5, 10000, 10001]", ruleBaseLayouts.get(5).getColOrder().toString());
        Assert.assertEquals("[1, 2, 3, 4, 5, 6, 7, 10000, 10001]", ruleBaseLayouts.get(6).getColOrder().toString());
    }
}
