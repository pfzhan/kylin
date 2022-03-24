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

import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
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
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexEntity.Range;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.FusionRuleDataResult;
import lombok.val;
import lombok.var;

public class FusionIndexServiceTest extends SourceTestCase {

    @InjectMocks
    private FusionIndexService fusionIndexService = Mockito.spy(new FusionIndexService());

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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        ReflectionTestUtils.setField(fusionIndexService, "indexPlanService", indexPlanService);
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private UpdateRuleBasedCuboidRequest createUpdateRuleRequest(String project, String modelId,
            NAggregationGroup aggregationGroup, boolean restoreDelIndex) {
        return UpdateRuleBasedCuboidRequest.builder().project(project).modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).restoreDeletedIndex(restoreDelIndex).build();
    }

    private NAggregationGroup mkAggGroup(Integer... dimension) {
        NAggregationGroup aggregationGroup = new NAggregationGroup();
        aggregationGroup.setIncludes(dimension);
        val selectRule = new SelectRule();
        selectRule.mandatoryDims = new Integer[0];
        selectRule.hierarchyDims = new Integer[0][0];
        selectRule.jointDims = new Integer[0][0];
        aggregationGroup.setSelectRule(selectRule);
        return aggregationGroup;
    }

    @Test
    public void testUpdateRuleWithHybrid() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.HYBRID);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.HYBRID, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.HYBRID);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, true);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testGetRuleWithHybrid() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.HYBRID, rule.getAggregationGroups().get(0).getIndexRange());
        Assert.assertEquals(true, rule.getIndexUpdateEnabled());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());
        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
        Assert.assertEquals(false, rule1.getIndexUpdateEnabled());
    }

    @Test
    public void testUpdateRuleWithBatch() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.BATCH);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(0, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.BATCH, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.BATCH);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, true);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(0, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testUpdateRuleWithStreaming() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(Range.STREAMING);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(Range.STREAMING, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(Range.STREAMING);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, true);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testGetTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        var response = fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.STREAMING, Range.HYBRID));
        Assert.assertEquals(5, streamingIndexes.size());

        var batchIndexes = fusionIndexService.getIndexes("streaming_test", batchId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.BATCH, Range.HYBRID));
        Assert.assertEquals(4, batchIndexes.size());

        batchIndexes = fusionIndexService.getIndexes("streaming_test", batchId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null,
                Lists.newArrayList(Range.HYBRID));
        Assert.assertEquals(3, batchIndexes.size());
    }

    @Test
    public void testHybridTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(modelId);
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.HYBRID, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID).id(20000060001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 1, indexPlan.getAllLayouts().size());

        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }
        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000070001L, Range.HYBRID);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testBatchTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(batchId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 2, indexPlan.getAllLayouts().size());
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.BATCH, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        var index = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null,
                Lists.newArrayList(20000000001L), null);
        Assert.assertEquals(1, index.size());
        Assert.assertEquals(Range.BATCH, index.get(0).getIndexRange());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.BATCH).id(20000000001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }

        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.BATCH);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testStreamingTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(modelId);
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
        Assert.assertThat(newLayout.getColOrder(), CoreMatchers.is(Arrays.asList(0, 11)));
        Assert.assertThat(newLayout.getShardByColumns(), CoreMatchers.is(Arrays.asList(11)));
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList()));
        Assert.assertEquals(Range.STREAMING, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(6, streamingIndexes.size());

        fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING).id(20000050001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        LayoutEntity newLayout1 = null;
        for (LayoutEntity layout : saved.getAllLayouts()) {
            if (newLayout1 == null) {
                newLayout1 = layout;
            } else {
                if (newLayout1.getId() < layout.getId()) {
                    newLayout1 = layout;
                }
            }
        }
        Assert.assertThat(newLayout1.getColOrder(), CoreMatchers.is(Arrays.asList(0, 12)));
        Assert.assertThat(newLayout1.getShardByColumns(), CoreMatchers.is(Arrays.asList(12)));
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList()));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000070001L, Range.STREAMING);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        Assert.assertEquals(5, streamingIndexes.size());
    }

    @Test
    public void testFusionCalculateAggIndexCount() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val aggGroup1 = mkAggGroup(0, 11);
        aggGroup1.setIndexRange(Range.HYBRID);

        val aggGroup2 = mkAggGroup(12);
        aggGroup2.setIndexRange(Range.BATCH);

        val aggGroup3 = mkAggGroup(13);
        aggGroup3.setIndexRange(Range.STREAMING);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2, aggGroup3)).build();
        AggIndexResponse response = fusionIndexService.calculateAggIndexCount(request);

        Assert.assertThat(response.getTotalCount().getResult(), is(10L));

        Assert.assertThat(response.getAggIndexCounts().get(0).getResult(), is(6L));
        Assert.assertThat(response.getAggIndexCounts().get(1).getResult(), is(1L));
        Assert.assertThat(response.getAggIndexCounts().get(2).getResult(), is(1L));
    }

    @Test
    public void testCalculateEmptyAggIndexCount() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val aggGroup1 = mkAggGroup(0, 11);
        aggGroup1.setIndexRange(Range.HYBRID);

        val aggGroup2 = mkAggGroup();
        aggGroup2.setIndexRange(Range.EMPTY);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2)).build();
        AggIndexResponse response = fusionIndexService.calculateAggIndexCount(request);

        Assert.assertEquals(1, response.getAggIndexCounts().size());
    }

    @Test
    public void testFusionDiffRuleBaseIndex() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        // hybrid +8 index
        val aggGroup1 = mkAggGroup(0, 11, 12);
        aggGroup1.setIndexRange(Range.HYBRID);

        //batch +1 index
        val aggGroup2 = mkAggGroup(12);
        aggGroup2.setIndexRange(Range.BATCH);

        //stream +1 index
        val aggGroup3 = mkAggGroup(13);
        aggGroup3.setIndexRange(Range.STREAMING);

        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Arrays.asList(aggGroup1, aggGroup2, aggGroup3)).build();
        val response = fusionIndexService.calculateDiffRuleBasedIndex(request);

        Assert.assertThat(response.getIncreaseLayouts(), is(10));
        Assert.assertThat(response.getDecreaseLayouts(), is(0));
        Assert.assertThat(response.getRollbackLayouts(), is(0));
    }

    @Test
    public void testStreamingIndexChange() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b73";

        try {
            fusionIndexService.createTableIndex("streaming_test",
                    CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId)
                            .indexRange(Range.STREAMING)
                            .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_LINENUMBER"))
                            .shardByColumns(Arrays.asList("SSB_STREAMING.LO_LINENUMBER")).isLoadData(true)
                            .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test",
                    CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId)
                            .indexRange(Range.STREAMING).id(20000000001L)
                            .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                            .shardByColumns(Arrays.asList("SSB_STREAMING.LO_CUSTKEY")).isLoadData(true)
                            .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.STREAMING);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                    .aggregationGroups(Lists.newArrayList(mkAggGroup(3))).build();
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testFusionModelWithBatchIndexChange() throws Exception {
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        val batchId = "cd2b9a23-699c-4699-b0dd-38c9412b3dfd";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        var saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        fusionIndexService.createTableIndex("streaming_test",
                CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                        .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                        .shardByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).isLoadData(true)
                        .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(5, saved.getAllLayouts().size());

        fusionIndexService.updateTableIndex("streaming_test",
                CreateTableIndexRequest.builder().project("streaming_test").modelId(modelId).indexRange(Range.BATCH)
                        .id(20000000001L)
                        .colOrder(Arrays.asList("SSB_STREAMING.LO_ORDERKEY", "SSB_STREAMING.LO_CUSTKEY"))
                        .shardByColumns(Arrays.asList("SSB_STREAMING.LO_CUSTKEY")).isLoadData(true)
                        .sortByColumns(Arrays.asList("SSB_STREAMING.LO_ORDERKEY")).build());

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, Range.BATCH);

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, saved.getAllLayouts().size());

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.BATCH);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        val response = fusionIndexService.calculateDiffRuleBasedIndex(request);
        Assert.assertThat(response.getIncreaseLayouts(), is(1));
    }

    @Test
    public void testFusionModelWithStreamingIndexChange() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        try {
            fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.STREAMING).id(20000050001L)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000050001L, Range.STREAMING);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.STREAMING);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        try {
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testFusionModelWithHybridIndexChange() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        NDataflow df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        try {
            fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                    .project("streaming_test").modelId(modelId).indexRange(Range.HYBRID).id(20000040001L)
                    .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                    .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                    .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            fusionIndexService.removeIndex("streaming_test", modelId, 20000040001L, Range.HYBRID);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        NAggregationGroup aggregationGroup = mkAggGroup(3);
        aggregationGroup.setIndexRange(Range.HYBRID);
        val request = UpdateRuleBasedCuboidRequest.builder().project("streaming_test").modelId(modelId)
                .aggregationGroups(Lists.newArrayList(aggregationGroup)).build();
        try {
            fusionIndexService.calculateDiffRuleBasedIndex(request);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRemoveIndexes() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(5, indexPlan.getAllIndexes().size());

        try {
            fusionIndexService.removeIndexes("streaming_test", modelId, indexPlan.getAllLayoutIds(false));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE.toErrorCode().getCodeString(),
                    e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        val batchId = "cd2b9a23-699c-4699-b0dd-38c9412b3dfd";
        val batchIndexPlan = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(4, batchIndexPlan.getAllLayouts().size());
        fusionIndexService.removeIndexes("streaming_test", batchId, batchIndexPlan.getAllLayoutIds(false));

        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getAllLayouts().size());
    }

    @Test
    public void testRemoveIndexe() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        fusionIndexService.removeIndex("streaming_test", modelId, 20000040001L, Range.HYBRID);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val saved = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(4, saved.getAllLayouts().size());
    }

    @Test
    public void testCheckStreamingJobAndSegments() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val df = dfMgr.getDataflow(modelId);
        val segRange = new SegmentRange.KafkaOffsetPartitionedSegmentRange(10L, 100L,
                createKafkaPartitionOffset(0, 200L), createKafkaPartitionOffset(0, 400L));
        dfMgr.appendSegmentForStreaming(df, segRange, RandomUtil.randomUUIDStr());

        val indexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        val indexUpdateEnabled = FusionIndexService.checkUpdateIndexEnabled("streaming_test", modelId);
        val result = FusionRuleDataResult.get(indexes, 20, 10, indexUpdateEnabled);
        Assert.assertEquals(indexUpdateEnabled, result.isIndexUpdateEnabled());

        val nullResult = FusionRuleDataResult.get(null, 20, 10, indexUpdateEnabled);
        Assert.assertEquals(indexUpdateEnabled, nullResult.isIndexUpdateEnabled());
        Assert.assertEquals(0, nullResult.getTotalSize());
    }

    @Test
    public void testGetAllIndex() throws Exception {
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        var indexResponses = fusionIndexService.getAllIndexes("streaming_test", modelId, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(8, indexResponses.size());

        val modelId1 = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        var indexResponses1 = fusionIndexService.getAllIndexes("streaming_test", modelId1, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(8, indexResponses1.size());

        val modelId2 = "e78a89dd-847f-4574-8afa-8768b4228b72";
        var indexResponses2 = fusionIndexService.getAllIndexes("streaming_test", modelId2, "", Lists.newArrayList(),
                "data_size", true, Lists.newArrayList());
        Assert.assertEquals(64, indexResponses2.size());
    }
}
