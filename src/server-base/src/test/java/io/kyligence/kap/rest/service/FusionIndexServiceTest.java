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
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.rest.constant.Constant;
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

import java.util.Arrays;

public class FusionIndexServiceTest extends CSVSourceTestCase {

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
        aggregationGroup.setIndexRange(IndexEntity.Range.HYBRID);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(IndexEntity.Range.HYBRID, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(IndexEntity.Range.HYBRID);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, true);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(3, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testUpdateRuleWithBatch() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        NAggregationGroup aggregationGroup = mkAggGroup(0);
        aggregationGroup.setIndexRange(IndexEntity.Range.BATCH);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(0, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(1, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(IndexEntity.Range.BATCH, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(IndexEntity.Range.BATCH);
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
        aggregationGroup.setIndexRange(IndexEntity.Range.STREAMING);
        IndexPlan saved = fusionIndexService.updateRuleBasedCuboid("streaming_test",
                createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, false)).getFirst();

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        Assert.assertEquals(1, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule.getAggregationGroups().size());
        Assert.assertEquals(IndexEntity.Range.STREAMING, rule.getAggregationGroups().get(0).getIndexRange());

        aggregationGroup = mkAggGroup(0, 11);
        aggregationGroup.setIndexRange(IndexEntity.Range.STREAMING);
        val updateRuleRequest = createUpdateRuleRequest("streaming_test", modelId, aggregationGroup, true);

        saved = fusionIndexService.updateRuleBasedCuboid("streaming_test", updateRuleRequest).getFirst();
        Assert.assertEquals(3, saved.getRuleBaseLayouts().size());
        Assert.assertEquals(0, indexPlanManager.getIndexPlan(batchId).getRuleBaseLayouts().size());

        val rule1 = fusionIndexService.getRule("streaming_test", modelId);
        Assert.assertEquals(1, rule1.getAggregationGroups().size());
    }

    @Test
    public void testHybridTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        var response = fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.HYBRID)
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
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));
        Assert.assertEquals(IndexEntity.Range.HYBRID, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(4, streamingIndexes.size());

        response = fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.HYBRID).id(20000000001L)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_CUSTKEY"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_CUSTKEY")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
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
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, IndexEntity.Range.HYBRID);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(3, streamingIndexes.size());
    }

    @Test
    public void testBatchTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        var response = fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.BATCH)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize, indexPlan.getAllLayouts().size());
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
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));
        Assert.assertEquals(IndexEntity.Range.BATCH, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(4, streamingIndexes.size());

        response = fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.BATCH).id(20000000001L)
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
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, IndexEntity.Range.BATCH);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(3, streamingIndexes.size());
    }

    @Test
    public void testStreamingTableIndex() throws Exception {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val origin = indexPlanManager.getIndexPlan(modelId);
        val originLayoutSize = origin.getAllLayouts().size();
        var response = fusionIndexService.createTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.STREAMING)
                .colOrder(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY", "P_LINEORDER_STREAMING.LO_LINENUMBER"))
                .shardByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_LINENUMBER")).isLoadData(true)
                .sortByColumns(Arrays.asList("P_LINEORDER_STREAMING.LO_ORDERKEY")).build());

        var saved = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(originLayoutSize + 1, saved.getAllLayouts().size());
        var indexPlan = indexPlanManager.getIndexPlan(batchId);
        Assert.assertEquals(originLayoutSize, indexPlan.getAllLayouts().size());
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
        Assert.assertThat(newLayout.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));
        Assert.assertEquals(IndexEntity.Range.STREAMING, newLayout.getIndexRange());

        var streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(4, streamingIndexes.size());

        response = fusionIndexService.updateTableIndex("streaming_test", CreateTableIndexRequest.builder()
                .project("streaming_test").modelId(modelId).indexRange(IndexEntity.Range.STREAMING).id(20000000001L)
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
        Assert.assertThat(newLayout1.getSortByColumns(), CoreMatchers.is(Arrays.asList(0)));

        fusionIndexService.removeIndex("streaming_test", modelId, 20000010001L, IndexEntity.Range.STREAMING);

        streamingIndexes = fusionIndexService.getIndexes("streaming_test", modelId, "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null);
        Assert.assertEquals(3, streamingIndexes.size());
    }
}
