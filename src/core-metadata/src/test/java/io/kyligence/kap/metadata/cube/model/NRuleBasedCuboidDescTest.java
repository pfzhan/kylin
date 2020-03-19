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
package io.kyligence.kap.metadata.cube.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.CubeTestUtils;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NRuleBasedCuboidDescTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGenCuboids() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [1],\n"
                                        + "          \"joint_dims\": [\n" + "            [3,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [1,2,3,4,5],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [[2,3,4]],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [\n"
                        + "            [1,5]\n" + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                log.error("Something wrong happened when update this IndexPlan.", e);
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(12, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.<List<Integer>> newArrayList(Lists.newArrayList(1),
                Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 4, 6), Lists.newArrayList(1, 3, 4, 5, 6)));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(10001L, 120001L, 30001L, 40001L, 80001L, 130001L, 140001L, 150001L, 160001L,
                        170001L, 180001L, 190001L)));
    }

    @Test
    public void testCorrectnessOfGenRuleBasedIndexes() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);
        indexPlanManager.createIndexPlan(newPlan);
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Lists.newArrayList(2, 1, 3));
            try {
                val group = JsonUtil.readValue("{ \"includes\": [2, 1, 3], "
                        + "\"select_rule\": { \"hierarchy_dims\": [], \"mandatory_dims\": [2], "
                        + "\"joint_dims\": [ [1,3] ] } }", NAggregationGroup.class);
                newRule.setAggregationGroups(Lists.newArrayList(group));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (Exception e) {
                log.error("Something wrong happened when update this indexPlan.", e);
            }
        });
        logLayouts(indexPlan.getAllLayouts());
        List<IndexEntity> allIndexes = indexPlan.getAllIndexes();
        Assert.assertEquals(2, allIndexes.size());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        Assert.assertEquals(Lists.newArrayList(2), allIndexes.get(0).getDimensions());
        Assert.assertEquals("{2}", allIndexes.get(0).getDimensionBitset().toString());
        IndexEntity entity0 = allIndexes.get(0);
        Assert.assertEquals(Lists.newArrayList(100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009,
                100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017), entity0.getMeasures());
        Assert.assertEquals(Lists.newArrayList(2, 100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008,
                100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017),
                entity0.getLayouts().get(0).getColOrder());
        Assert.assertEquals(Lists.newArrayList(2, 1, 3), allIndexes.get(1).getDimensions());
        Assert.assertEquals("{1, 2, 3}", allIndexes.get(1).getDimensionBitset().toString());
        IndexEntity entity1 = allIndexes.get(1);
        Assert.assertEquals(allIndexes.get(0).getMeasures(), entity1.getMeasures());
        Assert.assertEquals(Lists.newArrayList(2, 1, 3, 100000, 100001, 100002, 100003, 100004, 100005, 100007, //
                100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016, 100017),
                entity1.getLayouts().get(0).getColOrder());
    }

    @Test
    public void testGenTooManyCuboids() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"),
                IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        try {
            indexPlanManager.createIndexPlan(newPlan);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "Too many cuboids for the cube. Cuboid combination reached 41449 and limit is 40960. Abort calculation.",
                    e.getMessage());
        }
    }

    @Test
    public void testGenCuboidsWithAuto() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_mixed.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(14, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [1],\n"
                                        + "          \"joint_dims\": [\n" + "            [3,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [1,2,3,4,5],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [[2,3,4]],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [\n"
                        + "            [1,5]\n" + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(14, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.<List<Integer>> newArrayList(Lists.newArrayList(1, 3, 4, 5, 6),
                Lists.newArrayList(1), Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 4, 6)));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(130001L, 230001L, 150001L, 160001L, 200001L, 240001L, 250001L, 260001L, 270001L, 280001L, 290001L, 300001L)));
        val actualDims = indexPlan.getRuleBaseLayouts().stream()
                .map(layout -> layout.getOrderedDimensions().keySet().asList()).collect(Collectors.toSet());
        val expectedDims = Lists.<List> newArrayList(Lists.newArrayList(1, 2, 5), Lists.newArrayList(2, 3, 4),
                Lists.newArrayList(1, 3, 5), Lists.newArrayList(1, 2, 3, 5), Lists.newArrayList(1, 3, 4, 5, 6),
                Lists.newArrayList(1, 4, 6), Lists.newArrayList(1), Lists.newArrayList(1, 5),
                Lists.newArrayList(1, 2, 3, 4, 5), Lists.newArrayList(2, 3), Lists.newArrayList(2),
                Lists.newArrayList(1, 2, 3, 4, 5, 6));
        Assert.assertEquals(actualDims.size(), expectedDims.size());
        Assert.assertTrue(actualDims.containsAll(expectedDims));
    }

    @Test
    public void testGenCuboidsPartialEqual() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        val oldRule = newPlan.getRuleBasedIndex();
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");

        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [3],\n"
                                        + "          \"joint_dims\": [\n" + "            [1,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [0,1,2,3,4,5],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [[0,1,2]],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [\n"
                        + "            [3,4]\n" + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(20, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan,
                Lists.<List<Integer>> newArrayList(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6),
                        Lists.newArrayList(1, 3, 4, 5, 6), Lists.newArrayList(0, 1, 2, 3, 4),
                        Lists.newArrayList(0, 3, 4), Lists.newArrayList(0, 1), Lists.newArrayList(0, 1, 3, 4),
                        Lists.newArrayList(1, 3, 5), Lists.newArrayList(3, 4), Lists.newArrayList(0, 1, 2),
                        Lists.newArrayList(0)));
    }

    @Test
    public void testDiffRuleBasedIndex() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        IndexPlan indexPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        val newRule = new NRuleBasedIndex();
        newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
        val group1 = JsonUtil.readValue(""
                        + "{\n"
                        + "        \"includes\": [1, 3, 4, 5, 6],\n"
                        + "        \"measures\": [100001, 100002],\n"
                        + "        \"select_rule\": {\n"
                        + "          \"hierarchy_dims\": [],\n"
                        + "          \"mandatory_dims\": [3],\n"
                        + "          \"joint_dims\": [\n"
                        + "            [1, 5],\n"
                        + "            [4 ,6]\n"
                        + "          ]\n"
                        + "        }\n"
                        + "}", NAggregationGroup.class);
        val group2 = JsonUtil.readValue(""
                + "      {\n"
                + "        \"includes\": [0, 1, 2, 3, 4, 5],\n"
                + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [[0, 1, 2]],\n"
                + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": [\n"
                + "            [3 ,4]\n"
                + "          ]\n"
                + "        }\n"
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Arrays.asList(group1, group2));

        val result = indexPlan.diffRuleBasedIndex(newRule);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getFirst()) && CollectionUtils.isNotEmpty(result.getSecond()));
        Assert.assertTrue(result.getFirst().stream().map(LayoutEntity::getId).collect(Collectors.toSet()).contains(30001L));
        Assert.assertTrue(result.getSecond().stream().anyMatch(layoutEntity ->
                layoutEntity.getOrderedMeasures().containsKey(100001)
                        && layoutEntity.getOrderedMeasures().containsKey(100002)));
    }

    @Test
    public void testSetRuleBasedIndex() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(12, newPlan.getAllLayouts().size());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");
        NDataflow df = dataflowManager.getDataflow(newPlan.getId());

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, df.getLatestReadySegment().getId(), 30001L));
        dataflowManager.updateDataflow(update);

        val newRule = new NRuleBasedIndex();
        newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
        val group1 = JsonUtil.readValue(""
                + "{\n"
                + "        \"includes\": [1, 3, 4, 5, 6],\n"
                + "        \"measures\": [100001, 100002],\n"
                + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n"
                + "          \"mandatory_dims\": [3],\n"
                + "          \"joint_dims\": [\n"
                + "            [1, 5],\n"
                + "            [4 ,6]\n"
                + "          ]\n"
                + "        }\n"
                + "}", NAggregationGroup.class);
        val group2 = JsonUtil.readValue(""
                + "      {\n"
                + "        \"includes\": [0, 1, 2, 3, 4, 5],\n"
                + "        \"measures\": [\n" +
                "      100001,\n" +
                "      100002,\n" +
                "      100003\n" +
                "    ],\n"
                + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [[0, 1, 2]],\n"
                + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": [\n"
                + "            [3 ,4]\n"
                + "          ]\n"
                + "        }\n"
                + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Arrays.asList(group1, group2));

        val newIndexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(newRule, false, true);
        });

        Assert.assertTrue(CollectionUtils.isNotEmpty(newIndexPlan.getToBeDeletedIndexes()));
        Assert.assertTrue(newIndexPlan.getToBeDeletedIndexes().stream().anyMatch(indexEntity -> indexEntity.getId() == 30000L));

        val onlyDeleteRule = new NRuleBasedIndex();
        onlyDeleteRule.setDimensions(Arrays.asList(1, 3, 4, 5, 6));
        onlyDeleteRule.setAggregationGroups(Lists.newArrayList(JsonUtil.deepCopyQuietly(group1, NAggregationGroup.class)));
        val onlyDeleteIndexPlan = indexPlanManager.updateIndexPlan(newIndexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(30001L), true, true);
            copyForWrite.setRuleBasedIndex(onlyDeleteRule, false, true);
        });

        Assert.assertTrue(CollectionUtils.isEmpty(onlyDeleteIndexPlan.getToBeDeletedIndexes()));
    }

    @Test
    public void testSetRuleAgain() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        dataflowManager.createDataflow(newPlan, "ADMIN");
        NDataflow df = dataflowManager.getDataflow(newPlan.getId());

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, df.getLatestReadySegment().getId(), 30001L));
        dataflowManager.updateDataflow(update);

        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [3],\n"
                                        + "          \"joint_dims\": [\n" + "            [1,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [0,1,2,3,4,5],\n"
                        + "        \"select_rule\": {\n" + "          \"hierarchy_dims\": [[0,1,2]],\n"
                        + "          \"mandatory_dims\": [],\n" + "          \"joint_dims\": [\n"
                        + "            [3,4]\n" + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule, false, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        val copy = indexPlan.copy();
        copy.setRuleBasedIndex(copy.getRuleBasedIndex(), true);
        Assert.assertEquals(JsonUtil.writeValueAsIndentString(indexPlan), JsonUtil.writeValueAsIndentString(copy));
        Assert.assertTrue(CollectionUtils.isNotEmpty(indexPlan.getToBeDeletedIndexes()));
    }

    @Test
    public void testAddBlackListLayout() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        NRuleBasedIndex oldRuleBasedIndex = newPlan.getRuleBasedIndex();
        val indexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            copyForWrite.addRuleBasedBlackList(oldRuleBasedIndex.getLayoutIdMapping().subList(0, 2));
        });

        Assert.assertTrue(indexPlan.getRuleBasedIndex().getLayoutBlackList().size() == 2);
        Assert.assertEquals(indexPlan.getAllLayouts().size() + 2, newPlan.getAllLayouts().size());
        Set<Long> originalPlanLayoutIds = newPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        Set<Long> newPlanLayoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        originalPlanLayoutIds.removeAll(newPlanLayoutIds);
        Assert.assertTrue(CollectionUtils.isEqualCollection(originalPlanLayoutIds,
                indexPlan.getRuleBasedIndex().getLayoutBlackList()));
    }

    @Test
    public void testGenCuboidsForDifferentAggMeasures() throws IOException {
        val indexPlanManager = getIndexPlanManager();
        var indexPlan = getTmpTestIndexPlan("/ncube_rule_different_measure.json");
        indexPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), indexPlan);

        indexPlan = indexPlanManager.createIndexPlan(indexPlan);

        val colOrders = indexPlan.getAllLayouts().stream().map(layout -> Lists.<Integer> newArrayList(layout.getColOrder()))
                .collect(Collectors.toSet());

        testAgg1(colOrders);
        testAgg2(colOrders);
        testAgg3(colOrders);
        testAgg4(colOrders);
        testAgg5(colOrders);
        testAgg6(colOrders);

        // intersection case: (d1, d2, d3, m1), (d1, d2, d3, m2, m3)
        // (0, 1, 2, 3, 6) grows on agg 1, agg 2, agg 3
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100011, 100012, 100013, 100014)));
        // (16, 19, 20) grows on agg 4, agg 5, agg 6
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100013, 100014, 100015, 100016)));

        // base cuboid
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16,
                17, 18, 19, 20, 100000, 100001, 100002, 100003, 100004, 100005, 100007, 100008, 100009, 100010, 100011,
                100012, 100013, 100014, 100015, 100016)));

    }

    private void testAgg1(Set<ArrayList<Integer>> colOrders) {
        // agg 1 contains
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 15, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 4, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 4, 5, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 7, 8, 100000, 100001, 100002, 100003)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 9, 13, 14, 100000, 100001, 100002, 100003)));

        // agg 1 not contains
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 5, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(4, 5, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(7, 8, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 7, 100000, 100001, 100002, 100003)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 13, 14, 100000, 100001, 100002, 100003)));

    }

    private void testAgg2(Set<ArrayList<Integer>> colOrders) {
        // agg 2 contains
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 14, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 15, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 6, 14, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 6, 15, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 7, 8, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 4, 5, 6, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 4, 5, 6, 7, 8, 9, 13, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 100000,
                100001, 100004, 100005, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 100000,
                100001, 100004, 100005, 100016)));

        // agg 2 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(1, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(2, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 9, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 3, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 3, 4, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 5, 6, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 5, 6, 9, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 100000, 100001, 100004, 100005, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 6, 7, 9, 100000, 100001, 100004, 100005, 100016)));

    }

    private void testAgg3(Set<ArrayList<Integer>> colOrders) {
        // agg 3 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 9, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 14, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 8, 9, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 13, 14, 15, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 6, 7, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders
                .contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(
                Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 100011, 100012, 100013, 100014)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15,
                100011, 100012, 100013, 100014)));

        // agg 3 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(0, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 5, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 7, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 8, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 9, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 13, 100011, 100012, 100013, 100014)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(0, 1, 2, 3, 14, 100011, 100012, 100013, 100014)));

    }

    private void testAgg4(Set<ArrayList<Integer>> colOrders) {
        // agg 4 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100005, 100007, 100008)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 20, 100005, 100007, 100008)));

        // agg 4 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 19, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 19, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 20, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 100005, 100007, 100008)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 100005, 100007, 100008)));

    }

    private void testAgg5(Set<ArrayList<Integer>> colOrders) {
        // agg 5 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100000, 100001, 100002)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100000, 100001, 100002)));

        // agg 5 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(19, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100000, 100001, 100002)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 20, 100000, 100001, 100002)));

    }

    private void testAgg6(Set<ArrayList<Integer>> colOrders) {
        // agg 6 contains
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 100013, 100014, 100015, 100016)));
        Assert.assertTrue(
                colOrders.contains(Lists.<Integer> newArrayList(16, 17, 18, 19, 20, 100013, 100014, 100015, 100016)));

        // agg 6 not contains
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(16, 18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 18, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(17, 19, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(colOrders.contains(Lists.<Integer> newArrayList(18, 19, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(18, 19, 20, 100013, 100014, 100015, 100016)));
        Assert.assertFalse(
                colOrders.contains(Lists.<Integer> newArrayList(17, 18, 19, 100013, 100014, 100015, 100016)));

    }

    @Test
    public void testGenCuboidWithoutBaseCuboid() throws IOException {
        getTestConfig().setProperty("kylin.cube.aggrgroup.is-base-cuboid-always-valid", "false");
        val indexPlanManager = getIndexPlanManager();
        var newPlan = getTmpTestIndexPlan("/ncude_rule_based.json");

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        Assert.assertEquals(11, newPlan.getAllLayouts().size());
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [1,3,4,5,6],\n"
                        + "        \"measures\": [100000, 100001],\n" + "        \"select_rule\": {\n"
                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [1],\n"
                        + "          \"joint_dims\": [\n" + "            [3,5],\n" + "            [4,6]\n"
                        + "          ]\n" + "        }\n" + "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" + "      {\n" + "        \"includes\": [1,2,3,4,5],\n"
                        + "        \"measures\": [100002, 100003],\n" + "        \"select_rule\": {\n"
                        + "          \"hierarchy_dims\": [[2,3,4]],\n" + "          \"mandatory_dims\": [],\n"
                        + "          \"joint_dims\": [\n" + "            [1,5]\n" + "          ]\n" + "        }\n"
                        + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                log.error("Something wrong happened when update this IndexPlan.", e);
            }
        });

        val colOrders = indexPlan.getAllLayouts().stream().map(layout -> layout.getColOrder())
                .collect(Collectors.toSet());

        // does not contain base cuboid
        Assert.assertFalse(colOrders
                .contains(Lists.<Integer> newArrayList(1, 2, 3, 4, 5, 6, 100000, 100001, 100002, 100003, 100004, 100005,
                        100007, 100008, 100009, 100010, 100011, 100012, 100013, 100014, 100015, 100016)));
    }

    private NIndexPlanManager getIndexPlanManager() {
        return NIndexPlanManager.getInstance(getTestConfig(), "default");
    }

    private IndexPlan getTmpTestIndexPlan(String name) throws IOException {
        return JsonUtil.readValue(getClass().getResourceAsStream(name), IndexPlan.class);
    }

    private void logLayouts(List<LayoutEntity> layouts) {
        layouts.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        for (LayoutEntity allCuboidLayout : layouts) {
            log.debug("id:{}, auto:{}, manual:{}, {}", allCuboidLayout.getId(), allCuboidLayout.isAuto(),
                    allCuboidLayout.isManual(), allCuboidLayout.getColOrder());
        }
    }

    private void checkIntersection(NRuleBasedIndex oldRule, IndexPlan plan, List<List<Integer>> colOrders) {
        Set<LayoutEntity> originLayouts = oldRule.genCuboidLayouts();
        Set<LayoutEntity> targetLayouts = plan.getRuleBasedIndex().genCuboidLayouts();

        val intersection = Sets.intersection(originLayouts, targetLayouts).stream()
                .map(diffLayout -> diffLayout.getOrderedDimensions().keySet().asList()).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(intersection, colOrders));
    }
}
