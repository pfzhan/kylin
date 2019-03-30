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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.metadata.cube.CubeTestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
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
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" +
                        "        \"includes\": [1,3,4,5,6],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [],\n" +
                        "          \"mandatory_dims\": [1],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [3,5],\n" +
                        "            [4,6]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" +
                        "      {\n" +
                        "        \"includes\": [1,2,3,4,5],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [[2,3,4]],\n" +
                        "          \"mandatory_dims\": [],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [1,5]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(12, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.newArrayList(10001L, 30001L, 40001L, 80001L));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(10001L, 120001L, 30001L, 40001L, 80001L, 130001L, 140001L, 150001L, 160001L,
                        170001L, 180001L, 190001L)));
    }

    @Test
    public void testGenTooManyCuboids() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/enormous_rule_based_cube.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        try {
            indexPlanManager.createIndexPlan(newPlan);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Too many cuboids for the cube. Cuboid combination reached 41449 and limit is 40960. Abort calculation.", e.getMessage());
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
        Assert.assertEquals(13, newPlan.getAllLayouts().size());

        val indexPlan = indexPlanManager.updateIndexPlan(newPlan.getUuid(), copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" +
                        "        \"includes\": [1,3,4,5,6],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [],\n" +
                        "          \"mandatory_dims\": [1],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [3,5],\n" +
                        "            [4,6]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" +
                        "      {\n" +
                        "        \"includes\": [1,2,3,4,5],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [[2,3,4]],\n" +
                        "          \"mandatory_dims\": [],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [1,5]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(14, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.newArrayList(13001L, 15001L, 16001L, 20001L));
        Assert.assertThat(indexPlan.getRuleBasedIndex().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(13001L, 30001L, 15001L, 16001L, 20001L, 40001L, 50001L, 60001L, 2004L, 70001L, 80001L, 90001L)));
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
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            newRule.setMeasures(Arrays.asList(100000, 100001, 100002));
            try {
                val group1 = JsonUtil.readValue("{\n" +
                        "        \"includes\": [1,3,4,5,6],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [],\n" +
                        "          \"mandatory_dims\": [3],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [1,5],\n" +
                        "            [4,6]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" +
                        "      {\n" +
                        "        \"includes\": [0,1,2,3,4,5],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [[0,1,2]],\n" +
                        "          \"mandatory_dims\": [],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [3,4]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        logLayouts(indexPlan.getAllLayouts());

        Assert.assertEquals(20, indexPlan.getAllLayouts().size());
        checkIntersection(oldRule, indexPlan, Lists.newArrayList(1L, 20001L, 40001L, 50001L, 60001L, 70001L, 80001L, 90001L, 100001L, 110001L));
    }

    @Test
    public void testSetRuleAgain() throws Exception {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), IndexPlan.class);
        newPlan.setLastModified(0L);

        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);

        newPlan = indexPlanManager.createIndexPlan(newPlan);
        logLayouts(newPlan.getAllLayouts());
        Assert.assertEquals(12, newPlan.getAllLayouts().size());
        val indexPlan = indexPlanManager.updateIndexPlan("84e5fd14-09ce-41bc-9364-5d8d46e6481a", copyForWrite -> {
            val newRule = new NRuleBasedIndex();
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            try {
                val group1 = JsonUtil.readValue("{\n" +
                        "        \"includes\": [1,3,4,5,6],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [],\n" +
                        "          \"mandatory_dims\": [3],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [1,5],\n" +
                        "            [4,6]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                val group2 = JsonUtil.readValue("" +
                        "      {\n" +
                        "        \"includes\": [0,1,2,3,4,5],\n" +
                        "        \"select_rule\": {\n" +
                        "          \"hierarchy_dims\": [[0,1,2]],\n" +
                        "          \"mandatory_dims\": [],\n" +
                        "          \"joint_dims\": [\n" +
                        "            [3,4]\n" +
                        "          ]\n" +
                        "        }\n" +
                        "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Arrays.asList(group1, group2));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        val copy = indexPlan.copy();
        copy.setRuleBasedIndex(copy.getRuleBasedIndex(), true);
        Assert.assertEquals(JsonUtil.writeValueAsIndentString(indexPlan), JsonUtil.writeValueAsIndentString(copy));
    }
    private void logLayouts(List<LayoutEntity> layouts) {
        layouts.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        for (LayoutEntity allCuboidLayout : layouts) {
            log.debug("id:{}, auto:{}, manual:{}, {}", allCuboidLayout.getId(),
                    allCuboidLayout.isAuto(), allCuboidLayout.isManual(), allCuboidLayout.getColOrder());
        }
    }

    private void checkIntersection(NRuleBasedIndex oldRule, IndexPlan plan, Collection<Long> ids) {
        Set<LayoutEntity> originLayouts = oldRule.genCuboidLayouts();
        Set<LayoutEntity> targetLayouts = plan.getRuleBasedIndex().genCuboidLayouts();

        val difference = Maps.difference(Maps.asMap(originLayouts, LayoutEntity::getId), Maps.asMap(targetLayouts, input -> input.getId()));
        Assert.assertTrue(CollectionUtils.isEqualCollection(difference.entriesInCommon().values(), ids));
    }
}
