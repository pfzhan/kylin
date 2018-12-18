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
package io.kyligence.kap.cube.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
import io.kyligence.kap.cube.cuboid.NAggregationGroup;
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
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), NCubePlan.class);
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        val oldRule = newPlan.getRuleBasedCuboidsDesc();
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(12, newPlan.getAllCuboidLayouts().size());
        val cubePlan = cubePlanManager.updateCubePlan("ncube_rule_based", copyForWrite -> {
            val newRule = new NRuleBasedCuboidsDesc();
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
                copyForWrite.setRuleBasedCuboidsDesc(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(cubePlan.getAllCuboidLayouts());

        Assert.assertEquals(12, cubePlan.getAllCuboidLayouts().size());
        checkIntersection(oldRule, cubePlan, Lists.newArrayList(1001L, 3001L, 4001L, 8001L));
        Assert.assertThat(cubePlan.getRuleBasedCuboidsDesc().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(1001L, 12001L, 3001L, 4001L, 8001L, 13001L, 14001L, 15001L, 16001L,
                        17001L, 18001L, 19001L)));
    }

    @Test
    public void testGenCuboidsWithAuto() throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_mixed.json"), NCubePlan.class);
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        val oldRule = newPlan.getRuleBasedCuboidsDesc();
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(13, newPlan.getAllCuboidLayouts().size());

        val cubePlan = cubePlanManager.updateCubePlan("ncube_mixed", copyForWrite -> {
            val newRule = new NRuleBasedCuboidsDesc();
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
                copyForWrite.setRuleBasedCuboidsDesc(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        logLayouts(cubePlan.getAllCuboidLayouts());

        Assert.assertEquals(14, cubePlan.getAllCuboidLayouts().size());
        checkIntersection(oldRule, cubePlan, Lists.newArrayList(13001L, 15001L, 16001L, 20001L));
        Assert.assertThat(cubePlan.getRuleBasedCuboidsDesc().getLayoutIdMapping(),
                CoreMatchers.is(Arrays.asList(13001L, 23001L, 15001L, 16001L, 20001L, 24001L, 25001L, 26001L, 2004L, 27001L, 28001L, 29001L)));
    }

    @Test
    public void testGenCuboidsPartialEqual() throws IOException {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), NCubePlan.class);
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        val oldRule = newPlan.getRuleBasedCuboidsDesc();
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(12, newPlan.getAllCuboidLayouts().size());
        val cubePlan = cubePlanManager.updateCubePlan("ncube_rule_based", copyForWrite -> {
            val newRule = new NRuleBasedCuboidsDesc();
            newRule.setDimensions(Arrays.asList(0, 1, 2, 3, 4, 5, 6));
            newRule.setMeasures(Arrays.asList(1000, 1001, 1002));
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
                copyForWrite.setRuleBasedCuboidsDesc(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        logLayouts(cubePlan.getAllCuboidLayouts());

        Assert.assertEquals(20, cubePlan.getAllCuboidLayouts().size());
        checkIntersection(oldRule, cubePlan, Lists.newArrayList(1L, 2001L, 4001L, 5001L, 6001L, 7001L, 8001L, 9001L, 10001L, 11001L));
    }

    @Test
    public void testSetRuleAgain() throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), NCubePlan.class);
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(12, newPlan.getAllCuboidLayouts().size());
        val cubePlan = cubePlanManager.updateCubePlan("ncube_rule_based", copyForWrite -> {
            val newRule = new NRuleBasedCuboidsDesc();
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
                copyForWrite.setRuleBasedCuboidsDesc(newRule);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        val copy = cubePlan.copy();
        copy.setRuleBasedCuboidsDesc(copy.getRuleBasedCuboidsDesc(), true);
        Assert.assertEquals(JsonUtil.writeValueAsIndentString(cubePlan), JsonUtil.writeValueAsIndentString(copy));
    }

    private void logLayouts(List<NCuboidLayout> layouts) {
        layouts.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        for (NCuboidLayout allCuboidLayout : layouts) {
            log.debug("id:{}, auto:{}, manual:{}, {}", allCuboidLayout.getId(),
                    allCuboidLayout.isAuto(), allCuboidLayout.isManual(), allCuboidLayout.getColOrder());
        }
    }

    private void checkIntersection(NRuleBasedCuboidsDesc oldRule, NCubePlan plan, Collection<Long> ids) {
        Set<NCuboidLayout> originLayouts = oldRule.genCuboidLayouts();
        Set<NCuboidLayout> targetLayouts = plan.getRuleBasedCuboidsDesc().genCuboidLayouts();

        val difference = Maps.difference(Maps.asMap(originLayouts, NCuboidLayout::getId), Maps.asMap(targetLayouts, input -> input.getId()));
        Assert.assertTrue(CollectionUtils.isEqualCollection(difference.entriesInCommon().values(), ids));
    }
}
