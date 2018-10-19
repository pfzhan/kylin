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

import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        newPlan.setProject("default");
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        Assert.assertEquals(8, newPlan.getAllCuboidLayouts().size());
        val cubePlan = cubePlanManager.updateCubePlan("ncube_rule_based", new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val newRule = new NRuleBasedCuboidsDesc();
                newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
                newRule.setMeasures(Arrays.asList(1000, 1001, 1002));
                try {
                    val group1 = JsonUtil.readValue("{\n" +
                            "        \"includes\": [1,3,4,5,6],\n" +
                            "        \"select_rule\": {\n" +
                            "          \"hierarchy_dims\": [],\n" +
                            "          \"mandatory_dims\": [1],\n" +
                            "          \"joint_dims\": [\n" +
                            "            [3,5],\n" +
                            "            [4,6]\n" +
                            "          ],\n" +
                            "          \"dim_cap\": 1\n" +
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
                            "          ],\n" +
                            "          \"dim_cap\": 1\n" +
                            "        }\n" +
                            "}", NAggregationGroup.class);
                    newRule.setAggregationGroups(Arrays.asList(group1, group2));
                    copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        for (NCuboidLayout allCuboidLayout : cubePlan.getAllCuboidLayouts()) {
            log.debug("key: {}, {}, {}, {}, {}", allCuboidLayout, allCuboidLayout.getColOrder(), allCuboidLayout.getSortByColumns(), allCuboidLayout.getShardByColumns(), allCuboidLayout.getIndexType());
        }

        Assert.assertEquals(13, cubePlan.getAllCuboidLayouts().size());
    }

    @Test
    public void testGenCuboidsPartialEqual() throws IOException {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), NCubePlan.class);
        newPlan.setProject("default");
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        Assert.assertEquals(8, newPlan.getAllCuboidLayouts().size());
        for (NCuboidLayout allCuboidLayout : newPlan.getAllCuboidLayouts()) {
            log.debug("layout {}", allCuboidLayout.getColOrder());
        }
        val cubePlan = cubePlanManager.updateCubePlan("ncube_rule_based", new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
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
                            "          ],\n" +
                            "          \"dim_cap\": 1\n" +
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
                            "          ],\n" +
                            "          \"dim_cap\": 1\n" +
                            "        }\n" +
                            "}", NAggregationGroup.class);
                    newRule.setAggregationGroups(Arrays.asList(group1, group2));
                    copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        for (NCuboidLayout allCuboidLayout : cubePlan.getAllCuboidLayouts()) {
            log.debug("layout {}", allCuboidLayout.getColOrder());
        }

        Assert.assertEquals(12, cubePlan.getAllCuboidLayouts().size());
    }
}
