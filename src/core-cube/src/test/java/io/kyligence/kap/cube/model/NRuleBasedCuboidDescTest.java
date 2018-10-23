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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
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
        newPlan.setProject("default");
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(12, newPlan.getAllCuboidLayouts().size());
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
                    copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        logLayouts(cubePlan.getAllCuboidLayouts());

        Assert.assertEquals(24, cubePlan.getAllCuboidLayouts().size());
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 4, 6, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 3, 5, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 3, 4, 5, 6, 1000, 1001, 1002)));
        checkIntersection(cubePlan, Lists.newArrayList(10000001001L, 10000003001L, 10000004001L, 10000008001L));
        Assert.assertThat(cubePlan.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getCuboidIdMapping(),
                CoreMatchers.is(Arrays.asList(10_000_001_000L, 10_000_013_000L, 10_000_003_000L,
                        10_000_004_000L, 10_000_008_000L, 10_000_017_000L, 10_000_018_000L, 10_000_019_000L,
                        10_000_020_000L, 10_000_021_000L, 10_000_022_000L, 10_000_023_000L)));
    }

    @Test
    public void testGenCuboidsPartialEqual() throws IOException {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/ncude_rule_based.json"), NCubePlan.class);
        newPlan.setProject("default");
        newPlan.setLastModified(0L);

        newPlan = cubePlanManager.createCubePlan(newPlan);
        logLayouts(newPlan.getAllCuboidLayouts());
        Assert.assertEquals(12, newPlan.getAllCuboidLayouts().size());
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
                    copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        logLayouts(cubePlan.getAllCuboidLayouts());

        Assert.assertEquals(32, cubePlan.getAllCuboidLayouts().size());
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 3, 5, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(3, 4, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 3, 4, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1, 3, 4, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(1, 3, 4, 5, 6, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1, 2, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1, 2, 3, 4, 1000, 1001, 1002)));
        Assert.assertEquals(2, countLayouts(cubePlan, Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 1000, 1001, 1002)));
        checkIntersection(cubePlan, Lists.newArrayList(10000000001L, 10000002001L, 10000004001L, 10000005001L,
                10000006001L, 10000007001L, 10000008001L, 10000009001L, 10000010001L, 10000011001L));
    }

    private void logLayouts(List<NCuboidLayout> layouts) {
        Collections.sort(layouts, new Comparator<NCuboidLayout>() {
            @Override
            public int compare(NCuboidLayout o1, NCuboidLayout o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        for (NCuboidLayout allCuboidLayout : layouts) {
            log.debug("id:{}, {}, {}", allCuboidLayout.getId(), allCuboidLayout.getColOrder(),
                    allCuboidLayout.getIndexType());
        }
    }

    private int countLayouts(NCubePlan plan, List<Integer> colOrder) {
        int count = 0;
        for (NCuboidLayout layout : plan.getAllCuboidLayouts()) {
            if (colOrder.equals(layout.getColOrder())) {
                count++;
            }
        }
        return count;
    }

    private void checkIntersection(NCubePlan plan, Collection<Long> ids) {
        val allLayouts = plan.getRuleBaseCuboidLayouts();
        val originLayouts = FluentIterable.from(allLayouts).filter(new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout input) {
                return input.getVersion() == 1;
            }
        }).toSet();
        val targetLayouts = FluentIterable.from(allLayouts).filter(new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout input) {
                return input.getVersion() == 2;
            }
        }).toSet();

        val difference = Maps.difference(Maps.asMap(originLayouts, new Function<NCuboidLayout, Long>() {
            @Override
            public Long apply(NCuboidLayout input) {
                return input.getId();
            }
        }), Maps.asMap(targetLayouts, new Function<NCuboidLayout, Long>() {
            @Override
            public Long apply(NCuboidLayout input) {
                return input.getId();
            }
        }));
        Assert.assertTrue(CollectionUtils.isEqualCollection(difference.entriesInCommon().values(), ids));
    }
}
