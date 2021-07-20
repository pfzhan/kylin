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

package io.kyligence.kap.metadata.cube.cuboid;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class CuboidSchedulerTest extends NLocalFileMetadataTestCase {

    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test2403_dimCap3() throws IOException {
        IndexPlan cube = utCube("2.1.0.20403", 3);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(19, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(15, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(31, set.size());
        }
    }

    @Test
    public void test2403_dimCap2() throws IOException {

        IndexPlan cube = utCube("2.1.0.20403", 2);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(15, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(11, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(24, set.size());
        }
    }

    @Test
    public void test2403_dimCap1() throws IOException {

        IndexPlan cube = utCube("2.1.0.20403", 1);

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(0);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(6, set.size());
        }

        {
            NAggregationGroup agg = cube.getRuleBasedIndex().getAggregationGroups().get(1);
            val set = cube.getRuleBasedIndex().getCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(5, set.size());
        }

        {
            val set = cube.getRuleBasedIndex().getCuboidScheduler().getAllColOrders();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(11, set.size());
        }
    }

    @Test
    public void testMaskIsZero() throws IOException {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan cube = mgr.getIndexPlan("82fa7671-a935-45f5-8779-85703601f49a");
        cube = JsonUtil.deepCopy(cube, IndexPlan.class);
        cube.setIndexes(Lists.<IndexEntity> newArrayList());
        cube.initAfterReload(getTestConfig(), DEFAULT_PROJECT);
        val rule = new RuleBasedIndex();
        rule.setDimensions(Lists.<Integer> newArrayList());
        rule.setMeasures(Lists.<Integer> newArrayList());
        rule.setIndexPlan(cube);
        cube.setRuleBasedIndex(rule);
        val scheduler = (KECuboidSchedulerV1) cube.getRuleBasedIndex().getCuboidScheduler();
        Assert.assertEquals(0, scheduler.getAllColOrders().size());
    }

    private IndexPlan utCube(String resetVer, Integer resetDimCap) throws IOException {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan cube = mgr.getIndexPlan("82fa7671-a935-45f5-8779-85703601f49a");
        cube = JsonUtil.deepCopy(cube, IndexPlan.class);
        cube.setVersion(resetVer);

        if (resetDimCap != null) {
            for (NAggregationGroup g : cube.getRuleBasedIndex().getAggregationGroups())
                g.getSelectRule().dimCap = resetDimCap;
        }
        cube.initAfterReload(getTestConfig(), DEFAULT_PROJECT);
        return cube;
    }
}