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

package io.kyligence.kap.cube.cuboid;

import java.io.IOException;
import java.util.Set;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class KapCuboidSchedulerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test2402Before() throws IOException {
        CubeDesc cube = utCube("2.1.0", null);

        {
            AggregationGroup agg = cube.getAggregationGroups().get(0);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(7, set.size());
        }
        
        {
            AggregationGroup agg = cube.getAggregationGroups().get(1);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(1, set.size());
        }

        {
            Set<Long> set = cube.getInitialCuboidScheduler().getAllCuboidIds();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(8, set.size());
        }
    }

    @Test
    public void test2403_dimCap3() throws IOException {
        CubeDesc cube = utCube("2.1.0.20403", 3);

        {
            AggregationGroup agg = cube.getAggregationGroups().get(0);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(19, set.size());
        }

        {
            AggregationGroup agg = cube.getAggregationGroups().get(1);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(15, set.size());
        }

        {
            Set<Long> set = cube.getInitialCuboidScheduler().getAllCuboidIds();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(31, set.size());
        }
    }

    @Test
    public void test2403_dimCap2() throws IOException {
        CubeDesc cube = utCube("2.1.0.20403", 2);

        {
            AggregationGroup agg = cube.getAggregationGroups().get(0);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(15, set.size());
        }

        {
            AggregationGroup agg = cube.getAggregationGroups().get(1);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(11, set.size());
        }

        {
            Set<Long> set = cube.getInitialCuboidScheduler().getAllCuboidIds();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(24, set.size());
        }
    }

    @Test
    public void test2403_dimCap1() throws IOException {
        CubeDesc cube = utCube("2.1.0.20403", 1);

        {
            AggregationGroup agg = cube.getAggregationGroups().get(0);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg1 result");
            Assert.assertEquals(6, set.size());
        }

        {
            AggregationGroup agg = cube.getAggregationGroups().get(1);
            Set<Long> set = cube.getInitialCuboidScheduler().calculateCuboidsForAggGroup(agg);
            //KapCuboidScheduler2403.debugPrint(set, "agg2 result");
            Assert.assertEquals(5, set.size());
        }

        {
            Set<Long> set = cube.getInitialCuboidScheduler().getAllCuboidIds();
            //KapCuboidScheduler2403.debugPrint(set, "all result");
            Assert.assertEquals(11, set.size());
        }
    }

    private CubeDesc utCube(String resetVer, Integer resetDimCap) {
        CubeDescManager mgr = CubeDescManager.getInstance(getTestConfig());
        CubeDesc cube = mgr.getCubeDesc("ut_inner_join_cube_partial");
        cube.setVersion(resetVer);
        cube.setParentForward(256); // disable parent forward

        if (resetDimCap != null) {
            for (AggregationGroup g : cube.getAggregationGroups())
                g.getSelectRule().dimCap = resetDimCap;
        }

        cube.deInit();
        cube.init(getTestConfig());
        return cube;
    }
}
