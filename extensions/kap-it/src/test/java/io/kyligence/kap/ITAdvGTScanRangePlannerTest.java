/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package io.kyligence.kap;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageMockUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.gridtable.AdvGTScanRangePlanner;

@Ignore
public class ITAdvGTScanRangePlannerTest extends KAPHBaseMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testPrefixCuboid() {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("test_kylin_cube_with_slr_empty");
        CubeSegment segment = cube.getFirstSegment();

        List<TblColRef> groupList = StorageMockUtils.buildGroups();
        Set<TblColRef> groups = Sets.newHashSet(groupList);
        TupleFilter filter = StorageMockUtils.buildFilter3(groupList.get(0));

        List<FunctionDesc> metrics = StorageMockUtils.buildAggregations1();

        Cuboid cuboid = Cuboid.identifyCuboid(cube.getDescriptor(), groups, metrics);
        AdvGTScanRangePlanner planner = new AdvGTScanRangePlanner(segment, cuboid, filter, groups, groups, metrics);
        GTScanRequest scanRequest = planner.planScanRequest();
        System.out.println(scanRequest);

    }

}
