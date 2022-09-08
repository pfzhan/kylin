/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.metadata.recommendation.v2;

import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;


public class OptRecV2CCTest extends OptRecV2TestBase {

    public OptRecV2CCTest() {
        super("../../../kylin/src/core-metadata/src/test/resources/rec_v2/cc",
                new String[] { "1cc98309-f963-4808-aa4a-bfe025c21935", "66ee12c6-1fdf-48fc-8a79-44d7f503567c" });
    }

    @Test
    public void testInitRecommendationUseCC() throws Exception {
        prepareEnv(Lists.newArrayList(3));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(28).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-2, Lists.newArrayList(18), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-3, Lists.newArrayList(-1, 100000, -2))).addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);

    }

    @Test
    public void testInitRecommendationCCReuseCrossModel() throws Exception {
        prepareEnv(Lists.newArrayList(9));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(28).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-8, Lists.newArrayList(-7), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-9, Lists.newArrayList(-1, 100000, -8)))
                .addCCDep(ImmutableMap.of(-7, ImmutableList.of(10, 17))).setCCProperties(false, true);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationNewCC() throws Exception {
        prepareEnv(Lists.newArrayList(6));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(28).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-5, Lists.newArrayList(-4), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-6, Lists.newArrayList(-1, 100000, -5)))
                .addCCDep(ImmutableMap.of(-4, ImmutableList.of(10, 13)));

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationCCReuseSameModel() throws Exception {
        prepareEnv(Lists.newArrayList(12));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(28).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-11, Lists.newArrayList(27), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-12, Lists.newArrayList(-1, 100000, -11)))
                .addCCDep(ImmutableMap.of(27, ImmutableList.of(27))).setCCProperties(true, false);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }
}
