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

import java.io.IOException;

import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecV2GeneralTest extends OptRecV2TestBase {

    public OptRecV2GeneralTest() {
        super("../../../kylin/src/core-metadata/src/test/resources/rec_v2/general",
                new String[] { "a4f5117e-a609-4750-8c04-a73fa7959227" });
    }

    /**
     * ID = 3, Agg RawRecItem doesn't depend on ComputedColumn
     */
    @Test
    public void testInitRecommendationOfAggIndex() throws Exception {
        prepareEnv(Lists.newArrayList(3));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-2, Lists.newArrayList(3), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-3, Lists.newArrayList(-1, 100000, -2))).addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    /**
     * ID = 6, dim RawRecItem depend on a ComputedColumn on model
     */
    @Test
    public void testInitRecommendationOfDimIndexWithCCOnModel() throws IOException {
        prepareEnv(Lists.newArrayList(6));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-4, 26))
                .addMeasureDep(ImmutableMap.of(-5, Lists.newArrayList(5), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-6, Lists.newArrayList(-4, 100000, -5))).addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationOfAggIndexWithModelCC() throws IOException {
        prepareEnv(Lists.newArrayList(9));
        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-7, 12))
                .addMeasureDep(ImmutableMap.of(-8, Lists.newArrayList(26), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-9, Lists.newArrayList(-7, 100000, -8))).addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationOfAggIndexWithProposedCC() throws IOException {
        prepareEnv(Lists.newArrayList(13));
        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-11, 15))
                .addMeasureDep(ImmutableMap.of(-12, Lists.newArrayList(-10), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-13, Lists.newArrayList(-11, 100000, -12)))
                .addCCDep(ImmutableMap.of(-10, ImmutableList.of(5, 3)));

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationOfTableIndex() throws IOException {
        prepareEnv(Lists.newArrayList(18));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27)
                .addDimDep(ImmutableMap.of(-16, 1, -15, 17, -17, 13))
                .addMeasureDep(ImmutableMap.of(100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-18, Lists.newArrayList(-16, -17, -15))).addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitRecommendationOfReuseDimAndMeasure() throws IOException {
        prepareEnv(Lists.newArrayList(3, 28));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27)
                .addDimDep(ImmutableMap.of(-1, 8, -27, 23))
                .addMeasureDep(ImmutableMap.of(100000, Lists.newArrayList(100000), -2, Lists.newArrayList(3)))
                .addLayDep(ImmutableMap.of(-3, Lists.newArrayList(-1, 100000, -2), -28,
                        Lists.newArrayList(-27, -1, 100000, -2)))
                .addCCDep(ImmutableMap.of());

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        checkAllDependency(depBuilder.builder(), optRecV2);
    }

    @Test
    public void testInitErrorForColumnOnModelMissing() throws IOException {
        prepareEnv(Lists.newArrayList(29));
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        Assert.assertTrue(optRecV2.getBrokenRefIds().contains(29));
    }

    @Test
    public void testInitErrorForMeasureOnModelMissing() throws IOException {
        prepareEnv(Lists.newArrayList(29));
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        Assert.assertTrue(optRecV2.getBrokenRefIds().contains(29));
    }

}
