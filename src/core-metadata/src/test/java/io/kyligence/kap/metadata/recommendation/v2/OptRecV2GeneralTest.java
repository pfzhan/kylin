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

package io.kyligence.kap.metadata.recommendation.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class OptRecV2GeneralTest extends OptRecV2BaseTest {

    public OptRecV2GeneralTest() {
        super("../core-metadata/src/test/resources/rec_v2/general",
                new String[] { "a4f5117e-a609-4750-8c04-a73fa7959227" });
    }

    /**
     * ID = 3, Agg RawRecItem doesn't depend on ComputedColumn
     */
    @Test
    public void testInitRecommendationOfAggIndex() throws Exception {
        recommendItem(Lists.newArrayList(3));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-1, 8))
                .addMeasureDep(ImmutableMap.of(-2, Lists.newArrayList(3), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-3, Lists.newArrayList(-1, 100000, -2))).addCCDep(ImmutableMap.of());

        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));
    }

    /**
     * ID = 6, dim RawRecItem depend on a ComputedColumn on model
     */
    @Test
    public void testInitRecommendationOfDimIndexWithCCOnModel() throws IOException {
        recommendItem(Lists.newArrayList(6));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-4, 26))
                .addMeasureDep(ImmutableMap.of(-5, Lists.newArrayList(5), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-6, Lists.newArrayList(-4, 100000, -5))).addCCDep(ImmutableMap.of());

        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));
    }

    @Test
    public void testInitRecommendationOfAggIndexWithModelCC() throws IOException {
        recommendItem(Lists.newArrayList(9));
        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-7, 12))
                .addMeasureDep(ImmutableMap.of(-8, Lists.newArrayList(26), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-9, Lists.newArrayList(-7, 100000, -8))).addCCDep(ImmutableMap.of());

        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));
    }

    @Test
    public void testInitRecommendationOfAggIndexWithProposedCC() throws IOException {
        recommendItem(Lists.newArrayList(13));
        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27).addDimDep(ImmutableMap.of(-11, 15))
                .addMeasureDep(ImmutableMap.of(-12, Lists.newArrayList(-10), 100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-13, Lists.newArrayList(-11, 100000, -12)))
                .addCCDep(ImmutableMap.of(-10, ImmutableList.of(5, 3)));
        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));

    }

    @Test
    public void testInitRecommendationOfTableIndex() throws IOException {
        recommendItem(Lists.newArrayList(18));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27)
                .addDimDep(ImmutableMap.of(-16, 1, -15, 17, -17, 13))
                .addMeasureDep(ImmutableMap.of(100000, Lists.newArrayList(100000)))
                .addLayDep(ImmutableMap.of(-18, Lists.newArrayList(-16, -17, -15))).addCCDep(ImmutableMap.of());
        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));

    }

    @Test
    public void testInitRecommendationOfReuseDimAndMeasure() throws IOException {
        recommendItem(Lists.newArrayList(3, 28));

        Dependency.Builder depBuilder = new Dependency.Builder().addColSize(27)
                .addDimDep(ImmutableMap.of(-1, 8, -27, 23))
                .addMeasureDep(ImmutableMap.of(100000, Lists.newArrayList(100000), -2, Lists.newArrayList(3)))
                .addLayDep(ImmutableMap.of(-3, Lists.newArrayList(-1, 100000, -2), -28,
                        Lists.newArrayList(-27, -1, 100000, -2)))
                .addCCDep(ImmutableMap.of());
        checkAllDependency(depBuilder.builder(), new OptRecV2(getProject(), getDefaultUUID()));

    }

    @Test
    public void testInitErrorForColumnOnModelMissing() throws IOException {
        recommendItem(Lists.newArrayList(29));
        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Assert.assertTrue(optRecV2.getBrokenLayoutRefIds().contains(29));
    }

    @Test
    public void testInitErrorForMeasureOnModelMissing() throws IOException {
        recommendItem(Lists.newArrayList(31));
        OptRecV2 optRecV2 = new OptRecV2(getProject(), getDefaultUUID());
        Assert.assertTrue(optRecV2.getBrokenLayoutRefIds().contains(31));
    }

}
