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

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;

public class OptRecV2CCTest extends OptRecV2TestBase {

    public OptRecV2CCTest() {
        super("../core-metadata/src/test/resources/rec_v2/cc",
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
