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

import static io.kyligence.kap.metadata.recommendation.candidate.RawRecItem.RawRecState.INITIAL;
import static io.kyligence.kap.metadata.recommendation.candidate.RawRecItem.RawRecState.RECOMMENDED;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.ref.LayoutRef;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;

public class OptRecV2DisplayStateTest extends OptRecV2TestBase {

    public OptRecV2DisplayStateTest() {
        super("../core-metadata/src/test/resources/rec_v2/state",
                new String[] { "1cc98309-f963-4808-aa4a-bfe025c21935" });
    }

    @Test
    public void testAddLayoutDisplay() throws Exception {
        prepareEnv(Lists.newArrayList(3, 14));
        OptRecV2 recommendation = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        Map<Integer, LayoutRef> addRef = recommendation.getAdditionalLayoutRefs();
        Assert.assertEquals(2, addRef.size());
        addRef.forEach((k, ref) -> {
            RawRecItem recItem = recommendation.getRawRecItemMap().get(-ref.getId());
            Assert.assertEquals(RECOMMENDED, recItem.getState());
        });
    }

    @Test
    public void testRemLayoutDisplay() throws Exception {
        // at present, all removal layout recommendation will display
        prepareEnv(Lists.newArrayList(15, 16));
        OptRecV2 recommendation = OptRecManagerV2.getInstance(getProject()).loadOptRecV2(getDefaultUUID());
        recommendation.initRecommendation();
        Map<Integer, LayoutRef> removeRef = recommendation.getRemovalLayoutRefs();
        Assert.assertEquals(4, removeRef.size());
        removeRef.forEach((k, ref) -> {
            RawRecItem recItem = recommendation.getRawRecItemMap().get(-ref.getId());
            Assert.assertEquals(INITIAL, recItem.getState());
        });
    }
}
