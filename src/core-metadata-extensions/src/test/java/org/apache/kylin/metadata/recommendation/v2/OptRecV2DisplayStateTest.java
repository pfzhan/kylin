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

import java.util.Map;

import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.ref.LayoutRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.apache.kylin.metadata.recommendation.candidate.RawRecItem.RawRecState.INITIAL;
import static org.apache.kylin.metadata.recommendation.candidate.RawRecItem.RawRecState.RECOMMENDED;

public class OptRecV2DisplayStateTest extends OptRecV2TestBase {

    public OptRecV2DisplayStateTest() {
        super("../../../kylin/src/core-metadata/src/test/resources/rec_v2/state",
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
