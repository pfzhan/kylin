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

package io.kyligence.kap.rest.controller;

import java.util.List;

import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import io.kyligence.kap.rest.sequencesql.SequenceNodeOutput;

public class SequenceNodeOutputTest {

    SequenceNodeOutput a;
    SequenceNodeOutput b;

    @Before
    public void prepare() {
        List<List<String>> aResults = Lists.newArrayList();
        aResults.add(Lists.newArrayList("123"));
        aResults.add(Lists.newArrayList("123"));
        aResults.add(Lists.newArrayList("456"));
        aResults.add(Lists.newArrayList("567"));

        List<List<String>> bResults = Lists.newArrayList();
        bResults.add(Lists.newArrayList("123"));
        bResults.add(Lists.newArrayList("789"));

        a = new SequenceNodeOutput(new SQLResponse(null, aResults, 0, true, null));
        b = new SequenceNodeOutput(new SQLResponse(null, bResults, 0, true, null));
    }

    @Test
    public void testUnion() {
        SequenceNodeOutput union = SequenceNodeOutput.union(a, b);
        Assert.assertEquals(4, union.getResults().size());
    }

    @Test
    public void testIntersect() {
        SequenceNodeOutput intersect = SequenceNodeOutput.intersect(a, b);
        Assert.assertEquals(1, intersect.getResults().size());
    }

    @Test
    public void testForwardExcept() {
        SequenceNodeOutput result = SequenceNodeOutput.except(a, b);
        Assert.assertEquals(2, result.getResults().size());
    }

    @Test
    public void testBackwardExcept() {
        SequenceNodeOutput result = SequenceNodeOutput.except(b, a);
        Assert.assertEquals(1, result.getResults().size());
        Assert.assertEquals("789", result.getResults().get(0).get(0));
    }
}
