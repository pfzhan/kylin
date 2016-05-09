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

public class SequenceSQLResultTest {

    SequenceSQLResult a;
    SequenceSQLResult b;

    @Before
    public void prepare() {
        List<List<String>> aResults = Lists.newArrayList();
        aResults.add(Lists.newArrayList("123", "456"));
        aResults.add(Lists.newArrayList("123", "567"));

        List<List<String>> bResults = Lists.newArrayList();
        bResults.add(Lists.newArrayList("123", "456"));
        bResults.add(Lists.newArrayList("123", "678"));

        a = new SequenceSQLResult(new SQLResponse(null, aResults, 0, true, null));
        b = new SequenceSQLResult(new SQLResponse(null, bResults, 0, true, null));
    }

    @Test
    public void testUnion() {
        a.union(b);
        Assert.assertEquals(3, a.results.size());
    }

    @Test
    public void testIntersect() {
        a.intersect(b);
        Assert.assertEquals(1, a.results.size());
    }

    @Test
    public void testForwardExcept() {
        a.forwardExcept(b);
        Assert.assertEquals(1, a.results.size());
        Assert.assertEquals("567", a.results.get(0).get(1));
    }

    @Test
    public void testBackwardExcept() {
        a.backwardExcept(b);
        Assert.assertEquals(1, a.results.size());
        Assert.assertEquals("678", a.results.get(0).get(1));
    }
}
