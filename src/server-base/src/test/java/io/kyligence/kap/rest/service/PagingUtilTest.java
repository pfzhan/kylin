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


package io.kyligence.kap.rest.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kyligence.kap.rest.PagingUtil;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PagingUtilTest {
    @Test
    public void testPageCut() {
        ArrayList<String> list = Lists.newArrayList("a", "b", "c", "d", "e");
        Assert.assertEquals(Lists.newArrayList("d"), PagingUtil.cutPage(list, 3, 1));
    }

    @Test
    public void cutPageWithTwoList() {
        List<Integer> l1 = new ArrayList<>();
        List<Integer> l2 = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            l1.add(i);
        }
        for (int i = 0; i < 25; i++) {
            l2.add(i);
        }
        Pair<List<Integer>, List<Integer>> p0 = PagingUtil.cutPageWithTwoList(l1, l2, 0, 10);
        Pair<List<Integer>, List<Integer>> p1 = PagingUtil.cutPageWithTwoList(l1, l2, 1, 10);
        Pair<List<Integer>, List<Integer>> p2 = PagingUtil.cutPageWithTwoList(l1, l2, 2, 10);
        Pair<List<Integer>, List<Integer>> p3 = PagingUtil.cutPageWithTwoList(l1, l2, 3, 10);

        Assert.assertEquals(Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8), p0.getFirst());
        Assert.assertEquals(Lists.newArrayList(0), p0.getSecond());

        Assert.assertEquals(Collections.<Integer> emptyList(), p1.getFirst());
        Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p1.getSecond());

        Assert.assertEquals(Collections.<Integer> emptyList(), p2.getFirst());
        Assert.assertEquals(Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19, 20), p2.getSecond());

        Assert.assertEquals(Collections.<Integer> emptyList(), p3.getFirst());
        Assert.assertEquals(Lists.newArrayList(21, 22, 23, 24), p3.getSecond());
    }

    @Test
    public void testFuzzyMatching() {
        ArrayList<String> noAccessList = Lists.newArrayList("a1", "AB1", "Ab1", "aB1", "abc1");
        Assert.assertEquals(Lists.newArrayList("AB1", "Ab1", "aB1", "abc1"), PagingUtil.getIdentifierAfterFuzzyMatching("ab", false, noAccessList));
        Assert.assertEquals(Lists.newArrayList("abc1"), PagingUtil.getIdentifierAfterFuzzyMatching("ab", true, noAccessList));
    }
}
