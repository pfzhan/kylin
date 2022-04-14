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




package io.kyligence.kap.rest.service;

import java.util.ArrayList;
import org.apache.kylin.rest.util.PagingUtil;
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
    public void testFuzzyMatching() {
        ArrayList<String> noAccessList = Lists.newArrayList("a1", "AB1", "Ab1", "aB1", "abc1");
        Assert.assertEquals(Lists.newArrayList("AB1", "Ab1", "aB1", "abc1"), PagingUtil.getIdentifierAfterFuzzyMatching("ab", false, noAccessList));
        Assert.assertEquals(Lists.newArrayList("abc1"), PagingUtil.getIdentifierAfterFuzzyMatching("ab", true, noAccessList));
    }

    @Test
    public void testIsInCurrentPage() {
        Assert.assertTrue(PagingUtil.isInCurrentPage(0, 0, 5));
        Assert.assertTrue(PagingUtil.isInCurrentPage(6, 1, 5));
        Assert.assertFalse(PagingUtil.isInCurrentPage(11, 1, 5));
        Assert.assertFalse(PagingUtil.isInCurrentPage(1, 1, 5));
    }
}
