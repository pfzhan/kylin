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
