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

package org.apache.kylin.common;

import org.junit.Assert;
import org.junit.Test;

public class QueryTraceTest {

    @Test
    public void test() throws InterruptedException {
        QueryTrace trace = new QueryTrace();
        trace.startSpan("span 1");
        Thread.sleep(100);
        trace.startSpan("span 2");
        Thread.sleep(100);
        trace.endLastSpan();
        Assert.assertEquals(2, trace.spans().size());
        Assert.assertTrue(trace.getLastSpan().isPresent());
        Assert.assertEquals("span 2", trace.getLastSpan().get().name);
        assertTimeEqual(100, trace.getLastSpan().get().duration);

        trace.amendLast("span 2", trace.getLastSpan().get().start + trace.getLastSpan().get().getDuration() + 1000);
        assertTimeEqual(1100, trace.getLastSpan().get().duration);

        trace.startSpan("span 3");
        long duration = trace.calculateDuration("span 3",
                trace.getLastSpan().get().start + trace.getLastSpan().get().getDuration() + 1000);
        assertTimeEqual(999, duration);
    }

    @Test
    public void testGroups() {
        QueryTrace trace = new QueryTrace();
        trace.startSpan(QueryTrace.GET_ACL_INFO);
        trace.startSpan(QueryTrace.SQL_TRANSFORMATION);
        trace.startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
        trace.startSpan(QueryTrace.MODEL_MATCHING);
        trace.startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        trace.startSpan(QueryTrace.WAIT_FOR_EXECUTION);
        trace.startSpan(QueryTrace.EXECUTION);
        trace.startSpan(QueryTrace.FETCH_RESULT);
        trace.startSpan(QueryTrace.SQL_PUSHDOWN_TRANSFORMATION);
        trace.startSpan(QueryTrace.HIT_CACHE);
        trace.endLastSpan();

        for (QueryTrace.Span span : trace.spans()) {
            if (QueryTrace.PREPARATION.equals(span.getGroup())) {
                Assert.assertTrue(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            } else {
                Assert.assertFalse(QueryTrace.SPAN_GROUPS.containsKey(span.getName()));
            }
        }
    }

    private void assertTimeEqual(long expected, long actual) {
        Assert.assertTrue(Math.abs(expected - actual) < 1000);
    }

}
