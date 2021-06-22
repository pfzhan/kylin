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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.config.initialize.QueryMetricsListener;
import lombok.val;

public class QueryMetricsListenerTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private QueryMetricsListener queryMetricsListener;

    @InjectMocks
    private QueryService queryService;

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setup() {
        createTestMetadata();
        queryMetricsListener = Mockito.spy(new QueryMetricsListener());
        queryService = Mockito.spy(new QueryService());
    }

    @Test
    public void testSqlsFormat() {
        List<String> sqls = Lists.newArrayList("select * from A", "select A.a, B.b from A join B on A.a2=B.b2",
                "Select sum(a), b from A group by b", "select * from A as c limit 1");

        List<String> expectedFormattedSqls = Lists.newArrayList("SELECT\n  *\nFROM \"A\"",
                "SELECT\n  \"A\".\"A\",\n  \"B\".\"B\"\nFROM \"A\"\n  INNER JOIN \"B\" ON \"A\".\"A2\" = \"B\".\"B2\"",
                "SELECT\n  SUM(\"A\"),\n  \"B\"\nFROM \"A\"\nGROUP BY\n  \"B\"",
                "SELECT\n  *\nFROM \"A\" AS \"C\"\nLIMIT 1");
        val formated = queryService.format(sqls);

        Assert.assertEquals(sqls.size(), formated.size());
        for (int n = 0; n < sqls.size(); n++) {
            Assert.assertEquals(expectedFormattedSqls.get(n), formated.get(n));
        }
    }

    @Test
    public void testAliasLengthMaxThanConfig() {
        //To check if the config kylin.model.dimension-measure-name.max-length worked for SqlParser
        List<String> sqls = Lists.newArrayList("select A.a as AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA from A");

        List<String> expectedFormattedSqls = Lists.newArrayList("SELECT\n"
                + "  \"A\".\"A\" AS \"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"\n" + "FROM \"A\"");

        val formated = queryService.format(sqls);

        Assert.assertEquals(sqls.size(), formated.size());
        for (int n = 0; n < sqls.size(); n++) {
            Assert.assertEquals(expectedFormattedSqls.get(n), formated.get(n));
        }
    }

    private long getCounterCount(MetricsName name, MetricsCategory category, String entity, Map<String, String> tags) {
        val counter = MetricsGroup.getCounter(name, category, entity, tags);
        return counter == null ? 0 : counter.getCount();
    }

    @Test
    public void testUpdateQueryTimeMetrics() {
        long a = getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long b = getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long c = getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long d = getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>());
        long e = getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>());
        queryMetricsListener.updateQueryTimeMetrics(123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(1123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(3123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(5123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d + 1,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
        queryMetricsListener.updateQueryTimeMetrics(10123, "default", new HashMap<>());
        Assert.assertEquals(a + 1,
                getCounterCount(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(b + 1,
                getCounterCount(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(c + 1,
                getCounterCount(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(d + 1,
                getCounterCount(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, "default", new HashMap<>()));
        Assert.assertEquals(e + 1,
                getCounterCount(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, "default", new HashMap<>()));
    }

}
