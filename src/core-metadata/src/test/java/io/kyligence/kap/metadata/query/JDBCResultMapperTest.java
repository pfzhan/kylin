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

package io.kyligence.kap.metadata.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class JDBCResultMapperTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryHistoryResultMapper() {
        List<Map<String, Object>> rdbmsResultBySql = Lists.newArrayList();
        Map<String, Object> rdbmsResult1 = Maps.newHashMap();
        rdbmsResult1.put(QueryHistory.QUERY_ID, "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        rdbmsResult1.put(QueryHistory.SQL_TEXT, "select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        rdbmsResult1.put(QueryHistory.SQL_PATTERN, "SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        rdbmsResult1.put(QueryHistory.QUERY_DURATION, 5578L);
        rdbmsResult1.put(QueryHistory.TOTAL_SCAN_BYTES, 863L);
        rdbmsResult1.put(QueryHistory.TOTAL_SCAN_COUNT, 4096L);
        rdbmsResult1.put(QueryHistory.RESULT_ROW_COUNT, 500L);
        rdbmsResult1.put(QueryHistory.SUBMITTER, "ADMIN");
        rdbmsResult1.put(QueryHistory.REALIZATIONS, "0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
        rdbmsResult1.put(QueryHistory.QUERY_SERVER, "192.168.1.6:7070");
        rdbmsResult1.put(QueryHistory.ERROR_TYPE, "");
        rdbmsResult1.put(QueryHistory.IS_CACHE_HIT, true);
        rdbmsResult1.put(QueryHistory.IS_INDEX_HIT, true);
        rdbmsResult1.put(QueryHistory.QUERY_TIME, 1584888338274L);
        rdbmsResult1.put(QueryHistory.PROJECT_NAME, "default");
        rdbmsResultBySql.add(rdbmsResult1);
        List<QueryHistory> queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(rdbmsResultBySql);
        Assert.assertEquals("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", queryHistoryList.get(0).getQueryId());
        Assert.assertEquals("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500", queryHistoryList.get(0).getSql());
        Assert.assertEquals("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1",
                queryHistoryList.get(0).getSqlPattern());
        Assert.assertEquals(5578L, queryHistoryList.get(0).getDuration());
        Assert.assertEquals(863L, queryHistoryList.get(0).getTotalScanBytes());
        Assert.assertEquals(4096L, queryHistoryList.get(0).getTotalScanCount());
        Assert.assertEquals(500L, queryHistoryList.get(0).getResultRowCount());
        Assert.assertEquals("ADMIN", queryHistoryList.get(0).getQuerySubmitter());
        Assert.assertEquals("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index",
                queryHistoryList.get(0).getQueryRealizations());

        Assert.assertEquals("192.168.1.6:7070", queryHistoryList.get(0).getHostName());
        Assert.assertEquals("", queryHistoryList.get(0).getErrorType());
        Assert.assertEquals(true, queryHistoryList.get(0).isCacheHit());
        Assert.assertEquals(true, queryHistoryList.get(0).isIndexHit());
        Assert.assertEquals(1584888338274L, queryHistoryList.get(0).getQueryTime());
        Assert.assertEquals("default", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testQueryStatisticsResultMapper() {
        int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
        List<Map<String, Object>> rdbmsResultBySql = Lists.newArrayList();
        Map<String, Object> rdbmsResult1 = Maps.newHashMap();
        rdbmsResult1.put(JDBCResultMapper.COUNT, 30L);
        rdbmsResult1.put("time", Instant.ofEpochMilli(1584888338274L).minusMillis(offset).toEpochMilli());
        rdbmsResult1.put(JDBCResultMapper.AVG_DURATION, 1001L);
        rdbmsResult1.put(QueryHistory.MODEL, "771157c2-e6e2-4072-80c4-8ec25e1a83ea");
        Map<String, Object> rdbmsResult2 = Maps.newHashMap();
        rdbmsResultBySql.add(rdbmsResult1);
        rdbmsResultBySql.add(rdbmsResult2);

        List<QueryStatistics> queryStatistics = JDBCResultMapper.queryStatisticsResultMapper(rdbmsResultBySql);
        Assert.assertEquals(30L, queryStatistics.get(0).getCount());
        Assert.assertEquals(1584888338274L, queryStatistics.get(0).getTime().toEpochMilli());
        Assert.assertEquals(1001, queryStatistics.get(0).getMeanDuration(), 0.1);
        Assert.assertEquals("771157c2-e6e2-4072-80c4-8ec25e1a83ea", queryStatistics.get(0).getModel());
    }

    @Test
    public void testQueryHistoryCountResultMapper() {
        List<Map<String, Object>> rdbmsResultBySql = Lists.newArrayList();
        Map<String, Object> rdbmsResult1 = Maps.newHashMap();
        rdbmsResult1.put(JDBCResultMapper.COUNT, 20L);
        rdbmsResultBySql.add(rdbmsResult1);
        Assert.assertEquals(20L, JDBCResultMapper.queryHistoryCountResultMapper(rdbmsResultBySql));
    }

    @Test
    public void testFirstQHResultMapper() {
        List<Map<String, Object>> rdbmsResultBySql = Lists.newArrayList();
        Map<String, Object> rdbmsResult1 = Maps.newHashMap();
        rdbmsResult1.put(QueryHistory.QUERY_TIME, 1584888338274L);
        Map<String, Object> rdbmsResult2 = Maps.newHashMap();
        rdbmsResult2.put(QueryHistory.QUERY_TIME, 1584888338275L);
        rdbmsResultBySql.add(rdbmsResult1);
        rdbmsResultBySql.add(rdbmsResult2);

        List<QueryStatistics> queryStatistics = JDBCResultMapper.firstQHResultMapper(rdbmsResultBySql);
        Assert.assertEquals(1584888338274L, queryStatistics.get(0).getTime().toEpochMilli());
    }
}
