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

package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;

public class UseInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
    }

    @After
    public void teardown() {
        queryHistoryDAO.deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testExtractUseInfo() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        // 2022-05-13 10:00:00
        queryHistoryDAO.insert(createQueryMetrics(1652407200000L, 1000L, true, "default", true));
        UseInfoTool.extractUseInfo(mainDir, Long.MIN_VALUE, Long.MAX_VALUE);

        File useInfoDir = new File(mainDir, "use_info");
        Assert.assertTrue(new File(useInfoDir, "query_daily.csv").exists());
        Assert.assertTrue(new File(useInfoDir, "build_daily.csv").exists());
        Assert.assertTrue(new File(useInfoDir, "base").exists());
        List<String> lines = FileUtils.readLines(new File(useInfoDir, "query_daily.csv"), "utf-8");
        Assert.assertEquals(2, lines.size());
    }

    public static QueryMetrics createQueryMetrics(long queryTime, long duration, boolean indexHit, String project,
            boolean hitModel) {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(duration);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(indexHit);
        queryMetrics.setQueryTime(queryTime);
        queryMetrics.setQueryFirstDayOfMonth(TimeUtil.getMonthStart(queryTime));
        queryMetrics.setQueryFirstDayOfWeek(TimeUtil.getWeekStart(queryTime));
        queryMetrics.setQueryDay(TimeUtil.getDayStart(queryTime));
        queryMetrics.setProjectName(project);
        queryMetrics.setQueryStatus("SUCCEEDED");
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);

        if (hitModel) {
            QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001",
                    "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea",
                    Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
            realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
            realizationMetrics.setDuration(4591L);
            realizationMetrics.setQueryTime(1586405449387L);
            realizationMetrics.setProjectName(project);
            realizationMetrics.setModelId("82fa7671-a935-45f5-8779-85703601f49a.json");

            realizationMetrics.setSnapshots(
                    Lists.newArrayList(new String[] { "DEFAULT.TEST_KYLIN_ACCOUNT", "DEFAULT.TEST_COUNTRY" }));

            List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
            realizationMetricsList.add(realizationMetrics);
            realizationMetricsList.add(realizationMetrics);
            queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
        } else {
            queryMetrics.setEngineType(QueryHistory.EngineType.CONSTANTS.toString());
        }
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        return queryMetrics;
    }
}
