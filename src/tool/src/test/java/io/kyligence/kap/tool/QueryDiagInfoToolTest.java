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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import io.kyligence.kap.tool.util.ZipFileUtil;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.TimeUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import io.kyligence.kap.tool.obf.KylinConfObfuscatorTest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipFile;

public class QueryDiagInfoToolTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinConfObfuscatorTest.prepare();
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 5L, true, "newten", true));
    }

    @After
    public void teardown() {
        queryHistoryDAO.deleteQueryHistoryByProject("newten");
        cleanupTestMetadata();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        getTestConfig().setProperty("kylin.diag.task-timeout", "60s");
        long start = System.currentTimeMillis();
        new QueryDiagInfoTool().execute(new String[] { "-project", "newten", "-query", "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath() });
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed one minute. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed one minutes, please adjust this test.",
                duration < 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("_query_") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testGetQueryByQueryId() {
        val query = new QueryDiagInfoTool().getQueryByQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        Assert.assertEquals("newten", query.getProjectName());
        Assert.assertEquals(1580311512000L, query.getQueryTime());
        Assert.assertEquals(5L, query.getDuration());
    }

    @Test
    public void testExecuteWithDefaultIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // Default  true
        new QueryDiagInfoTool().execute(
                new String[] { "-project", "newten", "-query", "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath() });

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));
        Assert.assertTrue(hasMetadataFile);
    }

    @Test
    public void testExecuteWithFalseIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // includeMeta false
        new QueryDiagInfoTool().execute(
                new String[] { "-project", "newten", "-query", "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath(), "-includeMeta", "false"});

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));

        Assert.assertFalse(hasMetadataFile);
    }

    @Test
    public void testWithNotExistsQueryId() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        thrown.expect(new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Exception)) {
                    return false;
                }

                Throwable e = ((Exception) o).getCause();

                if (!e.getClass().equals(RuntimeException.class)) {
                    return false;
                }

                if (!e.getMessage().equals("Can not find the queryId: 6a9a1f-f992-4d52-a8ec-8ff3fd3de6b1")) {
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        new QueryDiagInfoTool().execute(
                new String[] { "-project", "newten", "-query", "6a9a1f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath() });
    }

    @Test
    public void testWithNotExistsProject() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        thrown.expect(new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Exception)) {
                    return false;
                }

                Throwable e = ((Exception) o).getCause();

                if (!e.getClass().equals(RuntimeException.class)) {
                    return false;
                }

                if (!e.getMessage().equals("Can not find the project: newen")) {
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        new QueryDiagInfoTool().execute(
                new String[] { "-project", "newen", "-query", "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath() });
    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new QueryDiagInfoTool().execute(
                new String[] { "-project", "newten", "-query", "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtil.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = io.kyligence.kap.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

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
