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

import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.RUNNING;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.SUCCESS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.parquet.Strings;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.pushdown.SparkSqlClient;
import lombok.val;

public class AysncQueryServiceTest extends ServiceTestBase {

    private static String TEST_BASE_DIR;
    private static File BASE;
    private static String PROJECT = "default";

    protected static SparkSession ss = SparderEnv.getSparkSession();

    @Autowired
    @Qualifier("asyncQueryService")
    AsyncQueryService asyncQueryService;

    List<String> columnNames = Lists.newArrayList("name", "age", "city");
    List<String> dataTypes = Lists.newArrayList("varchar", "int", "varchar");
    final String formatDefault = "csv";
    final String encodeDefault = "utf-8";
    final String fileNameDefault = "result";

    @Before
    public void setup() {
        super.setup();
        TEST_BASE_DIR = KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(PROJECT);
        BASE = new File(TEST_BASE_DIR);
        FileUtil.setWritable(BASE, true);
        FileUtil.fullyDelete(BASE);
        assertFalse(BASE.exists());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtil.setWritable(BASE, true);
        FileUtil.fullyDelete(BASE);
        assertFalse(BASE.exists());
    }

    @Test
    public void testProjectSearchByQueryId() throws IOException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        String queryId = RandomUtil.randomUUIDStr();
        if (sqlResponse.isException()) {
            AsyncQueryUtil.createErrorFlag(PROJECT, queryId, sqlResponse.getExceptionMessage());
        }
        assertEquals(PROJECT, asyncQueryService.searchQueryResultProject(queryId));
    }

    @Test
    public void testFailedQuery() throws IOException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        String queryId = RandomUtil.randomUUIDStr();
        if (sqlResponse.isException()) {
            AsyncQueryUtil.createErrorFlag(PROJECT, queryId, sqlResponse.getExceptionMessage());
        }
        assertTrue(asyncQueryService.queryStatus(PROJECT, queryId) == AsyncQueryService.QueryStatus.FAILED);
        String ret = asyncQueryService.retrieveSavedQueryException(PROJECT, queryId);
        assertEquals("some error!!!", ret);
    }

    @Test
    public void testCreateErrorFlagWhenMessageIsNull() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        AsyncQueryUtil.createErrorFlag(PROJECT, queryId, null);
    }

    @Test
    public void testAsyncQueryWithParquetSpecialCharacters() throws IOException {
        QueryContext queryContext = QueryContext.current();
        String queryId = queryContext.getQueryId();
        mockMetadata(queryId, true);
        queryContext.getQueryTagInfo().setAsyncQuery(true);
        queryContext.getQueryTagInfo().setFileFormat("CSV");
        queryContext.getQueryTagInfo().setFileEncode("utf-8");
        String sql = "select '\\(123\\)','123'";
        queryContext.setProject(PROJECT);

        ss.sqlContext().setConf("spark.sql.parquet.columnNameCheck.enabled", "false");
        SparkSqlClient.executeSql(ss, sql, UUID.fromString(queryId), PROJECT);

        Awaitility.await().atMost(60000, TimeUnit.MILLISECONDS).until(
                () -> AsyncQueryService.QueryStatus.SUCCESS.equals(asyncQueryService.queryStatus(PROJECT, queryId)));
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());

        SparderEnv.getSparkSession().sqlContext().setConf("spark.sql.parquet.columnNameCheck.enabled", "false");
        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "csv", encodeDefault, ",");
        List<org.apache.spark.sql.Row> rowList = ss.read()
                .parquet(asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId).toString()).collectAsList();
        List<String> result = Lists.newArrayList();
        rowList.stream().forEach(row -> {
            val list = row.toSeq().toList();
            for (int i = 0; i < list.size(); i++) {
                Object cell = list.apply(i);
                String column = cell == null ? "" : cell.toString();
                result.add(column);
            }
        });
        assertEquals("(123)" + "123", result.get(0) + result.get(1));
    }

    @Test
    public void testAsyncQueryDownCsvResultByParquet() throws IOException {
        QueryContext queryContext = QueryContext.current();
        String queryId = queryContext.getQueryId();
        mockMetadata(queryId, true);
        queryContext.getQueryTagInfo().setAsyncQuery(true);
        queryContext.getQueryTagInfo().setFileFormat("csv");
        queryContext.getQueryTagInfo().setFileEncode("utf-8");
        String sql = "select '123\"','123'";
        queryContext.setProject(PROJECT);
        SparkSqlClient.executeSql(ss, sql, UUID.fromString(queryId), PROJECT);
        assertSame(AsyncQueryService.QueryStatus.SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());
        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "csv", encodeDefault, ",");
        List<org.apache.spark.sql.Row> rowList = ss.read().parquet(asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId).toString()).collectAsList();
        List<String> result = Lists.newArrayList();
        rowList.stream().forEach(row -> {
            val list = row.toSeq().toList();
            for (int i = 0; i < list.size(); i++) {
                Object cell = list.apply(i);
                String column = cell == null ? "" : cell.toString();
                result.add(column);
            }
        });
        assertEquals("123\"" + "123", result.get(0) + result.get(1));
    }

    @Test
    public void testSuccessQueryAndDownloadXlsxResultByParquet() throws IOException {
        QueryContext queryContext = QueryContext.current();
        String queryId = queryContext.getQueryId();
        mockMetadata(queryId, true);
        queryContext.getQueryTagInfo().setAsyncQuery(true);
        queryContext.getQueryTagInfo().setFileFormat("xlsx");
        queryContext.getQueryTagInfo().setFileEncode("utf-8");
        String sql = "select '123\"','123'";
        queryContext.setProject(PROJECT);
        SparkSqlClient.executeSql(ss, sql, UUID.fromString(queryId), PROJECT);
        assertSame(AsyncQueryService.QueryStatus.SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
        HttpServletResponse response = mock(HttpServletResponse.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());
        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "xlsx", encodeDefault, ",");
        List<org.apache.spark.sql.Row> rowList = ss.read().parquet(asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId).toString()).collectAsList();
        List<String> result = Lists.newArrayList();
        rowList.stream().forEach(row -> {
                    val list = row.toSeq().toList();
                    for (int i = 0; i < list.size(); i++) {
                        Object cell = list.apply(i);
                        String column = cell == null ? "" : cell.toString();
                        result.add(column);
                    }
                });
        assertEquals("123\"" + "123", result.get(0) + result.get(1));
    }

    @Test
    public void testSuccessQueryAndDownloadResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        assertTrue(asyncQueryService.queryStatus(PROJECT, queryId) == AsyncQueryService.QueryStatus.SUCCESS);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, formatDefault, encodeDefault, ",");

        assertEquals("a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadResultIncludeHeader() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        String queryId = RandomUtil.randomUUIDStr();
        mockMetadata(queryId, false);
        mockResultFile(queryId, false, true);
        assertSame(AsyncQueryService.QueryStatus.SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(invocationOnMock -> {
            Object[] arguments = invocationOnMock.getArguments();
            baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
            return null;
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, true, response, formatDefault, encodeDefault, ",");

        assertEquals("name,age,city\n" + "a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadJsonResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        String queryId = RandomUtil.randomUUIDStr();
        mockJsonResultFile(queryId);
        assertSame(AsyncQueryService.QueryStatus.SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "json", encodeDefault, ",");

        assertEquals("[\"{'column1':'a1', 'column2':'b1'}\",\"{'column1':'a2', 'column2':'b2'}\"]",
                baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadXlsxResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        assertSame(AsyncQueryService.QueryStatus.SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                baos.write((byte[]) arguments[0], (int) arguments[1], (int) arguments[2]);
                return null;
            }
        }).when(servletOutputStream).write(any(byte[].class), anyInt(), anyInt());

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "xlsx", encodeDefault, ",");
    }

    @Test
    public void testCleanFolder() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(AsyncQueryUtil.getFileSystem().exists(resultPath));
        asyncQueryService.deleteAllFolder();
        assertTrue(!AsyncQueryUtil.getFileSystem().exists(resultPath));
    }

    @Test
    public void testDeleteByQueryId() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);

        // before delete
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(AsyncQueryUtil.getFileSystem().exists(resultPath));

        // after delete
        asyncQueryService.deleteByQueryId(PROJECT, queryId);
        try {
            new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("Can’t find the query by this query ID in this project. Please check and try again.",
                    e.getMessage());
        }
    }

    @Test
    public void testDeleteByQueryIdWhenQueryNotExist() throws IOException, InterruptedException {
        try {
            asyncQueryService.deleteByQueryId(PROJECT, "123");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("Can’t find the query by this query ID in this project. Please check and try again.",
                    e.getMessage());
        }
    }

    @Test
    public void testDeleteByTime() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        long time = System.currentTimeMillis();
        mockResultFile(queryId, false, true);

        // before delete
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(AsyncQueryUtil.getFileSystem().exists(resultPath));
        asyncQueryService.deleteOldQueryResult(PROJECT, time - 1000 * 60);
        resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(AsyncQueryUtil.getFileSystem().exists(resultPath));

        // after delete
        asyncQueryService.deleteOldQueryResult(PROJECT, time + 1000 * 60);
        try {
            new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("Can’t find the query by this query ID in this project. Please check and try again.",
                    e.getMessage());
        }
    }

    @Test
    public void testDeleteByTimeWhenAsyncQueryDirNotExist() throws IOException {
        long time = System.currentTimeMillis();
        Assert.assertTrue(asyncQueryService.deleteOldQueryResult(PROJECT, time + 1000 * 60));
    }

    @Test
    public void testCleanOldQueryResult() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        Assert.assertTrue(asyncQueryService.cleanOldQueryResult(PROJECT, 1));
    }

    @Test
    public void testQueryStatus() throws IOException, InterruptedException {
        final String queryId = RandomUtil.randomUUIDStr();
        final Exchanger<Boolean> exchanger = new Exchanger<Boolean>();

        Thread queryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    mockResultFile(queryId, true, true);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread client = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean hasRunning = false;
                    for (int i = 0; i < 10; i++) {
                        Thread.sleep(1000);
                        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(PROJECT, queryId);
                        if (queryStatus == RUNNING) {
                            hasRunning = true;
                        }
                    }
                    exchanger.exchange(hasRunning);
                } catch (Throwable e) {
                }
            }
        });
        queryThread.start();
        client.start();
        Boolean hasRunning = exchanger.exchange(false);
        assertTrue(hasRunning);
        Thread.sleep(1000);
        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(PROJECT, queryId);
        assertTrue(queryStatus == AsyncQueryService.QueryStatus.SUCCESS);
        long l = asyncQueryService.fileStatus(PROJECT, queryId);
        assertTrue(l == 20);
    }

    @Test
    public void testQueryStatusMiss() throws IOException {
        final String queryId = RandomUtil.randomUUIDStr();
        Assert.assertEquals(AsyncQueryService.QueryStatus.MISS, asyncQueryService.queryStatus(PROJECT, queryId));
    }

    @Test
    public void testCheckStatusSuccessHappyPass() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS, PROJECT, "");
    }

    @Test
    public void testCheckStatusFailedHappyPass() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        if (sqlResponse.isException()) {
            AsyncQueryUtil.createErrorFlag(PROJECT, queryId, sqlResponse.getExceptionMessage());
        }
        asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.FAILED, PROJECT, "");
    }

    @Test
    public void testCheckStatusException() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        try {
            asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS, PROJECT, "");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
        }
    }

    @Test
    public void testSaveAndGetUserName() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals("ADMIN", asyncQueryService.getQueryUsername(queryId, PROJECT));
    }

    @Test
    public void testGetUserNameNoResult() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        Assert.assertEquals(null, asyncQueryService.getQueryUsername(queryId, PROJECT));
    }

    @Test
    public void testHasPermissionWhenIsAdmin() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        Assert.assertEquals(true, asyncQueryService.hasPermission(queryId, PROJECT));
    }

    @Test
    public void testDeleteAllWhenRunning() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, false);
        asyncQueryService.deleteAllFolder();

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setColumnMetas(
                Lists.newArrayList(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0",
                        "c0", null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false)));
        try {
            AsyncQueryUtil.saveMetaData(PROJECT, sqlResponse.getColumnMetas(), queryId);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("KE-020040001", ((NAsyncQueryIllegalParamException) e).getErrorCode().getCodeString());
        }
        try {
            AsyncQueryUtil.saveFileInfo(PROJECT, formatDefault, encodeDefault, fileNameDefault, queryId, ",");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("KE-020040001", ((NAsyncQueryIllegalParamException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testQueryStatusWhenRunning() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, false);

        Assert.assertEquals(RUNNING, asyncQueryService.queryStatus(PROJECT, queryId));

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setColumnMetas(
                Lists.newArrayList(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0",
                        "c0", null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false)));
        AsyncQueryUtil.saveMetaData(PROJECT, sqlResponse.getColumnMetas(), queryId);
        AsyncQueryUtil.saveFileInfo(PROJECT, formatDefault, encodeDefault, fileNameDefault, queryId, ",");

        Assert.assertEquals(SUCCESS, asyncQueryService.queryStatus(PROJECT, queryId));
    }

    @Test
    public void testGetQueryUserNameWhenUserNameNotSaved() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        asyncQueryService.hasPermission(queryId, PROJECT);
    }

    @Test
    public void testHasPermissionWhenIsSelf() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(true, asyncQueryService.hasPermission(queryId, PROJECT));
    }

    @Test
    public void testBatchDeleteAll() throws Exception {
        Assert.assertEquals(true, asyncQueryService.batchDelete(null, null));
    }

    @Test
    public void testBatchDeleteOlderResult() throws Exception {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(true, asyncQueryService.batchDelete(PROJECT, "2011-11-11 11:11:11"));
    }

    @Test
    public void testBatchDeleteOlderFalse() throws Exception {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(false, asyncQueryService.batchDelete(PROJECT, null));
        Assert.assertEquals(false, asyncQueryService.batchDelete(null, "2011-11-11 11:11:11"));
    }

    @Test
    public void testSaveMetadata() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setColumnMetas(
                Lists.newArrayList(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0",
                        "c0", null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false)));
        AsyncQueryUtil.saveMetaData(PROJECT, sqlResponse.getColumnMetas(), queryId);
    }

    @Test
    public void testSaveFileInfo() throws IOException {
        String queryId = RandomUtil.randomUUIDStr();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        AsyncQueryUtil.saveFileInfo(PROJECT, formatDefault, encodeDefault, fileNameDefault, queryId, "sep");
        AsyncQueryService.FileInfo fileInfo = asyncQueryService.getFileInfo(PROJECT, queryId);
        assertEquals(formatDefault, fileInfo.getFormat());
        assertEquals(encodeDefault, fileInfo.getEncode());
        assertEquals(fileNameDefault, fileInfo.getFileName());
        assertEquals("sep", fileInfo.getSeparator());
    }

    @Test
    public void testGetMetadata() throws IOException, InterruptedException {
        String queryId = RandomUtil.randomUUIDStr();
        mockResultFile(queryId, false, true);
        mockMetadata(queryId, false);
        List<List<String>> metaData = asyncQueryService.getMetaData(PROJECT, queryId);
        assertArrayEquals(columnNames.toArray(), metaData.get(0).toArray());
        assertArrayEquals(dataTypes.toArray(), metaData.get(1).toArray());
    }

    public Path mockResultFile(String queryId, boolean block, boolean needMeta)
            throws IOException, InterruptedException {

        List<String> row1 = Lists.newArrayList("a1", "b1", "c1");
        List<String> row2 = Lists.newArrayList("a2", "b2", "c2");
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        if (block) {
            Thread.sleep(5000);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, "m00")); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8); //
                ICsvListWriter csvWriter = new CsvListWriter(osw, CsvPreference.STANDARD_PREFERENCE)) {
            csvWriter.write(row1);
            csvWriter.write(row2);
            fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getSuccessFlagFileName()));
            if (needMeta) {
                fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName()));
                fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getFileInfo()));
            }
        }

        return asyncQueryResultDir;
    }

    public Path mockJsonResultFile(String queryId) throws IOException {

        String row1 = "{'column1':'a1', 'column2':'b1'}\n";
        String row2 = "{'column1':'a2', 'column2':'b2'}";
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, "m00")); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            osw.write(StringEscapeUtils.unescapeJson(row1));
            osw.write(StringEscapeUtils.unescapeJson(row2));
            fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getSuccessFlagFileName()));
            fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName()));
            fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getFileInfo()));
        }

        return asyncQueryResultDir;
    }

    public void mockMetadata(String queryId, boolean needMeta) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            String metaString = Strings.join(columnNames, ",") + "\n" + Strings.join(dataTypes, ",");
            osw.write(metaString);
            if (needMeta) {
                fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName()));
                fileSystem.createNewFile(new Path(asyncQueryResultDir, AsyncQueryUtil.getFileInfo()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void mockFormat(String queryId) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            osw.write(formatDefault);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mockEncode(String queryId) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            osw.write(encodeDefault);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCsvWriter() throws IOException {
        List<List<Object>> rows = Lists.newArrayList(
                Lists.newArrayList(1, 3.12, "foo"),
                Lists.newArrayList(2, 3.123, "fo<>o"),
                Lists.newArrayList(3, 3.124, "fo\ro")
        );
        String expected = "1<>3.12<>foo\n2<>3.123<>\"fo<>o\"\n3<>3.124<>\"fo\ro\"\n";
        try (StringWriter sw = new StringWriter()) {
            CSVWriter.writeCsv(rows.iterator(), sw, "<>");
            assertEquals(expected, sw.toString());
        }
    }
}
