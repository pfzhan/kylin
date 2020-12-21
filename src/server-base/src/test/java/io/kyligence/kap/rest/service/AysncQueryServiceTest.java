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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Exchanger;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.parquet.Strings;
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

public class AysncQueryServiceTest extends ServiceTestBase {

    private static String TEST_BASE_DIR;
    private static File BASE;
    private static String PROJECT = "default";

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
    public void testFailedQuery() throws IOException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        if (sqlResponse.isException()) {
            asyncQueryService.createErrorFlag(PROJECT, queryId, sqlResponse.getExceptionMessage());
        }
        assertTrue(asyncQueryService.queryStatus(PROJECT, queryId) == AsyncQueryService.QueryStatus.FAILED);
        String ret = asyncQueryService.retrieveSavedQueryException(PROJECT, queryId);
        assertEquals("some error!!!", ret);
    }

    @Test
    public void testCreateErrorFlagWhenMessageIsNull() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.createErrorFlag(PROJECT, queryId, null);
    }

    @Test
    public void testSuccessQueryAndDownloadResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
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

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, formatDefault, encodeDefault);

        assertEquals("a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadResultIncludeHeader() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockMetadata(queryId);
        mockResultFile(queryId, false);
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

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, true, response, formatDefault, encodeDefault);

        assertEquals("name,age,city\n" + "a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadJsonResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
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

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "json", encodeDefault);

        assertEquals("[\"{'column1':'a1', 'column2':'b1'}\",\"{'column1':'a2', 'column2':'b2'}\"]",
                baos.toString(StandardCharsets.UTF_8.name()));
    }

    @Test
    public void testSuccessQueryAndDownloadXlsxResult() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(false);
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
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

        asyncQueryService.retrieveSavedQueryResult(PROJECT, queryId, false, response, "xlsx", encodeDefault);
    }

    @Test
    public void testCleanFolder() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(asyncQueryService.getFileSystem().exists(resultPath));
        asyncQueryService.deleteAllFolder();
        assertTrue(!asyncQueryService.getFileSystem().exists(resultPath));
    }

    @Test
    public void testDeleteByQueryId() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);

        // before delete
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(asyncQueryService.getFileSystem().exists(resultPath));

        // after delete
        asyncQueryService.deleteByQueryId(PROJECT, queryId);
        try {
            new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("The query corresponding to this query id in the current project cannot be found .",
                    e.getMessage());
        }
    }

    @Test
    public void testDeleteByQueryIdWhenQueryNotExist() throws IOException, InterruptedException {
        try {
            asyncQueryService.deleteByQueryId(PROJECT, "123");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("The query corresponding to this query id in the current project cannot be found .",
                    e.getMessage());
        }
    }

    @Test
    public void testDeleteByTime() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        long time = System.currentTimeMillis();
        mockResultFile(queryId, false);

        // before delete
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(asyncQueryService.getFileSystem().exists(resultPath));
        asyncQueryService.deleteOldQueryResult(PROJECT, time - 1000 * 60);
        resultPath = new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        assertTrue(asyncQueryService.getFileSystem().exists(resultPath));

        // after delete
        asyncQueryService.deleteOldQueryResult(PROJECT, time + 1000 * 60);
        try {
            new Path(asyncQueryService.asyncQueryResultPath(PROJECT, queryId));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
            Assert.assertEquals("The query corresponding to this query id in the current project cannot be found .",
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
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        Assert.assertTrue(asyncQueryService.cleanOldQueryResult(PROJECT, 1));
    }

    @Test
    public void testQueryStatus() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        final String queryId = uuid.toString();
        final Exchanger<Boolean> exchanger = new Exchanger<Boolean>();

        Thread queryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    mockResultFile(queryId, true);
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
                        if (queryStatus == AsyncQueryService.QueryStatus.RUNNING) {
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
        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(PROJECT, queryId);
        assertTrue(queryStatus == AsyncQueryService.QueryStatus.SUCCESS);
        long l = asyncQueryService.fileStatus(PROJECT, queryId);
        assertTrue(l == 20);
    }

    @Test
    public void testQueryStatusMiss() throws IOException {
        UUID uuid = UUID.randomUUID();
        final String queryId = uuid.toString();
        Assert.assertEquals(AsyncQueryService.QueryStatus.MISS, asyncQueryService.queryStatus(PROJECT, queryId));
    }

    @Test
    public void testCheckStatusSuccessHappyPass() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS, PROJECT, "");
    }

    @Test
    public void testCheckStatusFailedHappyPass() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.isException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        if (sqlResponse.isException()) {
            asyncQueryService.createErrorFlag(PROJECT, queryId, sqlResponse.getExceptionMessage());
        }
        asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.FAILED, PROJECT, "");
    }

    @Test
    public void testCheckStatusException() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        try {
            asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS, PROJECT, "");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NAsyncQueryIllegalParamException);
        }
    }

    @Test
    public void testSaveAndGetUserName() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals("ADMIN", asyncQueryService.getQueryUsername(queryId, PROJECT));
    }

    @Test
    public void testGetUserNameNoResult() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        Assert.assertEquals(null, asyncQueryService.getQueryUsername(queryId, PROJECT));
    }

    @Test
    public void testHasPermissionWhenIsAdmin() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        Assert.assertEquals(true, asyncQueryService.hasPermission(queryId, PROJECT));
    }

    @Test
    public void testHasPermissionWhenIsSelf() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(true, asyncQueryService.hasPermission(queryId, PROJECT));
    }

    @Test
    public void testBatchDeleteAll() throws Exception {
        Assert.assertEquals(true, asyncQueryService.batchDelete(null, null));
    }

    @Test
    public void testBatchDeleteOlderResult() throws Exception {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(true, asyncQueryService.batchDelete(PROJECT, "2011-11-11 11:11:11"));
    }

    @Test
    public void testBatchDeleteOlderFalse() throws Exception {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.saveQueryUsername(PROJECT, queryId);
        Assert.assertEquals(false, asyncQueryService.batchDelete(PROJECT, null));
        Assert.assertEquals(false, asyncQueryService.batchDelete(null, "2011-11-11 11:11:11"));
    }

    @Test
    public void testSaveMetadata() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setColumnMetas(
                Lists.newArrayList(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0",
                        "c0", null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false)));
        asyncQueryService.saveMetaData(PROJECT, sqlResponse, queryId);
    }

    @Test
    public void testSaveFileInfo() throws IOException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.saveFileInfo(PROJECT, formatDefault, encodeDefault, fileNameDefault, queryId);
        AsyncQueryService.FileInfo fileInfo = asyncQueryService.getFileInfo(PROJECT, queryId);
        assertEquals(fileInfo.getFormat(), formatDefault);
        assertEquals(fileInfo.getEncode(), encodeDefault);
        assertEquals(fileInfo.getFileName(), fileNameDefault);
    }

    @Test
    public void testGetMetadata() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        mockMetadata(queryId);
        List<List<String>> metaData = asyncQueryService.getMetaData(PROJECT, queryId);
        assertArrayEquals(columnNames.toArray(), metaData.get(0).toArray());
        assertArrayEquals(dataTypes.toArray(), metaData.get(1).toArray());
    }

    public Path mockResultFile(String queryId, boolean block) throws IOException, InterruptedException {

        List<String> row1 = Lists.newArrayList("a1", "b1", "c1");
        List<String> row2 = Lists.newArrayList("a2", "b2", "c2");
        FileSystem fileSystem = asyncQueryService.getFileSystem();
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
            fileSystem.createNewFile(new Path(asyncQueryResultDir, asyncQueryService.getSuccessFlagFileName()));
        }

        return asyncQueryResultDir;
    }

    public Path mockJsonResultFile(String queryId) throws IOException {

        String row1 = "{'column1':'a1', 'column2':'b1'}\n";
        String row2 = "{'column1':'a2', 'column2':'b2'}";
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, "m00")); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            osw.write(StringEscapeUtils.unescapeJson(row1));
            osw.write(StringEscapeUtils.unescapeJson(row2));
            fileSystem.createNewFile(new Path(asyncQueryResultDir, asyncQueryService.getSuccessFlagFileName()));
        }

        return asyncQueryResultDir;
    }

    public void mockMetadata(String queryId) throws IOException {
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, asyncQueryService.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            String metaString = Strings.join(columnNames, ",") + "\n" + Strings.join(dataTypes, ",");
            osw.write(metaString);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void mockFormat(String queryId) throws IOException {
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, asyncQueryService.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            osw.write(formatDefault);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mockEncode(String queryId) throws IOException {
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(PROJECT, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, asyncQueryService.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) { //
            osw.write(encodeDefault);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
