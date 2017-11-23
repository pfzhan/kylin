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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Exchanger;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.rest.response.SQLResponse;
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

import io.kyligence.kap.tool.storage.KapStorageCleanupCLI;

public class AysncQueryServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("asyncQueryService")
    AsyncQueryService asyncQueryService;

    private static String TEST_ROOT_DIR;
    private static String TEST_BASE_DIR;
    private static File base;
    List<String> columnNames = Lists.newArrayList("name", "age", "city");
    List<String> dataTypes = Lists.newArrayList("varchar", "int", "varchar");

    @Before
    public void setup() throws Exception {
        super.setup();
        TEST_ROOT_DIR = getLocalWorkingDirectory();
        TEST_BASE_DIR = KapConfig.getInstanceFromEnv().getAsyncResultBaseDir();
        base = new File(TEST_BASE_DIR);
        FileUtil.setWritable(base, true);
        FileUtil.fullyDelete(base);
        assertTrue(!base.exists());
    }

    @After
    public void after() throws Exception {
        super.after();

        FileUtil.setWritable(base, true);
        FileUtil.fullyDelete(base);
        assertTrue(!base.exists());
    }

    @Test
    public void testFailedQuery() throws IOException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.getIsException()).thenReturn(true);
        when(sqlResponse.getExceptionMessage()).thenReturn("some error!!!");

        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        if (sqlResponse.getIsException()) {
            asyncQueryService.createErrorFlag(sqlResponse.getExceptionMessage(), queryId);
        }
        assertTrue(asyncQueryService.queryStatus(queryId) == AsyncQueryService.QueryStatus.FAILED);
        String ret = asyncQueryService.retrieveSavedQueryException(queryId);
        assertEquals("some error!!!", ret);
    }

    @Test
    public void testTooSoonRetrieveResult() {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        try {
            asyncQueryService.retrieveSavedQueryException(queryId);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
    }

    @Test
    public void testSuccessQuery() throws IOException, InterruptedException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.getIsException()).thenReturn(false);
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        assertTrue(asyncQueryService.queryStatus(queryId) == AsyncQueryService.QueryStatus.SUCCESS);
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

        asyncQueryService.retrieveSavedQueryResult(queryId, response);

        assertEquals("a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString());

    }

    public Path mockResultFile(String queryId, boolean bolck) throws IOException, InterruptedException {

        List<String> row1 = Lists.newArrayList("a1", "b1", "c1");
        List<String> row2 = Lists.newArrayList("a2", "b2", "c2");
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.RUNNING);
        if (bolck) {
            Thread.sleep(5000);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, "m00")); //
                OutputStreamWriter osw = new OutputStreamWriter(os); //
                ICsvListWriter csvWriter = new CsvListWriter(osw, CsvPreference.STANDARD_PREFERENCE)) {
            csvWriter.write(row1);
            csvWriter.write(row2);
            fileSystem.createNewFile(new Path(asyncQueryResultDir, asyncQueryService.getSuccessFlagFileName()));
            asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return asyncQueryResultDir;
    }

    public Path mockOldResultFile(String queryId, boolean bolck) throws IOException, InterruptedException {

        List<String> row1 = Lists.newArrayList("a1", "b1", "c1");
        List<String> row2 = Lists.newArrayList("a2", "b2", "c2");
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.RUNNING);
        if (bolck) {
            Thread.sleep(5000);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, "m00")); //
                OutputStreamWriter osw = new OutputStreamWriter(os); //
                ICsvListWriter csvWriter = new CsvListWriter(osw, CsvPreference.STANDARD_PREFERENCE)) {
            csvWriter.write(row1);
            csvWriter.write(row2);
            fileSystem.createNewFile(new Path(asyncQueryResultDir, asyncQueryService.getSuccessFlagFileName()));
            asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileSystem.setTimes(asyncQueryResultDir, 0, 0);
        return asyncQueryResultDir;
    }

    public void mockMetadata(String queryId) throws IOException {
        FileSystem fileSystem = asyncQueryService.getFileSystem();
        Path asyncQueryResultDir = asyncQueryService.getAsyncQueryResultDir(queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem
                .create(new Path(asyncQueryResultDir, asyncQueryService.getMetaDataFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os)) { //
            String metaString = Strings.join(columnNames, ",") + "\n" + Strings.join(dataTypes, ",");
            osw.write(metaString);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testCleanFolder() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        Path resultPath = new Path(asyncQueryService.asyncQueryResultPath(queryId));
        assertTrue(asyncQueryService.getFileSystem().exists(resultPath));
        asyncQueryService.cleanAllFolder();
        assertTrue(!asyncQueryService.getFileSystem().exists(resultPath));
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
                        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(queryId);
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
        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(queryId);
        assertTrue(queryStatus == AsyncQueryService.QueryStatus.SUCCESS);
        long l = asyncQueryService.fileStatus(queryId);
        assertTrue(l == 20);
    }

    @Test
    public void testMetadata() throws IOException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        mockMetadata(queryId);
        List<List<String>> metaData = asyncQueryService.getMetaData(queryId);
        assertArrayEquals(columnNames.toArray(), metaData.get(0).toArray());
        assertArrayEquals(dataTypes.toArray(), metaData.get(1).toArray());
    }

    @Test
    public void testDeleteOld() throws IOException, InterruptedException {
        System.setProperty("cleanjob.test", "true");
        Path basePath = new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir());

        if (asyncQueryService.getFileSystem().exists(basePath)) {
            asyncQueryService.getFileSystem().delete(basePath, true);
        }

        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockOldResultFile(queryId, false);
        uuid = UUID.randomUUID();
        queryId = uuid.toString();
        mockOldResultFile(queryId, false);
        if (asyncQueryService.getFileSystem().exists(basePath)) {
            assertTrue(asyncQueryService.getFileSystem().listStatus(basePath).length == 2);
            KapStorageCleanupCLI kapStorageCleanupCLI = new KapStorageCleanupCLI();
            kapStorageCleanupCLI.cleanUnusedAsyncResult(new Configuration());
            assertTrue(asyncQueryService.getFileSystem().listStatus(basePath).length == 0);
        } else {
            fail();
        }
        System.setProperty("cleanjob.test", "false");

    }

    @Test
    public void testDeleteNew() throws IOException, InterruptedException {
        System.setProperty("cleanjob.test", "true");
        Path basePath = new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir());

        if (asyncQueryService.getFileSystem().exists(basePath)) {
            asyncQueryService.getFileSystem().delete(basePath, true);
        }
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        mockResultFile(queryId, false);
        uuid = UUID.randomUUID();
        queryId = uuid.toString();
        mockResultFile(queryId, false);
        if (asyncQueryService.getFileSystem().exists(basePath)) {
            assertTrue(asyncQueryService.getFileSystem().listStatus(basePath).length == 2);
            KapStorageCleanupCLI kapStorageCleanupCLI = new KapStorageCleanupCLI();
            kapStorageCleanupCLI.cleanUnusedAsyncResult(new Configuration());
            assertTrue(asyncQueryService.getFileSystem().listStatus(basePath).length == 2);
        } else {
            fail();
        }
        System.setProperty("cleanjob.test", "false");
    }

}
