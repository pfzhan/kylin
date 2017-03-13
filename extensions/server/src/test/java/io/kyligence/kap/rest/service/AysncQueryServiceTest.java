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
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.rest.model.SelectedColumnMeta;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

public class AysncQueryServiceTest extends ServiceTestBase {

    @Autowired
    AsyncQueryService asyncQueryService;

    private static final String TEST_ROOT_DIR = "/tmp/kylin/kylin_metadata/";
    private static final String TEST_BASE_DIR = TEST_ROOT_DIR + AsyncQueryService.BASE_FOLDER;
    private static final File base = new File(TEST_BASE_DIR);

    @Before
    public void setup() throws Exception {
        super.setup();
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
        asyncQueryService.flushResultToHdfs(sqlResponse, queryId);

        assertTrue(asyncQueryService.isQueryFailed(queryId));
        assertTrue(!asyncQueryService.isQuerySuccessful(queryId));

        assertEquals(2, countFileNum());

        String ret = asyncQueryService.retrieveSavedQueryException(queryId);
        assertEquals("some error!!!", ret);
        assertEquals(2, countFileNum());
    }

    @Test
    public void testTooSoonRetrieveResult() {
        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        try {
            asyncQueryService.retrieveSavedQueryException(queryId);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testSuccessQuery() throws IOException {
        SQLResponse sqlResponse = mock(SQLResponse.class);
        when(sqlResponse.getIsException()).thenReturn(false);
        List<String> row1 = Lists.newArrayList("a1", "b1", "c1");
        List<String> row2 = Lists.newArrayList("a2", "b2", "c2");
        List<List<String>> rows = Lists.newArrayList();
        rows.add(row1);
        rows.add(row2);
        when(sqlResponse.getResults()).thenReturn(rows);

        List<SelectedColumnMeta> metalist = Lists.newArrayList();
        metalist.add((SelectedColumnMeta) when(mock(SelectedColumnMeta.class).getName()).thenReturn("A").getMock());
        metalist.add((SelectedColumnMeta) when(mock(SelectedColumnMeta.class).getName()).thenReturn("B").getMock());
        metalist.add((SelectedColumnMeta) when(mock(SelectedColumnMeta.class).getName()).thenReturn("C").getMock());
        when(sqlResponse.getColumnMetas()).thenReturn(metalist);

        UUID uuid = UUID.randomUUID();
        String queryId = uuid.toString();
        asyncQueryService.flushResultToHdfs(sqlResponse, queryId);

        assertTrue(!asyncQueryService.isQueryFailed(queryId));
        assertTrue(asyncQueryService.isQuerySuccessful(queryId));

        assertEquals(2, countFileNum());

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

        assertEquals("A,B,C\r\n" + "a1,b1,c1\r\n" + "a2,b2,c2\r\n", baos.toString());
        assertEquals(2, countFileNum());

    }

    private int countFileNum() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = HadoopUtil.getWorkingFileSystem().listFiles(new Path(TEST_BASE_DIR), true);
        int count = 0;
        while (locatedFileStatusRemoteIterator.hasNext()) {
            count++;
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
        }
        return count;
    }
}
