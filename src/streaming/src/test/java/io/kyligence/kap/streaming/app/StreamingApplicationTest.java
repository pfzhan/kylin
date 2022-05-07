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
package io.kyligence.kap.streaming.app;

import static io.kyligence.kap.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingApplicationTest extends StreamingTestCase {

    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute_PrepareBeforeExecute() {
        val entry = Mockito.spy(new StreamingEntry());
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "xx", DEFAULT_PARSER_NAME };
        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doNothing().when(entry).doExecute();
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.execute(args);
    }

    @Test
    public void testExecute_DoExecute() {
        val entry = Mockito.spy(new StreamingEntry());
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "xx", DEFAULT_PARSER_NAME };
        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doNothing().when(entry).doExecute();
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.execute(args);
    }

    @Test
    public void testSystemExit_False() {
        overwriteSystemProp("streaming.local", "true");
        val entry = Mockito.spy(new StreamingEntry());
        Mockito.doReturn(false).when(entry).isJobOnCluster();
        entry.systemExit(0);
    }

    @Test
    public void testIsJobOnCluster_True() {
        overwriteSystemProp("streaming.local", "false");
        getTestConfig().setProperty("kylin.env", "PROD");
        val entry = Mockito.spy(new StreamingEntry());
        Assert.assertFalse(getTestConfig().isUTEnv());
        Assert.assertFalse(StreamingUtils.isLocalMode());
        Assert.assertTrue(entry.isJobOnCluster());
    }

    @Test
    public void testIsJobOnCluster_False() {

        {
            overwriteSystemProp("streaming.local", "false");
            val entry = Mockito.spy(new StreamingEntry());
            Assert.assertTrue(getTestConfig().isUTEnv());
            Assert.assertFalse(StreamingUtils.isLocalMode());
            Assert.assertFalse(entry.isJobOnCluster());
        }

        {
            overwriteSystemProp("streaming.local", "true");
            val entry = Mockito.spy(new StreamingEntry());
            getTestConfig().setProperty("kylin.env", "PROD");
            Assert.assertFalse(getTestConfig().isUTEnv());
            Assert.assertTrue(StreamingUtils.isLocalMode());
            Assert.assertFalse(entry.isJobOnCluster());
        }
    }

    @Test
    public void testCloseSparkSession_True() {
        overwriteSystemProp("streaming.local", "false");
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        Assert.assertFalse(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> entry.getSparkSession().sparkContext().isStopped());
    }

    @Test
    public void testCloseSparkSession_False() {
        overwriteSystemProp("streaming.local", "true");
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        Assert.assertFalse(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> !entry.getSparkSession().sparkContext().isStopped());

    }

    @Test
    public void testCloseSparkSession_AlreadyStop() {
        overwriteSystemProp("streaming.local", "false");
        val entry = Mockito.spy(new StreamingEntry());
        val ss = createSparkSession();
        entry.setSparkSession(ss);
        ss.stop();
        Assert.assertTrue(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> entry.getSparkSession().sparkContext().isStopped());

    }

    @Test
    public void testStartJobExecutionIdCheckThread_NotRunning() {
        val entry = Mockito.spy(new StreamingEntry());

        Mockito.doReturn(false).when(entry).isRunning();

        Assert.assertFalse(entry.isRunning());
        entry.startJobExecutionIdCheckThread();

        Awaitility.waitAtMost(3, TimeUnit.SECONDS).until(() -> !entry.isRunning());
    }

    @Test
    public void testStartJobExecutionIdCheckThread_Exception() {
        val entry = Mockito.spy(new StreamingEntry());
        Mockito.doReturn(true).when(entry).isRunning();
        Assert.assertTrue(entry.isRunning());
        entry.startJobExecutionIdCheckThread();

        Awaitility.waitAtMost(3, TimeUnit.SECONDS).until(() -> entry.isRunning());
    }

}
