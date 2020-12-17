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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.LogOutputTestCase;
import io.kyligence.kap.tool.util.ToolUtilTest;

public class InfluxDBToolTest extends LogOutputTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testDumpInfluxDBMetrics() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        InfluxDBTool.dumpInfluxDBMetrics(mainDir);

        Assert.assertTrue(new File(mainDir, "system_metrics").exists());
    }

    @Test
    public void testDumpInfluxDBMonitorMetrics() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        InfluxDBTool.dumpInfluxDBMonitorMetrics(mainDir);

        Assert.assertTrue(new File(mainDir, "monitor_metrics").exists());
    }

    @Test
    public void testDumpInfluxDBInPortUnavailable() throws Exception {
        int port = ToolUtilTest.getFreePort();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        String originHost = kapConfig.getMetricsRpcServiceBindAddress();
        kylinConfig.setProperty("kylin.metrics.influx-rpc-service-bind-address", "127.0.0.1:" + port);
        InfluxDBTool.dumpInfluxDBMetrics(mainDir);
        Assert.assertTrue(containsLog(String.format("Failed to Connect influxDB in 127.0.0.1:%s, skip dump.", port)));
        clearLogs();
        InfluxDBTool.dumpInfluxDBMonitorMetrics(mainDir);
        Assert.assertTrue(containsLog(String.format("Failed to Connect influxDB in 127.0.0.1:%s, skip dump.", port)));
        kylinConfig.setProperty("kylin.metrics.influx-rpc-service-bind-address", originHost);
    }

}
