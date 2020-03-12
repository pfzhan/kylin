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
package io.kyligence.kap.rest.monitor;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.metrics.service.MonitorMetric;
import io.kyligence.kap.common.metrics.service.QueryMonitorMetric;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitorReporterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    public QueryMonitorMetric mockQueryMonitorMetric() {
        QueryMonitorMetric monitorMetric = new QueryMonitorMetric();
        monitorMetric.setHost("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("query");
        monitorMetric.setCreateTime(System.currentTimeMillis());

        monitorMetric.setLastResponseTime(29 * 1000L);
        monitorMetric.setErrorAccumulated(1);
        monitorMetric.setSparkRestarting(false);

        return monitorMetric;
    }

    private void wait(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBasic() {
        MonitorReporter monitorReporter = MonitorReporter.getInstance();
        monitorReporter.reportInitialDelaySeconds = 5;
        monitorReporter.startReporter();

        monitorReporter.submit(new AbstractMonitorCollectTask(
                Lists.newArrayList(Constant.SERVER_MODE_ALL, Constant.SERVER_MODE_QUERY)) {
            @Override
            protected MonitorMetric collect() {
                return mockQueryMonitorMetric();
            }
        });

        wait(3);

        Assert.assertEquals(Integer.valueOf(1), monitorReporter.getQueueSize());

        wait(5);

        Assert.assertEquals(Integer.valueOf(0), monitorReporter.getQueueSize());

        monitorReporter.stopReporter();
    }

}
