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

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NQueryHistorySchedulerTest extends NLocalFileMetadataTestCase {

    NQueryHistoryScheduler queryHistoryScheduler;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        queryHistoryScheduler = NQueryHistoryScheduler.getInstance();
        queryHistoryScheduler.queryMetricsQueue.clear();
    }

    @After
    public void destroy() throws Exception {
        cleanupTestMetadata();
        queryHistoryScheduler.queryMetricsQueue.clear();
        queryHistoryScheduler.shutdown();
    }

    @Test
    public void testWriteQueryHistoryAsynchronousNormal() throws Exception {
        // product
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(5578L);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setRealizations("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(true);
        queryMetrics.setQueryTime(1584888338274L);
        queryMetrics.setProjectName("default");

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName("default");

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        queryMetrics.setRealizationMetrics(realizationMetricsList);

        NQueryHistoryScheduler queryHistoryScheduler = NQueryHistoryScheduler.getInstance();
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        Assert.assertEquals(2, queryHistoryScheduler.queryMetricsQueue.size());

        // consume
        queryHistoryScheduler.init();
        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0, queryHistoryScheduler.queryMetricsQueue.size());
        });
    }

    @Test
    public void testWriteQueryHistoryAsynchronousIfBufferFull() throws Exception {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(5578L);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setRealizations("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(true);
        queryMetrics.setQueryTime(1584888338274L);
        queryMetrics.setProjectName("default");

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList("[DEFAULT.TEST_ACCOUNT]"));
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName("default");

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        queryMetrics.setRealizationMetrics(realizationMetricsList);

        NQueryHistoryScheduler queryHistoryScheduler = NQueryHistoryScheduler.getInstance();
        queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);

        // insert 1500 to queryHistoryQueue
        for (long i = 0; i < 1500; i++) {
            queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        }
        // lost 500 queryHistory
        Assert.assertEquals(500, queryHistoryScheduler.queryMetricsQueue.size());
    }
}
