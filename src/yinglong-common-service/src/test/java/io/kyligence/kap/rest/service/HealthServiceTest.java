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

import com.google.common.collect.Maps;
import io.kyligence.kap.rest.response.HealthResponse;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class HealthServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("healthService")
    private HealthService healthService;

    @Before
    public void before() {
        SlowQueryDetector.clearCanceledSlowQueriesStatus();
    }

    @After
    public void after() {
        SlowQueryDetector.clearCanceledSlowQueriesStatus();
        SparderEnv.startSparkFailureTimes_$eq(0);
        SparderEnv.lastStartSparkFailureTime_$eq(0);
    }

    @Test
    public void testGetRestartSparkStatus() {
        int sparkFailureTimes = 1;
        long failueTime = System.currentTimeMillis();
        SparderEnv.startSparkFailureTimes_$eq(sparkFailureTimes);
        SparderEnv.lastStartSparkFailureTime_$eq(failueTime);

        HealthResponse.RestartSparkStatusResponse restartSparkStatus = healthService.getRestartSparkStatus();
        Assert.assertEquals(sparkFailureTimes, restartSparkStatus.getStartSparkFailureTimes());
        Assert.assertEquals(failueTime, restartSparkStatus.getLastStartSparkFailureTime());
    }

    @Test
    public void testGetCanceledBadQueriesStatus() {
        ConcurrentMap<String, SlowQueryDetector.CanceledSlowQueryStatus> canceledBadQueriesStatus = Maps
                .newConcurrentMap();

        String needReportQueryId = "need-report-query-id";
        //query.cancelTimes > 1 will be reported
        int needReportCanceledTimes = 2;
        String notNeedReportQueryId = "not-need-report-query-id";
        //query.cancelTimes <= 1 will not be reported
        int notNeedReportCanceledTimes = 1;
        long lastCancelTime = System.currentTimeMillis();
        float durationTime = 20.55F;

        canceledBadQueriesStatus.put(needReportQueryId, new SlowQueryDetector.CanceledSlowQueryStatus(needReportQueryId,
                needReportCanceledTimes, lastCancelTime, durationTime));
        canceledBadQueriesStatus.put(notNeedReportQueryId, new SlowQueryDetector.CanceledSlowQueryStatus(
                notNeedReportQueryId, notNeedReportCanceledTimes, lastCancelTime, durationTime));
        SlowQueryDetector.addCanceledSlowQueriesStatus(canceledBadQueriesStatus);

        List<HealthResponse.CanceledSlowQueryStatusResponse> slowQueriesStatus = healthService
                .getCanceledSlowQueriesStatus();
        Assert.assertEquals(1, slowQueriesStatus.size());
        Assert.assertEquals(needReportQueryId, slowQueriesStatus.get(0).getQueryId());
        Assert.assertEquals(needReportCanceledTimes, slowQueriesStatus.get(0).getCanceledTimes());
        Assert.assertEquals(lastCancelTime, slowQueriesStatus.get(0).getLastCanceledTime());
        Assert.assertTrue((durationTime - slowQueriesStatus.get(0).getQueryDurationTime())
                * (durationTime - slowQueriesStatus.get(0).getQueryDurationTime()) < 0.00001);
    }

}
