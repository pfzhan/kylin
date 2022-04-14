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

import com.google.common.collect.Lists;
import io.kyligence.kap.rest.response.HealthResponse;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.rest.service.BasicService;
import org.apache.spark.sql.SparderEnv;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component("healthService")
public class HealthService extends BasicService {

    public HealthResponse.RestartSparkStatusResponse getRestartSparkStatus() {
        return new HealthResponse.RestartSparkStatusResponse(SparderEnv.startSparkFailureTimes(),
                SparderEnv.lastStartSparkFailureTime());
    }

    public List<HealthResponse.CanceledSlowQueryStatusResponse> getCanceledSlowQueriesStatus() {
        List<HealthResponse.CanceledSlowQueryStatusResponse> canceledStatusList = Lists.newArrayList();

        Map<String, SlowQueryDetector.CanceledSlowQueryStatus> canceledBadQueriesStatus = SlowQueryDetector
                .getCanceledSlowQueriesStatus();

        for (SlowQueryDetector.CanceledSlowQueryStatus canceledBadQueryStatus : canceledBadQueriesStatus.values()) {
            if (canceledBadQueryStatus.getCanceledTimes() > 1) {
                canceledStatusList.add(new HealthResponse.CanceledSlowQueryStatusResponse(
                        canceledBadQueryStatus.getQueryId(), canceledBadQueryStatus.getCanceledTimes(),
                        canceledBadQueryStatus.getLastCanceledTime(), canceledBadQueryStatus.getQueryDurationTime()));
            }
        }
        return canceledStatusList;
    }

}
