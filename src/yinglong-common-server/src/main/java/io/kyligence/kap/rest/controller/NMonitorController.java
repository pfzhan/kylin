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
package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.response.ClusterStatisticStatusResponse;
import io.kyligence.kap.rest.response.ClusterStatusResponse;
import io.kyligence.kap.rest.response.ExecutorMemoryResponse;
import io.kyligence.kap.rest.response.ExecutorThreadInfoResponse;
import io.kyligence.kap.rest.service.MonitorService;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBIOException;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/monitor", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NMonitorController extends NBasicController {
    @Autowired
    @Qualifier("monitorService")
    private MonitorService monitorService;

    @Deprecated
    @ApiOperation(value = "getMemoryMetrics", tags = { "SM" }, notes = "Update URL: memory_info")
    @GetMapping(value = "/memory_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorMemoryResponse>> getMemoryMetrics() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Lists.newArrayList(), "");
    }

    @Deprecated
    @ApiOperation(value = "getThreadInfoMetrics", tags = { "SM" }, notes = "Update URL: thread_info")
    @GetMapping(value = "/thread_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorThreadInfoResponse>> getThreadInfoMetrics() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Lists.newArrayList(), "");
    }

    @ApiOperation(value = "getSparkMetrics", tags = { "SM" }, notes = "Fetch Spark Metrics")
    @GetMapping(value = "/spark/prometheus", produces = "text/plain;charset=utf-8")
    @ResponseBody
    public String getSparkMetricsForPrometheus() {
        return monitorService.fetchAndMergeSparkMetrics();
    }

    @ApiOperation(value = "getStatus", tags = { "SM" })
    @GetMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<ClusterStatusResponse> getClusterCurrentStatus() {
        ClusterStatusResponse result;
        try {
            result = monitorService.currentClusterStatus();
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "getStatusStatistics", tags = { "SM" })
    @GetMapping(value = "/status/statistics")
    @ResponseBody
    public EnvelopeResponse<ClusterStatisticStatusResponse> getClusterStatisticStatus(
            @RequestParam(value = "start") long start, @RequestParam(value = "end") long end) {
        long now = System.currentTimeMillis();
        end = end > now ? now : end;
        if (start > end) {
            throw new KylinException(INVALID_RANGE, String.format(Locale.ROOT, "start: %s > end: %s", start, end));
        }

        ClusterStatisticStatusResponse result;
        try {
            result = monitorService.statisticClusterByFloorTime(start, end);
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }
}
