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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.spark.memory.MetricsCollectHelper;
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

    @ApiOperation(value = "getMemoryMetrics (update)", notes = "Update URL: memory_info")
    @GetMapping(value = "/memory_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorMemoryResponse>> getMemoryMetrics() {
        Map<String, List<String>> memorySnapshotMap = MetricsCollectHelper.getMemorySnapshot();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, parseToMemoryResponse(memorySnapshotMap), "");
    }

    @ApiOperation(value = "getThreadInfoMetrics (update)", notes = "Update URL: thread_info")
    @GetMapping(value = "/thread_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorThreadInfoResponse>> getThreadInfoMetrics() {
        Map<String, List<String>> threadInfoSnapshotMap = MetricsCollectHelper.getThreadInfoSnapshot();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, parseToThreadInfoResponse(threadInfoSnapshotMap), "");
    }

    private List<ExecutorMemoryResponse> parseToMemoryResponse(Map<String, List<String>> memList) {
        List<ExecutorMemoryResponse> responseList = Lists.newArrayList();
        memList.forEach((name, list) -> {
            ExecutorMemoryResponse result = new ExecutorMemoryResponse();
            result.setExecutorName(name);
            result.setMemInfos(list);
            responseList.add(result);
        });
        return responseList;
    }

    private List<ExecutorThreadInfoResponse> parseToThreadInfoResponse(Map<String, List<String>> threadInfos) {
        List<ExecutorThreadInfoResponse> responseList = Lists.newArrayList();
        threadInfos.forEach((name, list) -> {
            ExecutorThreadInfoResponse result = new ExecutorThreadInfoResponse();
            result.setExecutorName(name);
            result.setThreadInfos(list);
            responseList.add(result);
        });
        return responseList;
    }

    @GetMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<ClusterStatusResponse> getClusterCurrentStatus() {
        ClusterStatusResponse result;
        try {
            result = monitorService.currentClusterStatus();
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @GetMapping(value = "/status/statistics")
    @ResponseBody
    public EnvelopeResponse<ClusterStatisticStatusResponse> getClusterStatisticStatus(
            @RequestParam(value = "start") long start, @RequestParam(value = "end") long end) {
        long now = System.currentTimeMillis();
        end = end > now ? now : end;
        if (start > end) {
            throw new KylinException("KE-1010", String.format("start: %s > end: %s", start, end));
        }

        ClusterStatisticStatusResponse result;
        try {
            result = monitorService.statisticClusterByFloorTime(start, end);
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }
}
