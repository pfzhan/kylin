/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.service.DTableService;
import org.apache.kylin.job.service.TableSampleService;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.apache.kylin.rest.request.RefreshSegmentsRequest;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ModelBuildSupporter;
import org.apache.kylin.rest.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Sets;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class SampleController extends NBasicController {

    private static final String TABLE = "table";
    private static final int MAX_SAMPLING_ROWS = 20_000_000;
    private static final int MIN_SAMPLING_ROWS = 10_000;

    @Deprecated
    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private DTableService dTableService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    @Autowired
    private TableSampleService tableSampleService;

    @ApiOperation(value = "refreshSegments", tags = {
            "AI" }, notes = "Update Body: refresh_start, refresh_end, affected_start, affected_end")
    @PutMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
        checkProjectName(request.getProject());
        checkRequiredArg(TABLE, request.getTable());
        checkRequiredArg("refresh start", request.getRefreshStart());
        checkRequiredArg("refresh end", request.getRefreshEnd());
        checkRequiredArg("affected start", request.getAffectedStart());
        checkRequiredArg("affected end", request.getAffectedEnd());
        validateRange(request.getRefreshStart(), request.getRefreshEnd());
        modelBuildService.refreshSegments(request.getProject(), request.getTable(), request.getRefreshStart(),
                request.getRefreshEnd(), request.getAffectedStart(), request.getAffectedEnd());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "partitionColumnFormat", tags = { "AI" })
    @GetMapping(value = "/partition_column_format")
    @ResponseBody
    public EnvelopeResponse<String> getPartitionColumnFormat(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table,
            @RequestParam(value = "partition_column") String partitionColumn) throws Exception {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("partitionColumn", partitionColumn);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                tableService.getPartitionColumnFormat(project, table, partitionColumn), "");
    }

    @ApiOperation(value = "samplingJobs", tags = { "AI" })
    @PostMapping(value = "/sampling_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        checkSamplingRows(request.getRows());
        checkSamplingTable(request.getQualifiedTableName());
        validatePriority(request.getPriority());

        tableSampleService.sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(),
                request.getRows(), request.getPriority(), request.getYarnQueue(), request.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "hasSamplingJob", tags = { "AI" }, notes = "Update Param: qualified_table_name")
    @GetMapping(value = "/sampling_check_result")
    @ResponseBody
    public EnvelopeResponse<Boolean> hasSamplingJob(@RequestParam(value = "project") String project,
            @RequestParam(value = "qualified_table_name") String qualifiedTableName) {
        checkProjectName(project);
        checkSamplingTable(qualifiedTableName);
        boolean hasSamplingJob = tableSampleService.hasSamplingJob(project, qualifiedTableName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasSamplingJob, "");
    }

    @PostMapping(value = "/feign/sampling")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public List<String> sampling(@RequestBody Set<String> tables, @RequestParam("project") String project,
            @RequestParam("rows") int rows, @RequestParam("priority") int priority,
            @RequestParam(value = "yarnQueue", required = false, defaultValue = "") String yarnQueue,
            @RequestParam(value = "tag", required = false) Object tag) {
        return tableSampleService.sampling(tables, project, rows, priority, yarnQueue, tag);
    }

    public static void checkSamplingRows(int rows) {
        if (rows > MAX_SAMPLING_ROWS || rows < MIN_SAMPLING_ROWS) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }
    }

    public static void checkSamplingTable(String tableName) {
        Message msg = MsgPicker.getMsg();
        if (tableName == null || StringUtils.isEmpty(tableName.trim())) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getFailedForNoSamplingTable());
        }

        if (tableName.contains(" ") || !tableName.contains(".") || tableName.split("\\.").length != 2) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getSamplingFailedForIllegalTableName());
        }
    }
}