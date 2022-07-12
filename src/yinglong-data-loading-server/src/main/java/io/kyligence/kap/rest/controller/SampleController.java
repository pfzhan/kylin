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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
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

import io.kyligence.kap.job.service.DTableService;
import io.kyligence.kap.job.service.TableSampleService;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.service.ModelBuildSupporter;
import io.kyligence.kap.rest.service.TableSamplingService;
import io.kyligence.kap.rest.service.TableService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class SampleController extends BaseController {

    private static final String TABLE = "table";

    @Deprecated
    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private DTableService dTableService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    @Deprecated
    @Autowired
    @Qualifier("tableSamplingService")
    private TableSamplingService tableSamplingService;


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
        TableSamplingService.checkSamplingRows(request.getRows());
        TableSamplingService.checkSamplingTable(request.getQualifiedTableName());
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
        TableSamplingService.checkSamplingTable(qualifiedTableName);
        boolean hasSamplingJob = tableSampleService.hasSamplingJob(project, qualifiedTableName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasSamplingJob, "");
    }

    @PostMapping(value = "/feign/sampling")
    @ResponseBody
    public List<String> sampling(@RequestBody Set<String> tables, @RequestParam("project") String project,
            @RequestParam("rows") int rows, @RequestParam("priority") int priority,
            @RequestParam(value = "yarnQueue", required = false, defaultValue = "") String yarnQueue,
            @RequestParam(value = "tag", required = false) Object tag) {
        return tableSampleService.sampling(tables, project, rows, priority, yarnQueue, tag);
    }
}
