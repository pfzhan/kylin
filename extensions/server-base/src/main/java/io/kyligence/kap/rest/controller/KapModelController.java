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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.model.DimensionAdvisor;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KapJobRequest;
import io.kyligence.kap.rest.request.ModelStatusRequest;
import io.kyligence.kap.rest.service.KapModelService;
import io.kyligence.kap.source.hive.modelstats.CollectModelStatsJob;

@Controller
@RequestMapping(value = "/models")
public class KapModelController extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KapModelController.class);

    @Autowired
    @Qualifier("kapModelService")
    private KapModelService kapModelService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    /**
     * Get modeling suggestions for the table
     *
     * @return suggestion map
     */

    @RequestMapping(value = "{project}/table_suggestions", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelDimensionSuggestions(@RequestParam(value = "table") String table,
            @PathVariable String project) throws IOException {

        Map<String, DimensionAdvisor.ColumnSuggestionType> result = kapModelService.inferDimensionSuggestions(table,
                project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{project}/{modelName}/checkable", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelCheckable(@PathVariable("project") String project,
            @PathVariable("modelName") String modelName) throws IOException, JobException {

        Map<Boolean, String> result = new HashMap<>();
        if (kapModelService.isFactTableStreaming(modelName)) {
            result.put(false, "Model check of streaming data model is not supported by now.");
        } else {
            result.put(true, "The model check is available.");
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{project}/{modelName}/stats", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse doModelStats(@PathVariable("project") String project,
            @PathVariable("modelName") String modelName, @RequestBody KapJobRequest req)
            throws IOException, JobException {
        KapMessage msg = KapMsgPicker.getMsg();

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        SegmentRange.TimePartitionedSegmentRange tsRange = null;
        try {
            tsRange = (req.getStartTime() == 0L && req.getEndTime() == 0L) ? null
                    : new SegmentRange.TimePartitionedSegmentRange(req.getStartTime(), req.getEndTime());
        } catch (IllegalStateException e) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, null, msg.getTSRANGE_ERROR());
        }

        CollectModelStatsJob job = new CollectModelStatsJob(project, modelName, submitter, //
                tsRange, req.getFrequency(), req.getCheckList(), req.getForceUpdate());
        String jobId = job.start();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobInstance(jobId), "");
    }

    @RequestMapping(value = "{project}/{modelName}/diagnose", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelDiagnosis(@PathVariable("project") String project,
            @PathVariable("modelName") String modelName) throws IOException {

        ModelStatusRequest request = kapModelService.getDiagnoseResult(modelName);
        String jobId = new CollectModelStatsJob(project, modelName).findRunningJob();
        if (null != jobId && null != jobService.getJobInstance(jobId)) {
            request.setProgress(jobService.getJobInstance(jobId).getProgress());
            if (jobService.getJobInstance(jobId).getStatus() == JobStatusEnum.ERROR) {
                request.setHeathStatus(ModelStatusRequest.HealthStatus.ERROR);
            } else
                request.setHeathStatus(ModelStatusRequest.HealthStatus.RUNNING);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, request, "");
    }

    @RequestMapping(value = "{project}/{modelName}/progress", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProgress(@RequestHeader("Accept-Language") String lang,
            @PathVariable("project") String project, @PathVariable("modelName") String modelName) throws IOException {
        KapMsgPicker.setMsg(lang);
        String jobId = new CollectModelStatsJob(project, modelName).findRunningJob();
        Map<Boolean, Double> result = new HashMap<>();
        if (jobId != null && null != jobService.getJobInstance(jobId)) {
            result.put(true, jobService.getJobInstance(jobId).getProgress());
        } else {
            result.put(false, 0.0);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "get_all_stats", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAllStats(@RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException, JobException {

        List<DataModelDesc> models = modelService.listAllModels(modelName, projectName, true);

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (models.size() <= offset) {
            offset = models.size();
            limit = 0;
        }

        if ((models.size() - offset) < limit) {
            limit = models.size() - offset;
        }

        List<ModelStatusRequest> modelStatusList = new ArrayList<>();
        for (DataModelDesc model : modelService.getModels(modelName, projectName, limit, offset)) {
            ModelStatusRequest request = kapModelService.getDiagnoseResult(model.getName());
            String jobId = new CollectModelStatsJob(projectName, model.getName()).findRunningJob();
            if (null != jobId && null != jobService.getJobInstance(jobId)) {
                request.setProgress(jobService.getJobInstance(jobId).getProgress());
                if (jobService.getJobInstance(jobId).getStatus() == JobStatusEnum.ERROR) {
                    request.setHeathStatus(ModelStatusRequest.HealthStatus.ERROR);
                } else
                    request.setHeathStatus(ModelStatusRequest.HealthStatus.RUNNING);
            }
            modelStatusList.add(request);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelStatusList, "");
    }

    @RequestMapping(value = "{project}/{table}/{column}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPartitionColumnStats(@PathVariable String project, @PathVariable String table,
            @PathVariable String column) throws IOException {
        String[] result = kapModelService.getColumnSamples(project, table, column);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "{project}/{table}/{column}/validate", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse valiatePartitionFormat(@RequestParam(value = "format") String format,
            @PathVariable String project, @PathVariable String table, @PathVariable String column) throws IOException {
        boolean result = kapModelService.validatePartitionFormat(project, table, column, format);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }
}