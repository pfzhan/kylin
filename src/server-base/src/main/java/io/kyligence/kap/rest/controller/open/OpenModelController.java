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
package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.service.ModelService;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelController extends NBasicController {

    @Autowired
    private NModelController modelController;

    @Autowired
    private ModelService modelService;

    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_name", required = false) String modelAlias, //
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "owner", required = false) String owner, //
            @RequestParam(value = "status", required = false) List<String> status, //
            @RequestParam(value = "table", required = false) String table, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {

        return modelController.getModels(modelAlias, exactMatch, project, owner, status, table, offset, limit, sortBy,
                reverse);
    }

    @VisibleForTesting
    public NDataModelResponse getModel(String modelName, String project) {
        if (modelService.getProjectManager().getProject(project) == null) {
            throw new BadRequestException(String.format("Can not find the project : %s!", project));
        }
        List<NDataModelResponse> responses = modelService.getModels(modelName, project, true, null, null, "last_modify",
                true);
        if (CollectionUtils.isEmpty(responses)) {
            throw new BadRequestException(String.format("Can not find the Segments with model_name %s!", modelName));
        }
        return responses.get(0);
    }

    @GetMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "model_name") String modelName, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        String modelId = getModel(modelName, project).getId();
        return modelController.getSegments(modelId, project, status, offset, limit, start, end, sortBy, reverse);
    }

    @PostMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model_name") String modelName,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        NDataModel nDataModel = getModel(modelName, buildSegmentsRequest.getProject());
        return modelController.buildSegmentsManually(nDataModel.getId(), buildSegmentsRequest);
    }

    @PutMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> refreshOrMergeSegmentsByIds(@PathVariable("model_name") String modelName,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        String modelId = getModel(modelName, request.getProject()).getId();
        return modelController.refreshOrMergeSegmentsByIds(modelId, request);
    }

    @DeleteMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model_name") String modelName,
            @RequestParam("project") String project, // 
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        if (purge) {
            ids = new String[0];
        }
        String modelId = getModel(modelName, project).getId();
        return modelController.deleteSegments(modelId, project, purge, force, ids);
    }

    @GetMapping(value = "/{project}/{model}/model_desc")
    @ResponseBody
    public EnvelopeResponse<NModelDescResponse> getModelDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias) {
        checkProjectName(project);
        NModelDescResponse result = modelService.getModelDesc(modelAlias, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @PutMapping(value = "/{project}/{model}/partition_desc")
    @ResponseBody
    public EnvelopeResponse<String> updateParatitionDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias,
            @RequestBody ModelParatitionDescRequest modelParatitionDescRequest) {
        if (modelParatitionDescRequest.getPartitionDesc() != null) {
            checkRequiredArg("partition_date_column",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateColumn());
            checkRequiredArg("partition_date_format",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateFormat());
        }
        validateDataRange(modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd());
        modelService.updateDataModelParatitionDesc(project, modelAlias, modelParatitionDescRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
