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

import io.kyligence.kap.rest.response.NDataSegmentResponse;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NSpanningTreeResponse;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import lombok.val;

@Controller
@RequestMapping(value = "/models")
public class NModelController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NModelController.class);
    private static final Message msg = MsgPicker.getMsg();
    private static final String MODEL_NAME = "modelName";
    private static final String NEW_MODEL_NAME = "newModelNAME";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private ModelSemanticHelper semanticService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModels(@RequestParam(value = "model", required = false) String modelName,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "owner", required = false) String owner,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sortby", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            for (NDataModelResponse modelDesc : modelService.getModels(modelName, project, exactMatch, owner, status,
                    sortBy, reverse)) {
                Preconditions.checkState(!modelDesc.isDraft());
                models.add(modelDesc);
            }
        } else {
            models.addAll(modelService.getRelateModels(project, table, modelName));
        }

        HashMap<String, Object> modelResponse = getDataResponse("models", models, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse createModel(@RequestBody ModelRequest modelRequest) throws Exception {
        checkProjectName(modelRequest.getProject());
        modelService.createModel(modelRequest.getProject(), modelRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segments", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSegments(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse){
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelName, project, start, end, sortBy, reverse, status);
        HashMap<String, Object> response = getDataResponse("segments", segments, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/model_info", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelInfo(
            @RequestParam(value = "model", required = false, defaultValue = "*") String model,
            @RequestParam(value = "project", required = false, defaultValue = "*") String project,
            @RequestParam(value = "suite", required = false, defaultValue = "*") String suite,
            @RequestParam(value = "start", required = false, defaultValue = "0") long start,
            @RequestParam(value = "end", required = false, defaultValue = "0") long end) {
        val result = modelService.getModelInfo(suite, model, project, start, end);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/agg_indices", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAggIndices(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        List<CuboidDescResponse> aggIndices = modelService.getAggIndices(modelName, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggIndices, "");
    }

    @RequestMapping(value = "/cuboids", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCuboids(@RequestParam(value = "id", required = true) Long id,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "model", required = true) String modelName) {
        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        CuboidDescResponse cuboidDesc = modelService.getCuboidById(modelName, project, id);
        if (cuboidDesc == null) {
            throw new BadRequestException("Can not find this cuboid " + id);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cuboidDesc, "");
    }

    @RequestMapping(value = "/table_indices", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableIndices(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        List<CuboidDescResponse> tableIndices = modelService.getTableIndices(modelName, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableIndices, "");
    }

    @RequestMapping(value = "/json", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelJson(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {

        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        try {
            String json = modelService.getModelJson(modelName, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, json, "");
        } catch (JsonProcessingException e) {
            throw new BadRequestException("can not get model json " + e);
        }

    }

    @RequestMapping(value = "/relations", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelRelations(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        List<NSpanningTreeResponse> modelRelations = modelService.getSimplifiedModelRelations(modelName, project);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelRelations, "");
    }

    @RequestMapping(value = "/affected_models", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAffectedModelsByToggleTableType(
            @RequestParam(value = "table", required = true) String tableName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "fact", required = true) boolean fact) {
        checkProjectName(project);
        checkRequiredArg("table", tableName);
        val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project, fact);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
    }

    @PutMapping(value = "/semantic", produces = "application/vnd.apache.kylin-v2+json")
    @ResponseBody
    public EnvelopeResponse updateSemantic(@RequestBody ModelRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_NAME, request.getName());
        modelService.updateDataModelSemantic(request.getProject(), request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/name", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelName(@RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_NAME, modelRenameRequest.getModelName());
        String newAlias = modelRenameRequest.getNewModelName();
        if (!StringUtils.containsOnly(newAlias, ModelService.VALID_MODEL_NAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", newAlias);
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newAlias));
        }

        modelService.renameDataModel(modelRenameRequest.getProject(), modelRenameRequest.getModelName(), newAlias);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/status", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelStatus(@RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_NAME, modelRenameRequest.getModelName());
        modelService.updateDataModelStatus(modelRenameRequest.getModelName(), modelRenameRequest.getProject(),
                modelRenameRequest.getStatus());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/management_type", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unlinkModel(@RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_NAME, unlinkModelRequest.getModelName());
        modelService.unlinkModel(unlinkModelRequest.getModelName(), unlinkModelRequest.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{project}/{model}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse deleteModel(@PathVariable("project") String project, @PathVariable("model") String model) {
        checkProjectName(project);
        modelService.dropModel(model, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segments/{project}/{model}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse deleteSegments(@PathVariable("project") String project, @PathVariable("model") String model,
            @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        if (ArrayUtils.isEmpty(ids)) {
            modelService.purgeModelManually(model, project);
        } else {
            modelService.deleteSegmentById(model, project, ids);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/clone", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneModel(@RequestBody ModelCloneRequest request) {
        checkProjectName(request.getProject());
        String newModelName = request.getNewModelName();
        String modelName = request.getModelName();
        checkRequiredArg(MODEL_NAME, modelName);
        checkRequiredArg(NEW_MODEL_NAME, newModelName);
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_MODEL_NAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", newModelName);
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newModelName));
        }
        modelService.cloneModel(request.getModelName(), request.getNewModelName(), request.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/segments", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse refreshSegmentsByIds(@RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        if (ArrayUtils.isEmpty(request.getIds())) {
            throw new BadRequestException("You should choose at least one segment to refresh!");
        }
        modelService.refreshSegmentById(request.getModelName(), request.getProject(), request.getIds());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segments", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse buildSegmentsManually(@RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        if (StringUtils.isNotEmpty(buildSegmentsRequest.getStart()) && StringUtils.isNotEmpty(buildSegmentsRequest.getEnd())) {
            validateRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        }
        modelService.buildSegmentsManually(buildSegmentsRequest.getProject(), buildSegmentsRequest.getModel(),
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/computed_columns/check", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse checkComputedColumns(@RequestBody ComputedColumnCheckRequest modelRequest)
            throws IOException {
        NDataModel modelDesc = modelService.convertToDataModel(modelRequest.getModelDesc());
        modelDesc.setSeekingCCAdvice(modelRequest.isSeekingExprAdvice());
        modelService.primaryCheck(modelDesc);
        modelService.checkComputedColumn(modelDesc, modelRequest.getProject(), modelRequest.getCcInCheck());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/computed_columns/usage", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getComputedColumnUsage(@RequestParam(value = "project", required = true) String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
    }

    @RequestMapping(value = "/{name}/data_check", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDataCheckDesc(@PathVariable("name") String modelName,
            @RequestBody ModelCheckRequest request) {
        request.checkSelf();
        modelService.updateModelDataCheckDesc(request.getProject(), modelName, request.getCheckOptions(),
                request.getFaultThreshold(), request.getFaultActions());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelConfig(@RequestParam(value = "model", required = false) String modelName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        val modelConfigs = modelService.getModelConfig(project);
        HashMap<String, Object> modelResponse = getDataResponse("model_config", modelConfigs, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "/{name}/config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelConfig(@PathVariable("name") String modelName,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelName, request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }
}
