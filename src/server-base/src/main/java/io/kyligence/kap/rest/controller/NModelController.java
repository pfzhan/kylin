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

import io.kyligence.kap.metadata.model.exception.LookupTableException;
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

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.response.IndexEntityResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import lombok.val;

@Controller
@RequestMapping(value = "/models")
public class NModelController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NModelController.class);
    private static final Message msg = MsgPicker.getMsg();
    private static final String MODEL_ID = "modelId";
    private static final String NEW_MODEL_NAME = "newModelNAME";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private ModelSemanticHelper semanticService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModels(@RequestParam(value = "model", required = false) String modelAlias,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "owner", required = false) String owner,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            for (NDataModelResponse modelDesc : modelService.getModels(modelAlias, project, exactMatch, owner, status,
                    sortBy, reverse)) {
                Preconditions.checkState(!modelDesc.isDraft());
                models.add(modelDesc);
            }
        } else {
            models.addAll(modelService.getRelateModels(project, table, modelAlias));
        }

        HashMap<String, Object> modelResponse = getDataResponse("models", models, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "/{project}/disable_all_models", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse offlineAllModelsInProject(@PathVariable("project") String project) {
        checkProjectName(project);
        modelService.offlineAllModelsInProject(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{project}/enable_all_models", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse onlineAllModelsInProject(@PathVariable("project") String project) {
        checkProjectName(project);
        modelService.onlineAllModelsInProject(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse createModel(@RequestBody ModelRequest modelRequest) throws Exception {
        checkProjectName(modelRequest.getProject());
        validateDataRange(modelRequest.getStart(), modelRequest.getEnd());
        validatePartitionDesc(modelRequest);
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
        } catch (LookupTableException e) {
            throw new BadRequestException(e.getMessage(), ResponseCode.CODE_UNDEFINED, e);
        }

    }

    @RequestMapping(value = "/data_range/latest_data", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getLatestData(@RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "partitionColumn", required = false) String column,
            @RequestParam(value = "model", required = false) String modelId) throws Exception {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                modelService.getLatestDataRange(project, table, column, modelId), "");
    }

    @RequestMapping(value = "/segments", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSegments(@RequestParam(value = "model", required = true) String modelId,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, project, start, end, sortBy,
                reverse, status);
        HashMap<String, Object> response = getDataResponse("segments", segments, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/model_info", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelInfo(
            @RequestParam(value = "model", required = false, defaultValue = "*") String model,
            @RequestParam(value = "project", required = false) List<String> projects,
            @RequestParam(value = "suite", required = false, defaultValue = "*") String suite,
            @RequestParam(value = "start", required = false, defaultValue = "0") long start,
            @RequestParam(value = "end", required = false, defaultValue = "0") long end) {
        val result = modelService.getModelInfo(suite, model, projects, start, end);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/agg_indices", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAggIndices(@RequestParam(value = "model", required = true) String modelId,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        List<IndexEntityResponse> aggIndices = modelService.getAggIndices(modelId, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggIndices, "");
    }

    @RequestMapping(value = "/cuboids", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCuboids(@RequestParam(value = "id", required = true) Long id,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "model", required = true) String modelId) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        IndexEntityResponse indexEntityResponse = modelService.getCuboidById(modelId, project, id);
        if (indexEntityResponse == null) {
            throw new BadRequestException("Can not find this cuboid " + id);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, indexEntityResponse, "");
    }

    @RequestMapping(value = "/table_indices", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableIndices(@RequestParam(value = "model", required = true) String modelId,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        List<IndexEntityResponse> tableIndices = modelService.getTableIndices(modelId, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableIndices, "");
    }

    @RequestMapping(value = "/indices", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse buildIndicesManually(@RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        val response = modelService.buildIndicesManually(request.getModelId(), request.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/json", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelJson(@RequestParam(value = "model", required = true) String modelId,
            @RequestParam(value = "project", required = true) String project) {

        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        try {
            String json = modelService.getModelJson(modelId, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, json, "");
        } catch (JsonProcessingException e) {
            throw new BadRequestException("can not get model json " + e);
        }

    }

    @RequestMapping(value = "/relations", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelRelations(@RequestParam(value = "model", required = true) String modelId,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        List<NSpanningTreeForWeb> modelRelations = modelService.getModelRelations(modelId, project);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelRelations, "");
    }

    @RequestMapping(value = "/affected_models", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAffectedModelsBySourceTableAction(
            @RequestParam(value = "table", required = true) String tableName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "action", required = true) String action
    ) {
        checkProjectName(project);
        checkRequiredArg("table", tableName);
        checkRequiredArg("action", action);

        if ("TOGGLE_PARTITION".equals(action)) {
            modelService.checkSingleIncrementingLoadingTable(project, tableName);
            val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("DROP_TABLE".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByDeletingTable(tableName, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("RELOAD_ROOT_FACT".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else {
            throw new IllegalArgumentException();
        }
    }

    @PutMapping(value = "/semantic", produces = "application/vnd.apache.kylin-v2+json")
    @ResponseBody
    public EnvelopeResponse updateSemantic(@RequestBody ModelRequest request) throws Exception {
        checkProjectName(request.getProject());
        validateDataRange(request.getStart(), request.getEnd());
        validatePartitionDesc(request);
        checkRequiredArg(MODEL_ID, request.getUuid());
        try {
            if (request.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                modelService.repairBrokenModel(request.getProject(), request);
            } else {
                modelService.updateDataModelSemantic(request.getProject(), request);
            }
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
        } catch (LookupTableException e) {
            throw new BadRequestException(e.getMessage(), ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @RequestMapping(value = "/name", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelName(@RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelRenameRequest.getModelId());
        String newAlias = modelRenameRequest.getNewModelName();
        if (!StringUtils.containsOnly(newAlias, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newAlias));
        }

        modelService.renameDataModel(modelRenameRequest.getProject(), modelRenameRequest.getModelId(), newAlias);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/status", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelStatus(@RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelRenameRequest.getModelId());
        modelService.updateDataModelStatus(modelRenameRequest.getModelId(), modelRenameRequest.getProject(),
                modelRenameRequest.getStatus());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/management_type", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unlinkModel(@RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_ID, unlinkModelRequest.getModelId());
        modelService.unlinkModel(unlinkModelRequest.getModelId(), unlinkModelRequest.getProject());
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

    @RequestMapping(value = "/purgeEffect", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPurgeModelAffectedResponse(
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "model", required = true) String model) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, model);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                modelService.getPurgeModelAffectedResponse(project, model), "");
    }

    @RequestMapping(value = "/clone", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneModel(@RequestBody ModelCloneRequest request) {
        checkProjectName(request.getProject());
        String newModelName = request.getNewModelName();
        String modelName = request.getModelId();
        checkRequiredArg(MODEL_ID, modelName);
        checkRequiredArg(NEW_MODEL_NAME, newModelName);
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newModelName));
        }
        modelService.cloneModel(request.getModelId(), request.getNewModelName(), request.getProject());
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
        modelService.refreshSegmentById(request.getModelId(), request.getProject(), request.getIds());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segments", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse buildSegmentsManually(@RequestBody BuildSegmentsRequest buildSegmentsRequest)
            throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        modelService.buildSegmentsManually(buildSegmentsRequest.getProject(), buildSegmentsRequest.getModelId(),
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
        ComputedColumnDesc checkedCC = modelService.checkComputedColumn(modelDesc, modelRequest.getProject(),
                modelRequest.getCcInCheck());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, checkedCC, "");
    }

    @RequestMapping(value = "/computed_columns/usage", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getComputedColumnUsage(@RequestParam(value = "project", required = true) String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
    }

    @RequestMapping(value = "/{id}/data_check", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDataCheckDesc(@PathVariable("id") String modelId,
            @RequestBody ModelCheckRequest request) {
        request.checkSelf();
        modelService.updateModelDataCheckDesc(request.getProject(), modelId, request.getCheckOptions(),
                request.getFaultThreshold(), request.getFaultActions());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelConfig(@RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        val modelConfigs = modelService.getModelConfig(project, modelName);
        HashMap<String, Object> modelResponse = getDataResponse("model_config", modelConfigs, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "/{id}/config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelConfig(@PathVariable("id") String modelId,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelId, request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    public void validatePartitionDesc(NDataModel model) {
        if (model.getPartitionDesc() != null
                && StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            throw new BadRequestException("Partition column does not exist!");
        }
    }
}
