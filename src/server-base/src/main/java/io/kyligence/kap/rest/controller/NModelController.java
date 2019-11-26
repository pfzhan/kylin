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

import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.rest.response.LayoutRecommendationDetailResponse;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggShardByColumnsResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelInfoResponse;
import io.kyligence.kap.rest.response.NRecomendedDataModelResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.RecommendationStatsResponse;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.OptimizeRecommendationService;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

@Controller
@RequestMapping(value = "/api/models")
public class NModelController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NModelController.class);
    private static final String MODEL_ID = "modelId";
    private static final String NEW_MODEL_NAME = "newModelNAME";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    private ModelSemanticHelper semanticService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    @Qualifier("optimizeRecommendationService")
    private OptimizeRecommendationService optimizeRecommendationService;

    @ApiOperation(value = "getModels (update){Red}", notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(
            @RequestParam(value = "model_name", required = false) String modelAlias,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "owner", required = false) String owner,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            models.addAll(modelService.getModels(modelAlias, project, exactMatch, owner, status, sortBy, reverse));
        } else {
            models.addAll(modelService.getRelateModels(project, table, modelAlias));
        }

        models = modelService.addOldParams(models);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(models, offset, limit), "");
    }

    @ApiOperation(value = "offlineAllModelsInProject (update)(check)", notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/disable_all_models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> offlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.offlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "offlineAllModelsInProject (update)(check)", notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/enable_all_models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> onlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.onlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> createModel(@RequestBody ModelRequest modelRequest) throws Exception {
        checkProjectName(modelRequest.getProject());
        validateDataRange(modelRequest.getStart(), modelRequest.getEnd());
        validatePartitionDesc(modelRequest);
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new BadRequestException(e.getMessage(), ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @ApiOperation(value = "batchSaveModels (update)(check)", notes = "Update URL: {project}; Update Param: project")
    @PostMapping(value = "/batch_save_models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> batchSaveModels(@RequestParam("project") String project,
            @RequestBody List<ModelRequest> modelRequests) throws Exception {
        checkProjectName(project);
        try {
            modelService.batchCreateModel(project, modelRequests);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new BadRequestException(e.getMessage(), ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @ApiOperation(value = "suggestModel (check)", notes = "; need check")
    @PostMapping(value = "/suggest_model", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<List<NRecomendedDataModelResponse>> suggestModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.suggestModel(request.getProject(), request.getSqls()), "");
    }

    /**
     * if exist same name model, then return true.
     *
     * @param modelRequest
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "validateModelAlias (check)", notes = "")
    @PostMapping(value = "/validate_model", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> validateModelAlias(@RequestBody ModelRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        if (StringUtils.isEmpty(modelRequest.getUuid()))
            throw new BadRequestException(MsgPicker.getMsg().getMODEL_ID_NOT_FOUND());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, !modelService
                .checkModelAliasUniqueness(modelRequest.getUuid(), modelRequest.getAlias(), modelRequest.getProject()),
                "");
    }

    @ApiOperation(value = "getLatestData (update)", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/data_range/latest_data", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getLatestData(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);

        ExistedDataRangeResponse response;
        try {
            response = modelService.getLatestDataRange(project, modelId);
        } catch (KylinTimeoutException e) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    MsgPicker.getMsg().getPUSHDOWN_DATARANGE_TIMEOUT());
        } catch (Exception e) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    MsgPicker.getMsg().getPUSHDOWN_DATARANGE_ERROR());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getSegments (update)", notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{model:.+}/segments", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "model") String modelId, // 
            @RequestParam(value = "project") String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, project, start, end, sortBy,
                reverse, status);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(segments, offset, limit), "");
    }

    @GetMapping(value = "/model_info", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<List<ModelInfoResponse>> getModelInfo(
            @RequestParam(value = "model", required = false, defaultValue = "*") String model,
            @RequestParam(value = "project", required = false) List<String> projects,
            @RequestParam(value = "suite", required = false, defaultValue = "*") String suite,
            @RequestParam(value = "start", required = false, defaultValue = "0") long start,
            @RequestParam(value = "end", required = false, defaultValue = "0") long end) {
        val result = modelService.getModelInfo(suite, model, projects, start, end);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "getAggIndices (update)", notes = "Update URL: model; Update Param: is_case_sensitive, page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{model:.+}/agg_indices", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<IndicesResponse> getAggIndices(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "index", required = false) Long indexId, //
            @RequestParam(value = "content", required = false) String contentSeg, //
            @RequestParam(value = "is_case_sensitive", required = false, defaultValue = "false") boolean isCaseSensitive, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset, //
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize, //
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val result = modelService.getAggIndices(project, modelId, indexId, contentSeg, isCaseSensitive, pageOffset,
                pageSize, sortBy, reverse);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "updateAggIndicesShardColumns (update)", notes = "Update URL: model;Update Param: model_id")
    @PostMapping(value = "/{model:.+}/agg_indices/shard_columns", produces = HTTP_VND_APACHE_KYLIN_JSON)
    @ResponseBody
    public EnvelopeResponse<String> updateAggIndicesShardColumns(@PathVariable("model") String modelId,
            @RequestBody AggShardByColumnsRequest aggShardByColumnsRequest) {
        checkProjectName(aggShardByColumnsRequest.getProject());
        aggShardByColumnsRequest.setModelId(modelId);
        checkRequiredArg(MODEL_ID, aggShardByColumnsRequest.getModelId());
        indexPlanService.updateShardByColumns(aggShardByColumnsRequest.getProject(), aggShardByColumnsRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getAggIndicesShardColumns (update)", notes = "Update URL: model; Update Response: model_id")
    @GetMapping(value = "/{model:.+}/agg_indices/shard_columns", produces = HTTP_VND_APACHE_KYLIN_JSON)
    @ResponseBody
    public EnvelopeResponse<AggShardByColumnsResponse> getAggIndicesShardColumns(
            @PathVariable(value = "model") String modelId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, indexPlanService.getShardByColumns(project, modelId),
                "");
    }

    @Deprecated
    @ApiOperation(value = "getTableIndices (update)", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/table_indices", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<IndicesResponse> getTableIndices(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getTableIndices(modelId, project), "");
    }

    @ApiOperation(value = "buildIndicesManually (update)", notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/indices", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model") String modelId,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        val response = modelService.buildIndicesManually(modelId, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getModelJson (update)", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/json", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> getModelJson(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {

        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        try {
            String json = modelService.getModelJson(modelId, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, json, "");
        } catch (JsonProcessingException e) {
            throw new BadRequestException("can not get model json " + e);
        }
    }

    @ApiOperation(value = "getModelSql (update)", notes = "Update URL: {model}")
    @GetMapping(value = "{model:.+}/sql", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> getModelSql(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);

        try {
            String sql = modelService.getModelSql(modelId, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, sql, "");
        } catch (Exception e) {
            throw new BadRequestException("can not get model sql, " + e);
        }
    }

    @Deprecated
    @ApiOperation(value = "getModelRelations (update)", notes = "Update URL: {model}")
    @GetMapping(value = "{model:.+}/relations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<List<NSpanningTreeForWeb>> getModelRelations(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        List<NSpanningTreeForWeb> modelRelations = modelService.getModelRelations(modelId, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelRelations, "");
    }

    @GetMapping(value = "/affected_models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<AffectedModelsResponse> getAffectedModelsBySourceTableAction(
            @RequestParam(value = "table") String tableName, // 
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "action") String action) {
        checkProjectName(project);
        checkRequiredArg("table", tableName);
        checkRequiredArg("action", action);

        if ("TOGGLE_PARTITION".equals(action)) {
            modelService.checkSingleIncrementingLoadingTable(project, tableName);
            val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("DROP_TABLE".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByDeletingTable(tableName, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("RELOAD_ROOT_FACT".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, affectedModelResponse, "");
        } else {
            throw new IllegalArgumentException();
        }
    }

    @PutMapping(value = "/semantic", produces = HTTP_VND_APACHE_KYLIN_JSON)
    @ResponseBody
    public EnvelopeResponse<String> updateSemantic(@RequestBody ModelRequest request) throws Exception {
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
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new BadRequestException(e.getMessage(), ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @ApiOperation(value = "updateModelName (update)", notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/name", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        String newAlias = modelRenameRequest.getNewModelName();
        if (!StringUtils.containsOnly(newAlias, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getINVALID_MODEL_NAME(), newAlias));
        }

        modelService.renameDataModel(modelRenameRequest.getProject(), modelId, newAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateModelStatus (update)", notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/status", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.updateDataModelStatus(modelId, modelRenameRequest.getProject(), modelRenameRequest.getStatus());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "unlinkModel (update)", notes = "Update Body: model_id")
    @PutMapping(value = "/{model:.+}/management_type", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unlinkModel(@PathVariable("model") String modelId,
            @RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.unlinkModel(modelId, unlinkModelRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel (update)", notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model") String model,
            @RequestParam("project") String project) {
        checkProjectName(project);
        modelService.dropModel(model, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteSegments (update)(check)", notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}/segments", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model") String model,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);

        if (purge) {
            modelService.purgeModelManually(model, project);
        } else if (ArrayUtils.isEmpty(ids)) {
            throw new BadRequestException("Segments id list can not empty!");
        } else {
            modelService.deleteSegmentById(model, project, ids);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getPurgeModelAffectedResponse (update)", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/purge_effect", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<PurgeModelAffectedResponse> getPurgeModelAffectedResponse(
            @PathVariable(value = "model") String model, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, model);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.getPurgeModelAffectedResponse(project, model), "");
    }

    @ApiOperation(value = "cloneModel (update)(check)", notes = "Add URL: {model}; Update Param: new_model_name")
    @PostMapping(value = "/{model:.+}/clone", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> cloneModel(@PathVariable("model") String modelId,
            @RequestBody ModelCloneRequest request) {
        checkProjectName(request.getProject());
        String newModelName = request.getNewModelName();
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg(NEW_MODEL_NAME, newModelName);
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getINVALID_MODEL_NAME(), newModelName));
        }
        modelService.cloneModel(modelId, request.getNewModelName(), request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshOrMergeSegmentsByIds (update)", notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/segments", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> refreshOrMergeSegmentsByIds(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        if (request.getType().equals(SegmentsRequest.SegmentsRequestType.REFRESH)) {
            if (ArrayUtils.isEmpty(request.getIds())) {
                throw new BadRequestException("You should choose at least one segment to refresh!");
            }
            modelService.refreshSegmentById(modelId, request.getProject(), request.getIds());
        } else {
            if (ArrayUtils.isEmpty(request.getIds()) || request.getIds().length < 2) {
                throw new BadRequestException("You should choose at least two segments to merge!");
            }
            modelService.mergeSegmentsManually(modelId, request.getProject(), request.getIds());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "buildSegmentsManually (update)", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segments", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> buildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        modelService.buildSegmentsManually(buildSegmentsRequest.getProject(), modelId, buildSegmentsRequest.getStart(),
                buildSegmentsRequest.getEnd());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "checkComputedColumns (check)", notes = "Update Response: table_identity, table_alias, column_name, inner_expression, data_type")
    @PostMapping(value = "/computed_columns/check", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<ComputedColumnDesc> checkComputedColumns(
            @RequestBody ComputedColumnCheckRequest modelRequest) {
        NDataModel modelDesc = modelService.convertToDataModel(modelRequest.getModelDesc());
        modelDesc.setSeekingCCAdvice(modelRequest.isSeekingExprAdvice());
        modelService.primaryCheck(modelDesc);
        ComputedColumnDesc checkedCC = modelService.checkComputedColumn(modelDesc, modelRequest.getProject(),
                modelRequest.getCcInCheck());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, checkedCC, "");
    }

    @GetMapping(value = "/computed_columns/usage", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<ComputedColumnUsageResponse> getComputedColumnUsage(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
    }

    @Deprecated
    @ApiOperation(value = "updateModelDataCheckDesc (check)", notes = "URL, front end Deprecated")
    @PutMapping(value = "/{model:.+}/data_check", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateModelDataCheckDesc(@PathVariable("model") String modelId,
            @RequestBody ModelCheckRequest request) {
        request.checkSelf();
        modelService.updateModelDataCheckDesc(request.getProject(), modelId, request.getCheckOptions(),
                request.getFaultThreshold(), request.getFaultActions());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getModelConfig (update)", notes = "Update Param: model_name, page_offset, page_size")
    @GetMapping(value = "/config", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ModelConfigResponse>>> getModelConfig(
            @RequestParam(value = "model_name", required = false) String modelName,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        val modelConfigs = modelService.getModelConfig(project, modelName);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(modelConfigs, offset, limit), "");
    }

    @PutMapping(value = "/{model:.+}/config", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateModelConfig(@PathVariable("model") String modelId,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelId, request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getOptimizeRecommendations (update)", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<OptRecommendationResponse> getOptimizeRecommendations(
            @PathVariable(value = "model") String modelId, // 
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "sources", required = false, defaultValue = "") List<String> sources) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optimizeRecommendationService.getRecommendationByModel(project, modelId, sources), "");
    }

    @ApiOperation(value = "applyOptimizeRecommendations (update)", notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> applyOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestBody ApplyRecommendationsRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        request.setModelId(modelId);
        optimizeRecommendationService.applyRecommendations(request, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "removeOptimizeRecommendations (update)", notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> removeOptimizeRecommendations(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "cc_recommendations", required = false) List<Long> ccItemIds,
            @RequestParam(value = "dimension_recommendations", required = false) List<Long> dimensionItemIds,
            @RequestParam(value = "measure_recommendations", required = false) List<Long> measureItemIds,
            @RequestParam(value = "index_recommendations", required = false) List<Long> indexItemIds) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val request = new RemoveRecommendationsRequest();
        request.setModelId(modelId);
        request.setProject(project);
        if (ccItemIds != null)
            request.setCcItemIds(ccItemIds);
        if (dimensionItemIds != null)
            request.setDimensionItemIds(dimensionItemIds);
        if (measureItemIds != null)
            request.setMeasureItemIds(measureItemIds);
        if (indexItemIds != null)
            request.setIndexItemIds(indexItemIds);

        optimizeRecommendationService.removeRecommendations(request, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getLayoutRecommendationContent (update)", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/recommendations/index", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<LayoutRecommendationDetailResponse> getLayoutRecommendationContent(
            @PathVariable(value = "model") String modelId, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "item_id") long itemId, //
            @RequestParam(value = "content", required = false) String content,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, optimizeRecommendationService
                .getLayoutRecommendationContent(project, modelId, content, itemId, offset, limit), "");
    }

    public static final String ILLEGAL_INPUT_MSG = "Request failed. {project/model name} not found.";
    public static final String ILLEGAL_MODE_MSG = "Request failed. Please check whether the recommendation mode is enabled in expert mode.";

    @ApiOperation(value = "getRecommendationsByProject (update){Red}", notes = "Del URL: project")
    @GetMapping(value = "/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<RecommendationStatsResponse> getRecommendationsByProject(
            @RequestParam("project") String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(project) || !projectService.isExistProject(project)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_ILLEGAL_INPUT, null, ILLEGAL_INPUT_MSG);
        }
        if (!projectService.isSemiAutoProject(project)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_MODE_NOT_MATCH, null, ILLEGAL_MODE_MSG);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optimizeRecommendationService.getRecommendationsStatsByProject(project), "");
    }

    @PutMapping(value = "/recommendations/batch", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> batchApplyRecommendations(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_names", required = false) List<String> modelAlias) {
        checkProjectName(project);
        if (StringUtils.isEmpty(project) || !projectService.isExistProject(project)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_ILLEGAL_INPUT, null, ILLEGAL_INPUT_MSG);
        }
        if (!projectService.isSemiAutoProject(project)) {
            return new EnvelopeResponse<>(ResponseCode.CODE_MODE_NOT_MATCH, null, ILLEGAL_MODE_MSG);
        }
        optimizeRecommendationService.batchApplyRecommendations(project, modelAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    public void validatePartitionDesc(NDataModel model) {
        if (model.getPartitionDesc() != null
                && StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            throw new BadRequestException("Partition column does not exist!");
        }
    }
}
