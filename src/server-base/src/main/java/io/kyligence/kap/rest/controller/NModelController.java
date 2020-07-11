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
import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SEGMENT_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_MERGE_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_REFRESH_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccerelateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
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
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.RecommendationItem;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.ApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.IncrementBuildSegmentsRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.NRecommendationListRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.RemoveRecommendationsRequest;
import io.kyligence.kap.rest.request.SegmentFixRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggShardByColumnsResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.LayoutRecommendationDetailResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NRecomendationListResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.RecommendationStatsResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.OptimizeRecommendationService;
import io.kyligence.kap.rest.service.ProjectService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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

    @ApiOperation(value = "getModels{Red}", notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(
            @RequestParam(value = "model_name", required = false) String modelAlias,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "owner", required = false) String owner,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "model_alias_or_owner", required = false) String modelAliasOrOwner,
            @RequestParam(value = "last_modify_from", required = false) Long lastModifyFrom,
            @RequestParam(value = "last_modify_to", required = false) Long lastModifyTo) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            models.addAll(modelService.getModels(modelAlias, project, exactMatch, owner, status, sortBy, reverse,
                    modelAliasOrOwner, lastModifyFrom, lastModifyTo));
        } else {
            models.addAll(modelService.getRelateModels(project, table, modelAlias));
        }

        models = modelService.addOldParams(models);
        models = modelService.updateReponseAcl(models, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(models, offset, limit), "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/disable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> offlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.offlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/enable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> onlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.onlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> createModel(@RequestBody ModelRequest modelRequest) throws Exception {
        checkProjectName(modelRequest.getProject());
        validatePartitionDesc(modelRequest.getPartitionDesc());
        String partitionDateFormat = modelRequest.getPartitionDesc() == null ? null
                : modelRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(modelRequest.getStart(), modelRequest.getEnd(), partitionDateFormat);
        try {
            modelService.createModel(modelRequest.getProject(), modelRequest);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e);
        }
    }

    @Deprecated
    @ApiOperation(value = "batchSaveModels", notes = "Update URL: {project}; Update Param: project")
    @PostMapping(value = "/batch_save_models")
    @ResponseBody
    public EnvelopeResponse<String> batchSaveModels(@RequestParam("project") String project,
            @RequestBody List<ModelRequest> modelRequests) throws Exception {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        try {
            modelService.batchCreateModel(project, modelRequests, Lists.newArrayList());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }

    @ApiOperation(value = "suggestModel", notes = "")
    @PostMapping(value = "/suggest_model")
    @ResponseBody
    public EnvelopeResponse<NRecomendationListResponse> suggestModel(@RequestBody SqlAccerelateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.suggestModel(request.getProject(), request.getSqls(), request.getReuseExistedModel()), "");
    }

    @ApiOperation(value = "suggestModel", notes = "")
    @PostMapping(value = "/model_recommendation")
    @ResponseBody
    public EnvelopeResponse<String> approveSuggestModel(@RequestBody NRecommendationListRequest request)
            throws Exception {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
//        modelService.approveSuggestModel(request.getProject(), request.getNewModels(), request.getRecommendations());
        try {
            modelService.batchCreateModel(request.getProject(), request.getNewModels(), request.getRecommendations());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }

    /**
     * if exist same name model, then return true.
     *
     * @param modelRequest
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "validateModelAlias", notes = "")
    @PostMapping(value = "/validate_model")
    @ResponseBody
    public EnvelopeResponse<Boolean> validateModelAlias(@RequestBody ModelRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelRequest.getUuid());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, !modelService
                .checkModelAliasUniqueness(modelRequest.getUuid(), modelRequest.getAlias(), modelRequest.getProject()),
                "");
    }

    @ApiOperation(value = "checkIfCanAnsweredByExistedModel", notes = "")
    @PostMapping(value = "/can_answered_by_existed_model")
    @ResponseBody
    public EnvelopeResponse<Boolean> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.couldAnsweredByExistedModel(request.getProject(), request.getSqls()), "");
    }

    /**
     * list model that is scd2 join condition
     *
     * @param project
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "listScd2Model", notes = "")
    @GetMapping(value = "name/scd2")
    @ResponseBody
    public EnvelopeResponse<List<String>> listScd2Model(@RequestParam("project") String project,
            @RequestParam(value = "non_offline", required = false, defaultValue = "true") boolean nonOffline) {
        checkProjectName(project);

        List<String> status = nonOffline ? modelService.getModelNonOffOnlineStatus()
                : Arrays.asList(ModelStatusToDisplayEnum.OFFLINE.name());
        List<String> scd2ModelsOnline = modelService.getSCD2ModelsAliasByStatus(project, status);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, scd2ModelsOnline, "");
    }

    @ApiOperation(value = "getLatestData", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/data_range/latest_data")
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getLatestData(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        return getPartitionLatestData(project, modelId, null);
    }

    @ApiOperation(value = "getLatestData", notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/data_range/latest_data")
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getPartitionLatestData(
            @PathVariable(value = "model") String modelId, @RequestBody PartitionColumnRequest request) {
        return getPartitionLatestData(request.getProject(), modelId, request.getPartitionDesc());
    }

    private EnvelopeResponse<ExistedDataRangeResponse> getPartitionLatestData(String project, String modelId,
            PartitionDesc partitionDesc) {
        checkProjectName(project);

        ExistedDataRangeResponse response;
        try {
            response = modelService.getLatestDataRange(project, modelId, partitionDesc);
        } catch (KylinTimeoutException e) {
            throw new KylinException(UNKNOWN_ERROR_CODE, MsgPicker.getMsg().getPUSHDOWN_DATARANGE_TIMEOUT());
        } catch (Exception e) {
            throw new KylinException(INVALID_RANGE, MsgPicker.getMsg().getPUSHDOWN_DATARANGE_ERROR());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getAggIndices", notes = "Update URL: model; Update Param: is_case_sensitive, page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{model:.+}/agg_indices")
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

    @ApiOperation(value = "updateAggIndicesShardColumns", notes = "Update URL: model;Update Param: model_id")
    @PostMapping(value = "/{model:.+}/agg_indices/shard_columns")
    @ResponseBody
    public EnvelopeResponse<String> updateAggIndicesShardColumns(@PathVariable("model") String modelId,
            @RequestBody AggShardByColumnsRequest aggShardByColumnsRequest) {
        checkProjectName(aggShardByColumnsRequest.getProject());
        aggShardByColumnsRequest.setModelId(modelId);
        checkRequiredArg(MODEL_ID, aggShardByColumnsRequest.getModelId());
        indexPlanService.updateShardByColumns(aggShardByColumnsRequest.getProject(), aggShardByColumnsRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getAggIndicesShardColumns", notes = "Update URL: model; Update Response: model_id")
    @GetMapping(value = "/{model:.+}/agg_indices/shard_columns")
    @ResponseBody
    public EnvelopeResponse<AggShardByColumnsResponse> getAggIndicesShardColumns(
            @PathVariable(value = "model") String modelId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, indexPlanService.getShardByColumns(project, modelId),
                "");
    }

    @Deprecated
    @ApiOperation(value = "getTableIndices", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/table_indices")
    @ResponseBody
    public EnvelopeResponse<IndicesResponse> getTableIndices(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getTableIndices(modelId, project), "");
    }

    @ApiOperation(value = "buildIndicesManually", notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/indices")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model") String modelId,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);

        modelService.validateCCType(modelId, request.getProject());

        val response = modelService.buildIndicesManually(modelId, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getModelJson", notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/json")
    @ResponseBody
    public EnvelopeResponse<String> getModelJson(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {

        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        try {
            String json = modelService.getModelJson(modelId, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, json, "");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can not get model json " + e);
        }
    }

    @ApiOperation(value = "getModelSql", notes = "Update URL: {model}")
    @GetMapping(value = "{model:.+}/sql")
    @ResponseBody
    public EnvelopeResponse<String> getModelSql(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);

        try {
            String sql = modelService.getModelSql(modelId, project);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, sql, "");
        } catch (Exception e) {
            throw new RuntimeException("can not get model sql, " + e);
        }
    }

    @Deprecated
    @ApiOperation(value = "getModelRelations", notes = "Update URL: {model}")
    @GetMapping(value = "{model:.+}/relations")
    @ResponseBody
    public EnvelopeResponse<List<NSpanningTreeForWeb>> getModelRelations(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        List<NSpanningTreeForWeb> modelRelations = modelService.getModelRelations(modelId, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelRelations, "");
    }

    @GetMapping(value = "/affected_models", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
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

    @PutMapping(value = "/semantic")
    @ResponseBody
    public EnvelopeResponse<String> updateSemantic(@RequestBody ModelRequest request) throws Exception {
        checkProjectName(request.getProject());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(request.getProject(), request.getId());
        validateDataRange(request.getStart(), request.getEnd(), partitionColumnFormat);
        validatePartitionDesc(request.getPartitionDesc());
        checkRequiredArg(MODEL_ID, request.getUuid());
        try {
            if (request.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                modelService.repairBrokenModel(request.getProject(), request);
            } else {
                modelService.updateDataModelSemantic(request.getProject(), request);
            }
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_UPDATE_MODEL, e);
        }
    }

    @PutMapping(value = "/{model:.+}/partition")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionSemantic(@PathVariable("model") String modelId,
            @RequestBody PartitionColumnRequest request) throws Exception {
        checkProjectName(request.getProject());
        validatePartitionDesc(request.getPartitionDesc());
        checkRequiredArg(MODEL_ID, modelId);
        try {
            modelService.updatePartitionColumn(request.getProject(), modelId, request.getPartitionDesc());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_UPDATE_MODEL, e);
        }
    }

    @ApiOperation(value = "updateModelName", notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/name")
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        String newAlias = modelRenameRequest.getNewModelName();
        if (!StringUtils.containsOnly(newAlias, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(MsgPicker.getMsg().getINVALID_MODEL_NAME(), newAlias));
        }

        modelService.renameDataModel(modelRenameRequest.getProject(), modelId, newAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateModelStatus", notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/status")
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.updateDataModelStatus(modelId, modelRenameRequest.getProject(), modelRenameRequest.getStatus());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "unlinkModel", notes = "Update Body: model_id")
    @PutMapping(value = "/{model:.+}/management_type")
    @ResponseBody
    public EnvelopeResponse<String> unlinkModel(@PathVariable("model") String modelId,
            @RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.unlinkModel(modelId, unlinkModelRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel", notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model") String model,
            @RequestParam("project") String project) {
        checkProjectName(project);
        modelService.dropModel(model, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getPurgeModelAffectedResponse", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/purge_effect")
    @ResponseBody
    public EnvelopeResponse<PurgeModelAffectedResponse> getPurgeModelAffectedResponse(
            @PathVariable(value = "model") String model, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, model);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.getPurgeModelAffectedResponse(project, model), "");
    }

    @ApiOperation(value = "cloneModel", notes = "Add URL: {model}; Update Param: new_model_name")
    @PostMapping(value = "/{model:.+}/clone")
    @ResponseBody
    public EnvelopeResponse<String> cloneModel(@PathVariable("model") String modelId,
            @RequestBody ModelCloneRequest request) {
        checkProjectName(request.getProject());
        String newModelName = request.getNewModelName();
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg(NEW_MODEL_NAME, newModelName);
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_NAME_FOR_MODEL_DIMENSION_MEASURE)) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(MsgPicker.getMsg().getINVALID_MODEL_NAME(), newModelName));
        }
        modelService.cloneModel(modelId, request.getNewModelName(), request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "checkComputedColumns", notes = "Update Response: table_identity, table_alias, column_name, inner_expression, data_type")
    @PostMapping(value = "/computed_columns/check")
    @ResponseBody
    public EnvelopeResponse<ComputedColumnDesc> checkComputedColumns(
            @RequestBody ComputedColumnCheckRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        modelRequest.getModelDesc().setProject(modelRequest.getProject());
        NDataModel modelDesc = modelService.convertToDataModel(modelRequest.getModelDesc());
        modelDesc.setSeekingCCAdvice(modelRequest.isSeekingExprAdvice());
        modelService.primaryCheck(modelDesc);
        ComputedColumnDesc checkedCC = modelService.checkComputedColumn(modelDesc, modelRequest.getProject(),
                modelRequest.getCcInCheck());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, checkedCC, "");
    }

    @GetMapping(value = "/computed_columns/usage")
    @ResponseBody
    public EnvelopeResponse<ComputedColumnUsageResponse> getComputedColumnUsage(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
    }

    @Deprecated
    @ApiOperation(value = "updateModelDataCheckDesc", notes = "URL, front end Deprecated")
    @PutMapping(value = "/{model:.+}/data_check")
    @ResponseBody
    public EnvelopeResponse<String> updateModelDataCheckDesc(@PathVariable("model") String modelId,
            @RequestBody ModelCheckRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelDataCheckDesc(request.getProject(), modelId, request.getCheckOptions(),
                request.getFaultThreshold(), request.getFaultActions());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getModelConfig", notes = "Update Param: model_name, page_offset, page_size")
    @GetMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ModelConfigResponse>>> getModelConfig(
            @RequestParam(value = "model_name", required = false) String modelAlias,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        val modelConfigs = modelService.getModelConfig(project, modelAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(modelConfigs, offset, limit), "");
    }

    @PutMapping(value = "/{model:.+}/config")
    @ResponseBody
    public EnvelopeResponse<String> updateModelConfig(@PathVariable("model") String modelId,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelId, request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getOptimizeRecommendations", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/recommendations")
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

    @ApiOperation(value = "applyOptimizeRecommendations", notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<String> applyOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestBody ApplyRecommendationsRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        request.setModelId(modelId);
        optimizeRecommendationService.applyRecommendations(request, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "removeOptimizeRecommendations", notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<String> removeOptimizeRecommendations(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "cc_recommendations", required = false) List<Long> ccItemIds,
            @RequestParam(value = "dimension_recommendations", required = false) List<Long> dimensionItemIds,
            @RequestParam(value = "measure_recommendations", required = false) List<Long> measureItemIds,
            @RequestParam(value = "index_recommendations", required = false) List<Long> indexItemIds,
            @RequestParam(value = "all", required = false) boolean cleanAll) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        val request = new RemoveRecommendationsRequest();
        request.setModelId(modelId);
        request.setProject(project);
        if (cleanAll) {
            request.setCleanAll(true);
            val optimizeRecommendation = OptimizeRecommendationManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).getOptimizeRecommendation(modelId);
            request.setCcItemIds(getItemIds(optimizeRecommendation.getCcRecommendations()));
            request.setDimensionItemIds(getItemIds(optimizeRecommendation.getDimensionRecommendations()));
            request.setMeasureItemIds(getItemIds(optimizeRecommendation.getMeasureRecommendations()));
            request.setIndexItemIds(getItemIds(optimizeRecommendation.getLayoutRecommendations()));
        } else {
            if (ccItemIds != null)
                request.setCcItemIds(ccItemIds);
            if (dimensionItemIds != null)
                request.setDimensionItemIds(dimensionItemIds);
            if (measureItemIds != null)
                request.setMeasureItemIds(measureItemIds);
            if (indexItemIds != null)
                request.setIndexItemIds(indexItemIds);
        }

        optimizeRecommendationService.removeRecommendations(request, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    private <T extends RecommendationItem<T>> List<Long> getItemIds(List<T> recommendationItems) {
        return recommendationItems.stream().map(RecommendationItem::getItemId).collect(Collectors.toList());
    }

    @ApiOperation(value = "getLayoutRecommendationContent", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/recommendations/index")
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

    @ApiOperation(value = "getRecommendationsByProject{Red}", notes = "Del URL: project")
    @GetMapping(value = "/recommendations")
    @ResponseBody
    public EnvelopeResponse<RecommendationStatsResponse> getRecommendationsByProject(
            @RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optimizeRecommendationService.getRecommendationsStatsByProject(project), "");
    }

    @PutMapping(value = "/recommendations/batch")
    @ResponseBody
    public EnvelopeResponse<String> batchApplyRecommendations(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_names", required = false) List<String> modelAlias) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        optimizeRecommendationService.batchApplyRecommendations(project, modelAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    public void validatePartitionDesc(PartitionDesc partitionDesc) {
        if (partitionDesc != null) {
            if (StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())) {
                throw new KylinException(INVALID_PARTITION_COLUMN, MsgPicker.getMsg().getPARTITION_COLUMN_NOT_EXIST());
            }
            if (partitionDesc.getPartitionDateFormat() != null) {
                validateDateTimeFormatPattern(partitionDesc.getPartitionDateFormat());
            }
        }
    }

    @ApiOperation(value = "checkBeforeModelSave")
    @PostMapping(value = "/model_save/check")
    @ResponseBody
    public EnvelopeResponse<ModelSaveCheckResponse> checkBeforeModelSave(@RequestBody ModelRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        ModelSaveCheckResponse response = modelService.checkBeforeModelSave(modelRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PutMapping(value = "/{model:.+}/owner")
    @ResponseBody
    public EnvelopeResponse<String> updateModelOwner(@PathVariable("model") String modelId,
            @RequestBody OwnerChangeRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("owner", request.getOwner());
        modelService.updateModelOwner(request.getProject(), modelId, request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    /* Segments */
    @ApiOperation(value = "getSegments", notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "model") String modelId, //
            @RequestParam(value = "project") String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "0") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "with_indexes", required = false) List<Long> withAllIndexes,
            @RequestParam(value = "without_indexes", required = false) List<Long> withoutAnyIndexes,
            @RequestParam(value = "all_to_complement", required = false, defaultValue = "false") Boolean allToComplement,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, project, start, end, status,
                withAllIndexes, withoutAnyIndexes, allToComplement, sortBy, reverse);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(segments, offset, limit), "");
    }

    @ApiOperation(value = "fixSegmentsManually", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segment_holes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> fixSegHoles(@PathVariable("model") String modelId,
            @RequestBody SegmentFixRequest segmentsRequest) throws Exception {
        checkProjectName(segmentsRequest.getProject());
        checkRequiredArg("segment_holes", segmentsRequest.getSegmentHoles());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(segmentsRequest.getProject(), modelId);
        segmentsRequest.getSegmentHoles()
                .forEach(seg -> validateDataRange(seg.getStart(), seg.getEnd(), partitionColumnFormat));
        JobInfoResponse response = modelService.fixSegmentHoles(segmentsRequest.getProject(), modelId,
                segmentsRequest.getSegmentHoles());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "checkSegments")
    @PostMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkSegment(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(buildSegmentsRequest.getProject(),
                modelId);
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        val res = modelService.checkSegHoleExistIfNewRangeBuild(buildSegmentsRequest.getProject(), modelId,
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "checkSegmentsIfDelete")
    @GetMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkHolesIfSegDeleted(@PathVariable("model") String model,
            @RequestParam("project") String project, @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        val res = modelService.checkSegHoleIfSegDeleted(model, project, ids);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "deleteSegments", notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model") String model,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);

        if (purge) {
            modelService.purgeModelManually(model, project);
        } else if (ArrayUtils.isEmpty(ids)) {
            throw new KylinException(EMPTY_SEGMENT_ID, MsgPicker.getMsg().getSEGMENT_LIST_IS_EMPTY());
        } else {
            modelService.deleteSegmentById(model, project, ids, force);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshOrMergeSegmentsByIds", notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> refreshOrMergeSegmentsByIds(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        if (request.getType().equals(SegmentsRequest.SegmentsRequestType.REFRESH)) {
            if (ArrayUtils.isEmpty(request.getIds())) {
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
            }
            modelService.refreshSegmentById(modelId, request.getProject(), request.getIds());
        } else {
            if (ArrayUtils.isEmpty(request.getIds()) || request.getIds().length < 2) {
                throw new KylinException(FAILED_MERGE_SEGMENT,
                        MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
            }
            modelService.mergeSegmentsManually(modelId, request.getProject(), request.getIds());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "buildSegmentsManually", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(buildSegmentsRequest.getProject(),
                modelId);
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());
        JobInfoResponse response = modelService.buildSegmentsManually(buildSegmentsRequest.getProject(), modelId,
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.isBuildAllIndexes());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/model_segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> incrementBuildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody IncrementBuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        String partitionColumnFormat = buildSegmentsRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());
        JobInfoResponse response = modelService.incrementBuildSegmentsManually(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.getPartitionDesc(), buildSegmentsRequest.getSegmentHoles(),
                buildSegmentsRequest.isBuildAllIndexes());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        JobInfoResponseWithFailure response = modelService.addIndexesToSegments(
                buildSegmentsRequest.getProject(),
                modelId,
                buildSegmentsRequest.getSegmentIds(),
                buildSegmentsRequest.getIndexIds(),
                buildSegmentsRequest.isParallelBuildBySegment());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/all_indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addAllIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        JobInfoResponseWithFailure response = modelService.addIndexesToSegments(
                buildSegmentsRequest.getProject(),
                modelId,
                buildSegmentsRequest.getSegmentIds(),
                null,
                buildSegmentsRequest.isParallelBuildBySegment());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes/deletion")
    @ResponseBody
    public EnvelopeResponse<String> deleteIndexesFromSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest deleteSegmentsRequest) throws Exception {
        checkProjectName(deleteSegmentsRequest.getProject());
        modelService.removeIndexesFromSegments(deleteSegmentsRequest.getProject(), modelId,
                deleteSegmentsRequest.getSegmentIds(), deleteSegmentsRequest.getIndexIds());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
