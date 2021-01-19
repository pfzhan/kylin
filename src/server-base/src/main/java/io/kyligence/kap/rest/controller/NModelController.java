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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SEGMENT_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DETECT_DATA_RANGE;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_MERGE_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_REFRESH_SEGMENT;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
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
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.IncrementBuildSegmentsRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelSuggestionRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentFixRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.request.UpdateMultiPartitionValueRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggShardByColumnsResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnCheckResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.MergeSegmentCheckResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.ModelSuggestionResponse;
import io.kyligence.kap.rest.response.MultiPartitionValueResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.SyncContext;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NModelController extends NBasicController {
    public static final String MODEL_ID = "modelId";
    private static final String NEW_MODEL_NAME = "newModelNAME";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @ApiOperation(value = "getModels{Red}", tags = {
            "AI" }, notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
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
        status = formatStatus(status, ModelStatusToDisplayEnum.class);
        List<NDataModel> models = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            models.addAll(modelService.getModels(modelAlias, project, exactMatch, owner, status, sortBy, reverse,
                    modelAliasOrOwner, lastModifyFrom, lastModifyTo));
        } else {
            models.addAll(modelService.getRelateModels(project, table, modelAlias));
        }

        DataResult<List<NDataModel>> filterModels = DataResult.get(models, offset, limit);
        filterModels.setValue(modelService.addOldParams(project, filterModels.getValue()));
        filterModels.setValue(modelService.updateReponseAcl(filterModels.getValue(), project));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, filterModels, "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", tags = {
            "AI" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/disable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> offlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.offlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", tags = {
            "AI" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/enable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> onlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.onlineAllModelsInProject(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "createModel", tags = { "AI" })
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
    @ApiOperation(value = "batchSaveModels", tags = { "AI" }, notes = "Update URL: {project}; Update Param: project")
    @PostMapping(value = "/batch_save_models")
    @ResponseBody
    public EnvelopeResponse<String> batchSaveModels(@RequestParam("project") String project,
            @RequestBody List<ModelRequest> modelRequests) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        try {
            modelService.batchCreateModel(project, modelRequests, Lists.newArrayList());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }

    @ApiOperation(value = "suggestModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/suggest_model")
    @ResponseBody
    public EnvelopeResponse<ModelSuggestionResponse> suggestModel(@RequestBody SqlAccelerateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        AbstractContext proposeContext = modelService.suggestModel(request.getProject(), request.getSqls(),
                request.getReuseExistedModel(), true);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.buildModelSuggestionResponse(proposeContext), "");
    }

    @ApiOperation(value = "suggestModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/model_recommendation")
    @ResponseBody
    public EnvelopeResponse<String> approveSuggestModel(@RequestBody ModelSuggestionRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        try {
            request.getNewModels().forEach(req -> {
                req.setWithModelOnline(request.isWithModelOnline());
                req.setWithEmptySegment(request.isWithEmptySegment());
            });
            modelService.batchCreateModel(request.getProject(), request.getNewModels(), request.getReusedModels());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }

    /**
     * if exist same name model, then return true.
     */
    @ApiOperation(value = "validateModelAlias", tags = { "AI" }, notes = "")
    @PostMapping(value = "/validate_model")
    @ResponseBody
    public EnvelopeResponse<Boolean> validateModelAlias(@RequestBody ModelRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelRequest.getUuid());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, !modelService
                .checkModelAliasUniqueness(modelRequest.getUuid(), modelRequest.getAlias(), modelRequest.getProject()),
                "");
    }

    @ApiOperation(value = "checkIfCanAnsweredByExistedModel", tags = { "AI" }, notes = "")
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
    @ApiOperation(value = "listScd2Model", tags = { "AI" }, notes = "")
    @GetMapping(value = "name/scd2")
    @ResponseBody
    public EnvelopeResponse<List<String>> listScd2Model(@RequestParam("project") String project,
            @RequestParam(value = "non_offline", required = false, defaultValue = "true") boolean nonOffline) {
        checkProjectName(project);

        List<String> status = nonOffline ? modelService.getModelNonOffOnlineStatus()
                : Collections.singletonList(ModelStatusToDisplayEnum.OFFLINE.name());
        List<String> scd2ModelsOnline = modelService.getSCD2ModelsAliasByStatus(project, status);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, scd2ModelsOnline, "");
    }

    /**
     * list model that is multi partition
     */
    @ApiOperation(value = "listMultiPartitionModel", tags = { "AI" }, notes = "")
    @GetMapping(value = "/name/multi_partition")
    @ResponseBody
    public EnvelopeResponse<List<String>> listMultiPartitionModel(@RequestParam("project") String project,
            @RequestParam(value = "non_offline", required = false, defaultValue = "true") boolean nonOffline) {
        checkProjectName(project);
        List<String> onlineStatus = null;
        if (nonOffline) {
            onlineStatus = modelService.getModelNonOffOnlineStatus();
        }
        List<String> multiPartitionModels = modelService.getMultiPartitionModelsAlias(project, onlineStatus);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, multiPartitionModels, "");
    }

    @ApiOperation(value = "getLatestData", tags = { "AI" }, notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/data_range/latest_data")
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getLatestData(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        return getPartitionLatestData(project, modelId, null);
    }

    @ApiOperation(value = "getLatestData", tags = { "AI" }, notes = "Update URL: {model}")
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
            throw new KylinException(FAILED_DETECT_DATA_RANGE, MsgPicker.getMsg().getPUSHDOWN_DATARANGE_TIMEOUT());
        } catch (Exception e) {
            throw new KylinException(INVALID_RANGE, MsgPicker.getMsg().getPUSHDOWN_DATARANGE_ERROR());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getAggIndices", tags = {
            "AI" }, notes = "Update URL: model; Update Param: is_case_sensitive, page_offset, page_size, sort_by; Update Response: total_size")
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

    @ApiOperation(value = "updateAggIndicesShardColumns", tags = {
            "AI" }, notes = "Update URL: model;Update Param: model_id")
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

    @ApiOperation(value = "getAggIndicesShardColumns", tags = {
            "AI" }, notes = "Update URL: model; Update Response: model_id")
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
    @ApiOperation(value = "getTableIndices", tags = { "AI" }, notes = "Update URL: {model}")
    @GetMapping(value = "/{model:.+}/table_indices")
    @ResponseBody
    public EnvelopeResponse<IndicesResponse> getTableIndices(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getTableIndices(modelId, project), "");
    }

    @ApiOperation(value = "buildIndicesManually", tags = { "DW" }, notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/indices")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model") String modelId,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);

        modelService.validateCCType(modelId, request.getProject());

        val response = modelService.buildIndicesManually(modelId, request.getProject(), request.getPriority());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getModelJson", tags = { "AI" }, notes = "Update URL: {model}")
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

    @ApiOperation(value = "getModelSql", tags = { "AI" }, notes = "Update URL: {model}")
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

    @ApiOperation(value = "getAffectedModels", tags = { "AI" })
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

    @ApiOperation(value = "updateModelSemantic", tags = { "AI" })
    @PutMapping(value = "/semantic")
    @ResponseBody
    public EnvelopeResponse<String> updateSemantic(@RequestBody ModelRequest request) {
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
        } catch (Exception e) {
            throw new KylinException(FAILED_UPDATE_MODEL, MsgPicker.getMsg().getDEFAULT_MODEL_REASON(), e);
        }
    }

    @ApiOperation(value = "changePartition", tags = { "AI" })
    @PutMapping(value = "/{model:.+}/partition")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionSemantic(@PathVariable("model") String modelId,
            @RequestBody PartitionColumnRequest request) throws Exception {
        checkProjectName(request.getProject());
        validatePartitionDesc(request.getPartitionDesc());
        checkRequiredArg(MODEL_ID, modelId);
        try {
            modelService.updatePartitionColumn(request.getProject(), modelId, request.getPartitionDesc(),
                    request.getMultiPartitionDesc());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_UPDATE_MODEL, e);
        }
    }

    @ApiOperation(value = "updateModelName", tags = { "AI" }, notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/name")
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        String newAlias = modelRenameRequest.getNewModelName();
        if (!StringUtils.containsOnly(newAlias, ModelService.VALID_NAME_FOR_MODEL)) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MODEL_NAME(), newAlias));
        }

        modelService.renameDataModel(modelRenameRequest.getProject(), modelId, newAlias);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateModelStatus", tags = { "AI" }, notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/status")
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.updateDataModelStatus(modelId, modelRenameRequest.getProject(), modelRenameRequest.getStatus());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "unlinkModel", tags = { "AI" }, notes = "Update Body: model_id")
    @PutMapping(value = "/{model:.+}/management_type")
    @ResponseBody
    public EnvelopeResponse<String> unlinkModel(@PathVariable("model") String modelId,
            @RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.unlinkModel(modelId, unlinkModelRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel", tags = { "AI" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model") String model,
            @RequestParam("project") String project) {
        checkProjectName(project);
        modelService.dropModel(model, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getPurgeModelAffectedResponse", tags = { "AI" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/purge_effect")
    @ResponseBody
    public EnvelopeResponse<PurgeModelAffectedResponse> getPurgeModelAffectedResponse(
            @PathVariable(value = "model") String model, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, model);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.getPurgeModelAffectedResponse(project, model), "");
    }

    @ApiOperation(value = "cloneModel", tags = { "AI" }, notes = "Add URL: {model}; Update Param: new_model_name")
    @PostMapping(value = "/{model:.+}/clone")
    @ResponseBody
    public EnvelopeResponse<String> cloneModel(@PathVariable("model") String modelId,
            @RequestBody ModelCloneRequest request) {
        checkProjectName(request.getProject());
        String newModelName = request.getNewModelName();
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg(NEW_MODEL_NAME, newModelName);
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_NAME_FOR_MODEL)) {
            throw new KylinException(INVALID_MODEL_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getINVALID_MODEL_NAME(), newModelName));
        }
        modelService.cloneModel(modelId, request.getNewModelName(), request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "checkComputedColumns", tags = {
            "AI" }, notes = "Update Response: table_identity, table_alias, column_name, inner_expression, data_type")
    @PostMapping(value = "/computed_columns/check")
    @ResponseBody
    public EnvelopeResponse<ComputedColumnCheckResponse> checkComputedColumns(
            @RequestBody ComputedColumnCheckRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        modelRequest.getModelDesc().setProject(modelRequest.getProject());
        NDataModel modelDesc = modelService.convertToDataModel(modelRequest.getModelDesc());
        modelDesc.setSeekingCCAdvice(modelRequest.isSeekingExprAdvice());
        modelService.primaryCheck(modelDesc);
        ComputedColumnCheckResponse response = modelService.checkComputedColumn(modelDesc, modelRequest.getProject(),
                modelRequest.getCcInCheck());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getComputedColumnUsage", tags = { "AI" })
    @GetMapping(value = "/computed_columns/usage")
    @ResponseBody
    public EnvelopeResponse<ComputedColumnUsageResponse> getComputedColumnUsage(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
    }

    @Deprecated
    @ApiOperation(value = "updateModelDataCheckDesc", tags = { "AI" }, notes = "URL, front end Deprecated")
    @PutMapping(value = "/{model:.+}/data_check")
    @ResponseBody
    public EnvelopeResponse<String> updateModelDataCheckDesc(@PathVariable("model") String modelId,
            @RequestBody ModelCheckRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelDataCheckDesc(request.getProject(), modelId, request.getCheckOptions(),
                request.getFaultThreshold(), request.getFaultActions());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getModelConfig", tags = { "AI" }, notes = "Update Param: model_name, page_offset, page_size")
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

    @ApiOperation(value = "updateModelConfig", tags = { "AI" })
    @PutMapping(value = "/{model:.+}/config")
    @ResponseBody
    public EnvelopeResponse<String> updateModelConfig(@PathVariable("model") String modelId,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelId, request);
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

    @ApiOperation(value = "checkBeforeModelSave", tags = { "AI" })
    @PostMapping(value = "/model_save/check")
    @ResponseBody
    public EnvelopeResponse<ModelSaveCheckResponse> checkBeforeModelSave(@RequestBody ModelRequest modelRequest) {
        checkProjectName(modelRequest.getProject());
        ModelSaveCheckResponse response = modelService.checkBeforeModelSave(modelRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updateModelOwner", tags = { "AI" })
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
    @ApiOperation(value = "getSegments", tags = {
            "DW" }, notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
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

    @ApiOperation(value = "fixSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
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
                segmentsRequest.getSegmentHoles(), segmentsRequest.getIgnoredSnapshotTables());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "checkSegments", tags = { "DW" })
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

    @ApiOperation(value = "checkSegmentsIfDelete", tags = { "DW" })
    @GetMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkHolesIfSegDeleted(@PathVariable("model") String model,
            @RequestParam("project") String project, @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        val res = modelService.checkSegHoleIfSegDeleted(model, project, ids);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "deleteSegments", tags = { "DW" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model") String model,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        checkProjectName(project);

        if (purge) {
            modelService.purgeModelManually(model, project);
        } else {
            checkSegmentParms(ids, names);
            String[] idsDeleted = modelService.convertSegmentIdWithName(model, project, ids, names);
            if (ArrayUtils.isEmpty(idsDeleted)) {
                throw new KylinException(EMPTY_SEGMENT_ID, MsgPicker.getMsg().getSEGMENT_LIST_IS_EMPTY());
            }
            modelService.deleteSegmentById(model, project, idsDeleted, force);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        checkSegmentParms(request.getIds(), request.getNames());
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        String[] segIds = modelService.convertSegmentIdWithName(modelId, request.getProject(), request.getIds(),
                request.getNames());

        if (SegmentsRequest.SegmentsRequestType.REFRESH == request.getType()) {
            if (ArrayUtils.isEmpty(segIds)) {
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
            }
            jobInfos = modelService.refreshSegmentById(
                    new RefreshSegmentParams(request.getProject(), modelId, segIds, request.isRefreshAllIndexes())
                            .withIgnoredSnapshotTables(request.getIgnoredSnapshotTables())
                            .withPriority(request.getPriority()));
        } else {
            if (ArrayUtils.isEmpty(segIds) || segIds.length < 2) {
                throw new KylinException(FAILED_MERGE_SEGMENT,
                        MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
            }
            val jobInfo = modelService.mergeSegmentsManually(
                    new MergeSegmentParams(request.getProject(), modelId, segIds).withPriority(request.getPriority()));
            if (jobInfo != null) {
                jobInfos.add(jobInfo);
            }
        }
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segments/merge_check")
    @ResponseBody
    public EnvelopeResponse<MergeSegmentCheckResponse> checkMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        if (ArrayUtils.isEmpty(request.getIds()) || request.getIds().length < 2) {
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
        }
        Pair<Long, Long> merged = modelService
                .checkMergeSegments(new MergeSegmentParams(request.getProject(), modelId, request.getIds()));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                new MergeSegmentCheckResponse(merged.getFirst(), merged.getSecond()), "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
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
                buildSegmentsRequest.isBuildAllIndexes(), buildSegmentsRequest.getIgnoredSnapshotTables(),
                buildSegmentsRequest.getSubPartitionValues(), buildSegmentsRequest.getPriority());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/model_segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> incrementBuildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody IncrementBuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        String partitionColumnFormat = buildSegmentsRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());

        IncrementBuildSegmentParams inrcParams = new IncrementBuildSegmentParams(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.getPartitionDesc(), buildSegmentsRequest.getMultiPartitionDesc(),
                buildSegmentsRequest.getSegmentHoles(), buildSegmentsRequest.isBuildAllIndexes(),
                buildSegmentsRequest.getSubPartitionValues())
                        .withIgnoredSnapshotTables(buildSegmentsRequest.getIgnoredSnapshotTables())
                        .withPriority(buildSegmentsRequest.getPriority());

        JobInfoResponse response = modelService.incrementBuildSegmentsManually(inrcParams);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        JobInfoResponseWithFailure response = modelService.addIndexesToSegments(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getSegmentIds(), buildSegmentsRequest.getIndexIds(),
                buildSegmentsRequest.isParallelBuildBySegment(), buildSegmentsRequest.getPriority());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/all_indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addAllIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        JobInfoResponseWithFailure response = modelService.addIndexesToSegments(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getSegmentIds(), null, buildSegmentsRequest.isParallelBuildBySegment(),
                buildSegmentsRequest.getPriority());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes/deletion")
    @ResponseBody
    public EnvelopeResponse<String> deleteIndexesFromSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest deleteSegmentsRequest) {
        checkProjectName(deleteSegmentsRequest.getProject());
        modelService.removeIndexesFromSegments(deleteSegmentsRequest.getProject(), modelId,
                deleteSegmentsRequest.getSegmentIds(), deleteSegmentsRequest.getIndexIds());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "export model", tags = { "QE" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/export")
    @ResponseBody
    public void exportModel(@PathVariable("model") String modelId, @RequestParam(value = "project") String project,
            @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String serverHost,
            @RequestParam(value = "server_port", required = false) Integer serverPort, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        checkProjectName(project);

        String host = KylinConfig.getInstanceFromEnv().getModelExportHost();
        Integer port = KylinConfig.getInstanceFromEnv().getModelExportPort() == -1 ? null
                : KylinConfig.getInstanceFromEnv().getModelExportPort();

        host = host == null ? serverHost : host;
        port = port == null ? serverPort : port;

        host = host == null ? request.getServerName() : host;
        port = port == null ? request.getServerPort() : port;

        BISyncModel syncModel = modelService.exportModel(project, modelId, exportAs, element, host, port);

        String fileName = String.format(Locale.ROOT, "%s_%s_%s", project,
                modelService.getModelById(modelId, project).getAlias(),
                new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault(Locale.Category.FORMAT)).format(new Date()));
        switch (exportAs) {
        case TABLEAU_CONNECTOR_TDS:
        case TABLEAU_ODBC_TDS:
            response.setContentType("application/xml");
            response.setHeader("Content-Disposition",
                    String.format(Locale.ROOT, "attachment; filename=\"%s.tds\"", fileName));
            break;
        default:
            throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, "unrecognized export target");
        }
        syncModel.dump(response.getOutputStream());
        response.getOutputStream().flush();
        response.getOutputStream().close();
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsBuildRequest param) {
        checkProjectName(param.getProject());
        checkRequiredArg("segment_id", param.getSegmentId());
        checkRequiredArg("sub_partition_values", param.getSubPartitionValues());
        val response = modelService.buildSegmentPartitionByValue(param.getProject(), modelId, param.getSegmentId(),
                param.getSubPartitionValues(), param.isParallelBuildBySegment());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PutMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsRefreshRequest param) {
        checkProjectName(param.getProject());
        checkRequiredArg("segment_id", param.getSegmentId());
        val response = modelService.refreshSegmentPartition(param, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "deleteMultiPartition", tags = { "DW" })
    @DeleteMapping(value = "/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartition(@RequestParam("model") String modelId,
            @RequestParam("project") String project, @RequestParam("segment") String segment,
            @RequestParam(value = "ids") String[] ids) {
        checkProjectName(project);
        HashSet<Long> partitions = Sets.newHashSet();
        Arrays.stream(ids).forEach(id -> partitions.add(Long.parseLong(id)));
        modelService.deletePartitions(project, segment, modelId, partitions);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMultiPartitions", tags = { "DW" })
    @GetMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SegmentPartitionResponse>>> getMultiPartition(
            @PathVariable("model") String modelId, @RequestParam("project") String project,
            @RequestParam("segment_id") String segId,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset, //
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        val responseList = modelService.getSegmentPartitions(project, modelId, segId, status, sortBy, reverse);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(responseList, pageOffset, pageSize),
                "");
    }

    @ApiOperation(value = "updateMultiPartitionMapping", tags = { "QE" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model") String modelId,
            @RequestBody MultiPartitionMappingRequest mappingRequest) {
        checkProjectName(mappingRequest.getProject());
        modelService.updateMultiPartitionMapping(mappingRequest.getProject(), modelId, mappingRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<List<MultiPartitionValueResponse>> getMultiPartitionValues(
            @PathVariable("model") String modelId, @RequestParam("project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.getMultiPartitionValues(project, modelId),
                "");
    }

    @ApiOperation(value = "addMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> addMultiPartitionValues(@PathVariable("model") String modelId,
            @RequestBody UpdateMultiPartitionValueRequest request) {
        checkProjectName(request.getProject());
        modelService.addMultiPartitionValues(request.getProject(), modelId, request.getSubPartitionValues());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartitionValues(@PathVariable("model") String modelId,
            @RequestParam("project") String project, @RequestParam(value = "ids") Long[] ids) {
        checkProjectName(project);
        modelService.deletePartitions(project, null, modelId, Sets.newHashSet(ids));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

}
