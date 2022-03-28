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
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
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
import io.kyligence.kap.rest.request.ModelValidationRequest;
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
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnCheckResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.InvalidIndexesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.MergeSegmentCheckResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.MultiPartitionValueResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.SegmentCheckResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.service.FusionIndexService;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.rest.service.params.IncrementBuildSegmentParams;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.SyncContext;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.log4j.Log4j;

@Log4j
@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NModelController extends NBasicController {
    public static final String MODEL_ID = "modelId";
    private static final String NEW_MODEL_NAME = "newModelNAME";
    //The front-end supports only the following formats
    private static final List<String> SUPPORTED_FORMATS = ImmutableList.of("ZZ", "DD", "D", "Do", "dddd", "ddd", "dd", //
            "d", "MMM", "MM", "M", "yyyy", "yy", "hh", "hh", "h", "HH", "H", "m", "mm", "ss", "s", "SSS", "SS", "S", //
            "A", "a");
    private static final Pattern QUOTE_PATTERN = Pattern.compile("\'(.*?)\'");

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    private FusionIndexService fusionIndexService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @ApiOperation(value = "getModels{Red}", tags = {
            "AI" }, notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(
            @RequestParam(value = "model_id", required = false) String modelId, //
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
            @RequestParam(value = "model_attributes", required = false) List<ModelAttributeEnum> modelAttributes,
            @RequestParam(value = "last_modify_from", required = false) Long lastModifyFrom,
            @RequestParam(value = "last_modify_to", required = false) Long lastModifyTo,
            @RequestParam(value = "only_normal_dim", required = false, defaultValue = "true") boolean onlyNormalDim) {
        checkProjectName(project);
        status = formatStatus(status, ModelStatusToDisplayEnum.class);

        DataResult<List<NDataModel>> filterModels = modelService.getModels(modelId, modelAlias, exactMatch, project,
                owner, status, table, offset, limit, sortBy, reverse, modelAliasOrOwner, modelAttributes,
                lastModifyFrom, lastModifyTo, onlyNormalDim);
        fusionModelService.setModelUpdateEnabled(filterModels);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, filterModels, "");
    }

    @ApiOperation(value = "validateNewModelAlias", tags = { "AI" })
    @PostMapping(value = "/name/validation")
    @ResponseBody
    public EnvelopeResponse<Boolean> validateNewModelAlias(@RequestBody ModelValidationRequest modelRequest) {
        checkRequiredArg("model_name", modelRequest.getModelName());
        checkProjectName(modelRequest.getProject());
        val exists = fusionModelService.modelExists(modelRequest.getModelName(), modelRequest.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, exists, "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", tags = {
            "AI" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/disable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> offlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.offlineAllModelsInProject(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "offlineAllModelsInProject", tags = {
            "AI" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/enable_all_models")
    @ResponseBody
    public EnvelopeResponse<String> onlineAllModelsInProject(@RequestParam("project") String project) {
        checkProjectName(project);
        modelService.onlineAllModelsInProject(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "createModel", tags = { "AI" })
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> createModel(@RequestBody ModelRequest modelRequest)
            throws Exception {
        checkProjectName(modelRequest.getProject());
        validatePartitionDesc(modelRequest.getPartitionDesc());
        String partitionDateFormat = modelRequest.getPartitionDesc() == null ? null
                : modelRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(modelRequest.getStart(), modelRequest.getEnd(), partitionDateFormat);
        try {
            NDataModel model = modelService.createModel(modelRequest.getProject(), modelRequest);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                    BuildBaseIndexResponse.from(modelService.getIndexPlan(model.getId(), model.getProject())), "");
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }

    @ApiOperation(value = "suggestModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/suggest_model")
    @ResponseBody
    public EnvelopeResponse<SuggestionResponse> suggestModel(@RequestBody SqlAccelerateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        AbstractContext proposeContext = modelSmartService.suggestModel(request.getProject(), request.getSqls(),
                request.getReuseExistedModel(), true);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                modelSmartService.buildModelSuggestionResponse(proposeContext), "");
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, !modelService
                .checkModelAliasUniqueness(modelRequest.getUuid(), modelRequest.getAlias(), modelRequest.getProject()),
                "");
    }

    @ApiOperation(value = "checkIfCanAnsweredByExistedModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/can_answered_by_existed_model")
    @ResponseBody
    public EnvelopeResponse<Boolean> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                modelSmartService.couldAnsweredByExistedModel(request.getProject(), request.getSqls()), "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, scd2ModelsOnline, "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, multiPartitionModels, "");
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

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
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
        fusionIndexService.updateShardByColumns(aggShardByColumnsRequest.getProject(), aggShardByColumnsRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getAggIndicesShardColumns", tags = {
            "AI" }, notes = "Update URL: model; Update Response: model_id")
    @GetMapping(value = "/{model:.+}/agg_indices/shard_columns")
    @ResponseBody
    public EnvelopeResponse<AggShardByColumnsResponse> getAggIndicesShardColumns(
            @PathVariable(value = "model") String modelId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, indexPlanService.getShardByColumns(project, modelId),
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelService.getTableIndices(modelId, project), "");
    }

    @ApiOperation(value = "buildIndicesManually", tags = { "DW" }, notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/indices")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model") String modelId,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        checkRequiredArg(MODEL_ID, modelId);

        modelService.validateCCType(modelId, request.getProject());

        val response = modelBuildService.buildIndicesManually(modelId, request.getProject(), request.getPriority(),
                request.getYarnQueue(), request.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, json, "");
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sql, "");
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("DROP_TABLE".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByDeletingTable(tableName, project);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, affectedModelResponse, "");
        } else if ("RELOAD_ROOT_FACT".equals(action)) {
            val affectedModelResponse = modelService.getAffectedModelsByToggleTableType(tableName, project);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, affectedModelResponse, "");
        } else {
            throw new IllegalArgumentException();
        }
    }

    private boolean isSupportFormatsFormats(PartitionDesc partitionDesc) {
        if (partitionDesc.partitionColumnIsTimestamp()) {
            return false;
        }
        String dateFormat = partitionDesc.getPartitionDateFormat();
        Matcher matcher = QUOTE_PATTERN.matcher(dateFormat);
        while (matcher.find()) {
            dateFormat = dateFormat.replaceAll(matcher.group(), "");
        }
        for (String frontEndFormat : SUPPORTED_FORMATS) {
            dateFormat = dateFormat.replaceAll(frontEndFormat, "");
        }
        int length = dateFormat.length();
        for (int i = 0; i < length; i++) {
            char c = dateFormat.charAt(i);
            if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                return false;
            }
        }
        return true;
    }

    @ApiOperation(value = "checkPartitionDesc", tags = { "AI" })
    @PostMapping(value = "/check_partition_desc")
    @ResponseBody
    public EnvelopeResponse<String> checkPartitionDesc(@RequestBody PartitionDesc partitionDesc) {
        try {
            validatePartitionDesc(partitionDesc);
            String partitionDateFormat = partitionDesc.getPartitionDateFormat();
            PartitionDesc.TimestampType timestampType = partitionDesc.getTimestampType();
            if (timestampType == null) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(partitionDateFormat,
                        Locale.getDefault(Locale.Category.FORMAT));
                String dateFormat = simpleDateFormat.format(new Date());
                return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, dateFormat, "");
            } else {
                long timestamp = System.currentTimeMillis() / timestampType.millisecondRatio;
                return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, timestamp + "", "");
            }
        } catch (Exception e) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_CUSTOMIZE_FORMAT());
        }
    }

    @ApiOperation(value = "detectInvalidIndexes", tags = { "AI" })
    @PostMapping(value = "/invalid_indexes")
    @ResponseBody
    public EnvelopeResponse<InvalidIndexesResponse> detectInvalidIndexes(@RequestBody ModelRequest request) {
        checkProjectName(request.getProject());
        request.setPartitionDesc(null);
        InvalidIndexesResponse response = modelService.detectInvalidIndexes(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updateModelSemantic", tags = { "AI" })
    @PutMapping(value = "/semantic")
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> updateSemantic(@RequestBody ModelRequest request) {
        checkProjectName(request.getProject());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(request.getProject(), request.getId());
        validateDataRange(request.getStart(), request.getEnd(), partitionColumnFormat);
        validatePartitionDesc(request.getPartitionDesc());
        checkRequiredArg(MODEL_ID, request.getUuid());
        try {
            BuildBaseIndexResponse response = BuildBaseIndexResponse.EMPTY;
            if (request.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                modelService.repairBrokenModel(request.getProject(), request);
            } else {
                response = fusionModelService.updateDataModelSemantic(request.getProject(), request);
            }
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
        } catch (LookupTableException e) {
            log.error("Update model failed", e);
            throw new KylinException(FAILED_UPDATE_MODEL, e);
        } catch (Exception e) {
            log.error("Update model failed", e);
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(FAILED_UPDATE_MODEL, root.getMessage());
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
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            log.error("Change partition failed", e);
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

        fusionModelService.renameDataModel(modelRenameRequest.getProject(), modelId, newAlias);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateModelStatus", tags = { "AI" }, notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/status")
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkProjectName(modelRenameRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.updateDataModelStatus(modelId, modelRenameRequest.getProject(), modelRenameRequest.getStatus());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "unlinkModel", tags = { "AI" }, notes = "Update Body: model_id")
    @PutMapping(value = "/{model:.+}/management_type")
    @ResponseBody
    public EnvelopeResponse<String> unlinkModel(@PathVariable("model") String modelId,
            @RequestBody UnlinkModelRequest unlinkModelRequest) {
        checkProjectName(unlinkModelRequest.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        modelService.unlinkModel(modelId, unlinkModelRequest.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel", tags = { "AI" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model") String model,
            @RequestParam("project") String project) {
        checkProjectName(project);
        fusionModelService.dropModel(model, project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getPurgeModelAffectedResponse", tags = { "AI" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/purge_effect")
    @ResponseBody
    public EnvelopeResponse<PurgeModelAffectedResponse> getPurgeModelAffectedResponse(
            @PathVariable(value = "model") String model, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, model);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getComputedColumnUsage", tags = { "AI" })
    @GetMapping(value = "/computed_columns/usage")
    @ResponseBody
    public EnvelopeResponse<ComputedColumnUsageResponse> getComputedColumnUsage(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelService.getComputedColumnUsages(project), "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(modelConfigs, offset, limit), "");
    }

    @ApiOperation(value = "updateModelConfig", tags = { "AI" })
    @PutMapping(value = "/{model:.+}/config")
    @ResponseBody
    public EnvelopeResponse<String> updateModelConfig(@PathVariable("model") String modelId,
            @RequestBody ModelConfigRequest request) {
        checkProjectName(request.getProject());
        modelService.updateModelConfig(request.getProject(), modelId, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    public void validatePartitionDesc(PartitionDesc partitionDesc) {
        if (partitionDesc != null) {
            if (partitionDesc.isEmpty()) {
                throw new KylinException(INVALID_PARTITION_COLUMN, MsgPicker.getMsg().getPARTITION_COLUMN_NOT_EXIST());
            }
            if (!isSupportFormatsFormats(partitionDesc)) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_CUSTOMIZE_FORMAT());
            }
            if (partitionDesc.getPartitionDateFormat() != null && !partitionDesc.partitionColumnIsTimestamp()) {
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updateModelOwner", tags = { "AI" })
    @PutMapping(value = "/{model:.+}/owner")
    @ResponseBody
    public EnvelopeResponse<String> updateModelOwner(@PathVariable("model") String modelId,
            @RequestBody OwnerChangeRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("owner", request.getOwner());
        fusionModelService.updateModelOwner(request.getProject(), modelId, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /* Segments */
    @ApiOperation(value = "getSegments", tags = {
            "DW" }, notes = "Update Param: page_offset, page_size, sort_by; Update Response: total_size")
    @GetMapping(value = "/{dataflow:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "dataflow") String dataflowId, //
            @RequestParam(value = "project") String project,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "0") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "with_indexes", required = false) List<Long> withAllIndexes,
            @RequestParam(value = "without_indexes", required = false) List<Long> withoutAnyIndexes,
            @RequestParam(value = "all_to_complement", required = false, defaultValue = "false") Boolean allToComplement,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean reverse) {
        checkProjectName(project);
        validateRange(start, end);
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(dataflowId, project, start, end, status,
                withAllIndexes, withoutAnyIndexes, allToComplement, sortBy, reverse);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(segments, offset, limit), "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "checkSegmentsIfDelete", tags = { "DW" })
    @GetMapping(value = "/{model:.+}/segment/validation")
    @ResponseBody
    public EnvelopeResponse<SegmentCheckResponse> checkHolesIfSegDeleted(@PathVariable("model") String model,
            @RequestParam("project") String project, @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        val res = modelService.checkSegHoleIfSegDeleted(model, project, ids);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "deleteSegments", tags = { "DW" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{dataflow:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("dataflow") String dataflowId,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        checkProjectName(project);

        if (purge) {
            modelService.purgeModelManually(dataflowId, project);
        } else {
            checkSegmentParms(ids, names);
            String[] idsDeleted = modelService.convertSegmentIdWithName(dataflowId, project, ids, names);
            if (ArrayUtils.isEmpty(idsDeleted)) {
                throw new KylinException(EMPTY_SEGMENT_ID, MsgPicker.getMsg().getSEGMENT_LIST_IS_EMPTY());
            }
            modelService.deleteSegmentById(dataflowId, project, idsDeleted, force);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        checkSegmentParms(request.getIds(), request.getNames());
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        String[] segIds = modelService.convertSegmentIdWithName(modelId, request.getProject(), request.getIds(),
                request.getNames());

        if (SegmentsRequest.SegmentsRequestType.REFRESH == request.getType()) {
            if (ArrayUtils.isEmpty(segIds)) {
                throw new KylinException(FAILED_REFRESH_SEGMENT, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
            }
            jobInfos = modelBuildService.refreshSegmentById(
                    new RefreshSegmentParams(request.getProject(), modelId, segIds, request.isRefreshAllIndexes())
                            .withIgnoredSnapshotTables(request.getIgnoredSnapshotTables())
                            .withPriority(request.getPriority()).withPartialBuild(request.isPartialBuild())
                            .withBatchIndexIds(request.getBatchIndexIds()).withYarnQueue(request.getYarnQueue())
                            .withTag(request.getTag()));
        } else {
            if (ArrayUtils.isEmpty(segIds) || segIds.length < 2) {
                throw new KylinException(FAILED_MERGE_SEGMENT,
                        MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
            }
            val jobInfo = modelBuildService.mergeSegmentsManually(
                    new MergeSegmentParams(request.getProject(), modelId, segIds).withPriority(request.getPriority())
                            .withYarnQueue(request.getYarnQueue()).withTag(request.getTag()));
            if (jobInfo != null) {
                jobInfos.add(jobInfo);
            }
        }
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segments/merge_check")
    @ResponseBody
    public EnvelopeResponse<MergeSegmentCheckResponse> checkMergeSegments(@PathVariable("model") String modelId,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        checkParamLength("tag", request.getTag(), 1024);
        if (ArrayUtils.isEmpty(request.getIds()) || request.getIds().length < 2) {
            throw new KylinException(FAILED_MERGE_SEGMENT, MsgPicker.getMsg().getINVALID_MERGE_SEGMENT_BY_TOO_LESS());
        }
        Pair<Long, Long> merged = modelService
                .checkMergeSegments(new MergeSegmentParams(request.getProject(), modelId, request.getIds()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new MergeSegmentCheckResponse(merged.getFirst(), merged.getSecond()), "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(buildSegmentsRequest.getProject(),
                modelId);
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());
        JobInfoResponse response = modelBuildService.buildSegmentsManually(buildSegmentsRequest.getProject(), modelId,
                buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.isBuildAllIndexes(), buildSegmentsRequest.getIgnoredSnapshotTables(),
                buildSegmentsRequest.getSubPartitionValues(), buildSegmentsRequest.getPriority(),
                buildSegmentsRequest.isBuildAllSubPartitions(), buildSegmentsRequest.getBatchIndexIds(),
                buildSegmentsRequest.isPartialBuild(), buildSegmentsRequest.getYarnQueue(),
                buildSegmentsRequest.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/model_segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> incrementBuildSegmentsManually(@PathVariable("model") String modelId,
            @RequestBody IncrementBuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        String partitionColumnFormat = buildSegmentsRequest.getPartitionDesc().getPartitionDateFormat();
        validateDataRange(buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(), partitionColumnFormat);
        modelService.validateCCType(modelId, buildSegmentsRequest.getProject());

        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getStart(), buildSegmentsRequest.getEnd(),
                buildSegmentsRequest.getPartitionDesc(), buildSegmentsRequest.getMultiPartitionDesc(),
                buildSegmentsRequest.getSegmentHoles(), buildSegmentsRequest.isBuildAllIndexes(),
                buildSegmentsRequest.getSubPartitionValues())
                        .withIgnoredSnapshotTables(buildSegmentsRequest.getIgnoredSnapshotTables())
                        .withPriority(buildSegmentsRequest.getPriority())
                        .withBuildAllSubPartitions(buildSegmentsRequest.isBuildAllSubPartitions())
                        .withYarnQueue(buildSegmentsRequest.getYarnQueue()).withTag(buildSegmentsRequest.getTag());

        JobInfoResponse response = fusionModelService.incrementBuildSegmentsManually(incrParams);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        val response = fusionModelService.addIndexesToSegments(modelId, buildSegmentsRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/all_indexes")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> addAllIndexesToSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest buildSegmentsRequest) {
        checkProjectName(buildSegmentsRequest.getProject());
        checkParamLength("tag", buildSegmentsRequest.getTag(), 1024);
        JobInfoResponseWithFailure response = modelBuildService.addIndexesToSegments(buildSegmentsRequest.getProject(),
                modelId, buildSegmentsRequest.getSegmentIds(), null, buildSegmentsRequest.isParallelBuildBySegment(),
                buildSegmentsRequest.getPriority());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/model_segments/indexes/deletion")
    @ResponseBody
    public EnvelopeResponse<String> deleteIndexesFromSegments(@PathVariable("model") String modelId,
            @RequestBody IndexesToSegmentsRequest deleteSegmentsRequest) {
        checkProjectName(deleteSegmentsRequest.getProject());
        modelService.removeIndexesFromSegments(deleteSegmentsRequest.getProject(), modelId,
                deleteSegmentsRequest.getSegmentIds(), deleteSegmentsRequest.getIndexIds());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        String projectName = checkProjectName(project);

        String host = getHost(serverHost, request.getServerName());
        Integer port = getPort(serverPort, request.getServerPort());

        BISyncModel syncModel = modelService.exportModel(projectName, modelId, exportAs, element, host, port);

        dumpSyncModel(modelId, exportAs, projectName, syncModel, response);
    }

    @ApiOperation(value = "biExport", tags = { "QE" })
    @GetMapping(value = "/bi_export")
    @ResponseBody
    public void biExport(@RequestParam("model") String modelId, @RequestParam(value = "project") String project,
            @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String serverHost,
            @RequestParam(value = "server_port", required = false) Integer serverPort, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);

        String host = getHost(serverHost, request.getServerName());
        Integer port = getPort(serverPort, request.getServerPort());

        BISyncModel syncModel = AclPermissionUtil.isAdmin()
                ? modelService.exportModel(projectName, modelId, exportAs, element, host, port)
                : modelService.biExportCustomModel(projectName, modelId, exportAs, element, host, port);

        dumpSyncModel(modelId, exportAs, projectName, syncModel, response);
    }

    private void dumpSyncModel(String modelId, SyncContext.BI exportAs, String projectName, BISyncModel syncModel,
            HttpServletResponse response) throws IOException {
        NDataModelManager manager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = manager.getDataModelDesc(modelId);
        String alias = dataModel.getAlias();
        String fileName = String.format(Locale.ROOT, "%s_%s_%s", projectName, alias,
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

    private String getHost(String serverHost, String serverName) {
        String host = KylinConfig.getInstanceFromEnv().getModelExportHost();
        host = Optional.ofNullable(Optional.ofNullable(host).orElse(serverHost)).orElse(serverName);
        return host;
    }

    private Integer getPort(Integer serverPort, Integer requestServerPort) {
        Integer port = KylinConfig.getInstanceFromEnv().getModelExportPort() == -1 ? null
                : KylinConfig.getInstanceFromEnv().getModelExportPort();
        port = Optional.ofNullable(Optional.ofNullable(port).orElse(serverPort)).orElse(requestServerPort);
        return port;
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsBuildRequest param) {
        checkProjectName(param.getProject());
        checkParamLength("tag", param.getTag(), 1024);
        checkRequiredArg("segment_id", param.getSegmentId());
        checkRequiredArg("sub_partition_values", param.getSubPartitionValues());
        val response = modelBuildService.buildSegmentPartitionByValue(param.getProject(), modelId, param.getSegmentId(),
                param.getSubPartitionValues(), param.isParallelBuildBySegment(), param.isBuildAllSubPartitions(),
                param.getPriority(), param.getYarnQueue(), param.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildMultiPartition", tags = { "DW" })
    @PutMapping(value = "/{model:.+}/model_segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model") String modelId,
            @RequestBody PartitionsRefreshRequest param) {
        checkProjectName(param.getProject());
        checkParamLength("tag", param.getTag(), 1024);
        checkRequiredArg("segment_id", param.getSegmentId());
        val response = modelBuildService.refreshSegmentPartition(param, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(responseList, pageOffset, pageSize),
                "");
    }

    @ApiOperation(value = "updateMultiPartitionMapping", tags = { "QE" }, notes = "Add URL: {model}")
    @PutMapping(value = "/{model:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model") String modelId,
            @RequestBody MultiPartitionMappingRequest mappingRequest) {
        checkProjectName(mappingRequest.getProject());
        modelService.updateMultiPartitionMapping(mappingRequest.getProject(), modelId, mappingRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<List<MultiPartitionValueResponse>> getMultiPartitionValues(
            @PathVariable("model") String modelId, @RequestParam("project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                modelService.getMultiPartitionValues(project, modelId), "");
    }

    @ApiOperation(value = "addMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> addMultiPartitionValues(@PathVariable("model") String modelId,
            @RequestBody UpdateMultiPartitionValueRequest request) {
        checkProjectName(request.getProject());
        modelService.addMultiPartitionValues(request.getProject(), modelId, request.getSubPartitionValues());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteMultiPartitionValues", tags = { "DW" }, notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartitionValues(@PathVariable("model") String modelId,
            @RequestParam("project") String project, @RequestParam(value = "ids") Long[] ids) {
        checkProjectName(project);
        modelService.deletePartitions(project, null, modelId, Sets.newHashSet(ids));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

}
