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
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.DATETIME_FORMAT_PARSE_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_INVALID;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
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

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.request.AddSegmentRequest;
import io.kyligence.kap.rest.request.AggShardByColumnsRequest;
import io.kyligence.kap.rest.request.ComputedColumnCheckRequest;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;
import io.kyligence.kap.rest.request.MergeSegmentRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelSuggestionRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.ModelValidationRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.request.UpdateMultiPartitionValueRequest;
import io.kyligence.kap.rest.response.AffectedModelsResponse;
import io.kyligence.kap.rest.response.AggShardByColumnsResponse;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.ComputedColumnCheckResponse;
import io.kyligence.kap.rest.response.ComputedColumnUsageResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.InvalidIndexesResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.MultiPartitionValueResponse;
import io.kyligence.kap.rest.response.OpenRecApproveResponse;
import io.kyligence.kap.rest.response.OptRecResponse;
import io.kyligence.kap.rest.response.PurgeModelAffectedResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.service.FusionIndexService;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.SyncContext;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
@EnableDiscoveryClient
@EnableFeignClients
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
    private FusionModelService fusionModelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    private FusionIndexService fusionIndexService;

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
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> createModel(@RequestBody ModelRequest modelRequest) {
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
        String projectName = checkProjectName(project);
        ExistedDataRangeResponse response = modelService.getLatestDataRange(projectName, modelId, null);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getLatestData", tags = { "AI" }, notes = "Update URL: {model}")
    @PostMapping(value = "/{model:.+}/data_range/latest_data")
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getPartitionLatestData(
            @PathVariable(value = "model") String modelId, @RequestBody PartitionColumnRequest request) {
        String project = checkProjectName(request.getProject());
        ExistedDataRangeResponse response = modelService.getLatestDataRange(project, modelId,
                request.getPartitionDesc());
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
            throw new KylinRuntimeException("can not get model json " + e);
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
            throw new KylinRuntimeException("can not get model sql, " + e);
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
    }

    @ApiOperation(value = "detectInvalidIndexes", tags = { "AI" })
    @PostMapping(value = "/invalid_indexes")
    @ResponseBody
    public EnvelopeResponse<InvalidIndexesResponse> detectInvalidIndexes(@RequestBody ModelRequest request) {
        checkProjectName(request.getProject());
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
            throw new KylinException(MODEL_NAME_INVALID, newAlias);
        }

        fusionModelService.renameDataModel(modelRenameRequest.getProject(), modelId, newAlias);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateModelStatus", tags = { "AI" }, notes = "Update Body: model_id, new_model_name")
    @PutMapping(value = "/{model:.+}/status", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model") String modelId,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        String actualProject = checkProjectName(modelRenameRequest.getProject());
        modelRenameRequest.setProject(actualProject);
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
            throw new KylinException(MODEL_NAME_INVALID, newModelName);
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
                throw new KylinException(INVALID_PARTITION_COLUMN, MsgPicker.getMsg().getPartitionColumnNotExist());
            }
            if (!isSupportFormatsFormats(partitionDesc)) {
                throw new KylinException(DATETIME_FORMAT_PARSE_ERROR, partitionDesc.getPartitionDateFormat());
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
            @RequestParam(value = "server_port", required = false) Integer serverPort,
            @RequestParam(value = "dimensions", required = false) List<String> dimensions,
            @RequestParam(value = "measures", required = false) List<String> measures, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);

        String host = getHost(serverHost, request.getServerName());
        Integer port = getPort(serverPort, request.getServerPort());

        SyncContext syncContext = modelService.getSyncContext(projectName, modelId, exportAs, element, host, port);

        BISyncModel syncModel = AclPermissionUtil.isAdmin()
                ? modelService.exportTDSDimensionsAndMeasuresByAdmin(syncContext, dimensions, measures)
                : modelService.exportTDSDimensionsAndMeasuresByNormalUser(syncContext, dimensions, measures);

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

    // feign API for smart module

    @PostMapping(value = "/feign/batch_save_models")
    @ResponseBody
    public void batchCreateModel(@RequestBody ModelSuggestionRequest request) {
        modelService.batchCreateModel(request);
    }

    @PostMapping(value = "/feign/update_recommendations_count")
    @ResponseBody
    public void updateRecommendationsCount(@RequestParam("project") String project,
                                           @RequestParam("modelId") String modelId, @RequestParam("size") int size) {
        modelService.updateRecommendationsCount(project, modelId, size);
    }

    @PostMapping(value = "/feign/approve")
    @ResponseBody
    public OptRecResponse approve(@RequestParam("project") String project, @RequestBody OptRecRequest request) {
        return modelService.approve(project, request);
    }

    @PostMapping(value = "/feign/approve_all_rec_items")
    @ResponseBody
    public OpenRecApproveResponse.RecToIndexResponse approveAllRecItems(@RequestParam("project") String project,
                                                                        @RequestParam("modelId") String modelId, @RequestParam("modelAlias") String modelAlias,
                                                                        @RequestParam("recActionType") String recActionType) {
        return modelService.approveAllRecItems(project, modelId, modelAlias, recActionType);
    }

    @PostMapping(value = "/feign/save_new_models_and_indexes")
    @ResponseBody
    public void saveNewModelsAndIndexes(@RequestParam("project") String project,
                                        @RequestBody List<ModelRequest> newModels) {
        modelService.saveNewModelsAndIndexes(project, newModels);
    }

    @PostMapping(value = "/feign/save_rec_result")
    @ResponseBody
    public void saveRecResult(@RequestBody SuggestionResponse modelSuggestionResponse,
                              @RequestParam("project") String project) {
        modelService.saveRecResult(modelSuggestionResponse, project);
    }

    @PostMapping(value = "/feign/update_models")
    @ResponseBody
    public void updateModels(@RequestBody List<SuggestionResponse.ModelRecResponse> reusedModels,
                             @RequestParam("project") String project) {
        modelService.updateModels(reusedModels, project);
    }

    // feign API for data_loading module

    @PostMapping(value = "/feign/get_model_id_by_fuzzy_name")
    @ResponseBody
    public List<String> getModelIdsByFuzzyName(@RequestParam("fuzzyName") String fuzzyName,
            @RequestParam("project") String project) {
        return modelService.getModelNamesByFuzzyName(fuzzyName, project);
    }

    @PostMapping(value = "/feign/get_model_name_by_id")
    @ResponseBody
    public String getModelNameById(@RequestParam("modelId") String modelId, @RequestParam("project") String project) {
        return modelService.getModelNameById(modelId, project);
    }

    @PostMapping(value = "/feign/get_segment_by_range")
    @ResponseBody
    public Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end) {
        return modelService.getSegmentsByRange(modelId, project, start, end);
    }

    @PostMapping(value = "/feign/update_second_storage_model")
    @ResponseBody
    public String updateSecondStorageModel(@RequestParam("project") String project,
                                           @RequestParam("modelId") String modelId) {
        return modelService.updateSecondStorageModel(project, modelId);
    }

    @PostMapping(value = "/feign/update_data_model_semantic")
    @ResponseBody
    public void updateDataModelSemantic(@RequestParam("project") String project, @RequestBody ModelRequest request) {
        modelService.updateDataModelSemantic(project, request);
    }

    @PostMapping(value = "/feign/save_data_format_if_not_exist")
    @ResponseBody
    public void saveDateFormatIfNotExist(@RequestParam("project") String project,
                                         @RequestParam("modelId") String modelId, @RequestParam("format") String format) {
        modelService.saveDateFormatIfNotExist(project, modelId, format);
    }

    @PostMapping(value = "/feign/append_segment")
    @ResponseBody
    public NDataSegment appendSegment(@RequestBody AddSegmentRequest request) {
        return modelService.appendSegment(request);
    }

    @PostMapping(value = "/feign/refresh_segment")
    @ResponseBody
    public NDataSegment refreshSegment(@RequestParam("project") String project,
                                       @RequestParam("indexPlanUuid") String indexPlanUuid, @RequestParam("segmentId") String segmentId) {
        return modelService.refreshSegment(project, indexPlanUuid, segmentId);
    }

    @PostMapping(value = "/feign/append_partitions")
    @ResponseBody
    public NDataSegment appendPartitions(@RequestParam("project") String project, @RequestParam("dfIF") String dfId,
                                         @RequestParam("segId") String segId, @RequestBody List<String[]> partitionValues) {
        return modelService.appendPartitions(project, dfId, segId, partitionValues);
    }

    @PostMapping(value = "/feign/merge_segments")
    @ResponseBody
    NDataSegment mergeSegments(@RequestParam("project") String project,
                               @RequestBody MergeSegmentRequest mergeSegmentRequest) {
        return modelService.mergeSegments(project, mergeSegmentRequest);
    }

    @PostMapping(value = "/feign/delete_segment_by_id")
    @ResponseBody
    public void deleteSegmentById(@RequestParam("model") String model, @RequestParam("project") String project,
                                  @RequestBody String[] ids, @RequestParam("force") boolean force) {
        modelService.deleteSegmentById(model, project, ids, force);
    }

    @PostMapping(value = "/feign/remove_indexes_from_segments")
    @ResponseBody
    public void removeIndexesFromSegments(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
                                          @RequestParam("segmentIds") List<String> segmentIds, @RequestParam("indexIds") List<Long> indexIds) {
        modelService.removeIndexesFromSegments(project, modelId, segmentIds, indexIds);
    }

    @PostMapping(value = "/feign/update_index")
    @ResponseBody
    public void updateIndex(@RequestParam("project") String project, @RequestParam("epochId") long epochId,
                            @RequestParam("modelId") String modelId, @RequestBody Set<Long> toBeDeletedLayoutIds,
                            @RequestParam("deleteAuto") boolean deleteAuto, @RequestParam("deleteManual") boolean deleteManual) {
        modelService.updateIndex(project, epochId, modelId, toBeDeletedLayoutIds, deleteAuto, deleteManual);
    }

    @PostMapping(value = "/feign/update_dataflow")
    @ResponseBody
    public void updateDataflow(@RequestBody DataFlowUpdateRequest dataFlowUpdateRequest) {
        modelService.updateDataflow(dataFlowUpdateRequest);
    }

    @PostMapping(value = "/feign/update_dataflow_maxBucketId")
    @ResponseBody
    public void updateDataflow(@RequestParam("project") String project, @RequestParam("dfId") String dfId,
            @RequestParam("segmentId") String segmentId, @RequestParam("maxBucketId") long maxBucketIt) {
        modelService.updateDataflow(project, dfId, segmentId, maxBucketIt);
    }

    @PostMapping(value = "/feign/update_index_plan")
    @ResponseBody
    public void updateIndexPlan(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
                                @RequestBody IndexPlan indexplan, @RequestParam("action") String action) {
        modelService.updateIndexPlan(project, uuid, indexplan, action);
    }

    @PostMapping(value = "/feign/update_dataflow_status")
    @ResponseBody
    public void updateDataflowStatus(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
                                     @RequestParam("status") RealizationStatusEnum status) {
        modelService.updateDataflowStatus(project, uuid, status);
    }
}
