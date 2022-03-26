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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SQL_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.constant.ModelAttributeEnum;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.CheckSegmentRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OpenBatchApproveRecItemsRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UpdateMultiPartitionValueRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.OpenAccSqlResponse;
import io.kyligence.kap.rest.response.OpenGetIndexResponse;
import io.kyligence.kap.rest.response.OpenGetIndexResponse.IndexDetail;
import io.kyligence.kap.rest.response.OpenOptRecLayoutsResponse;
import io.kyligence.kap.rest.response.OpenRecApproveResponse;
import io.kyligence.kap.rest.response.OpenRecApproveResponse.RecToIndexResponse;
import io.kyligence.kap.rest.response.OpenSuggestionResponse;
import io.kyligence.kap.rest.response.OpenValidationResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.kyligence.kap.rest.service.FusionIndexService;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.kyligence.kap.tool.bisync.SyncContext;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelController extends NBasicController {

    private static final String LAST_MODIFY = "last_modified";
    private static final String USAGE = "usage";
    private static final String DATA_SIZE = "data_size";
    private static final Set<String> INDEX_SORT_BY_SET = ImmutableSet.of(USAGE, LAST_MODIFY, DATA_SIZE);
    private static final Set<String> INDEX_SOURCE_SET = Arrays.stream(IndexEntity.Source.values()).map(Enum::name)
            .collect(Collectors.toSet());
    private static final Set<String> INDEX_STATUS_SET = Arrays.stream(IndexEntity.Status.values()).map(Enum::name)
            .collect(Collectors.toSet());

    @Autowired
    private NModelController modelController;

    @Autowired
    private FusionIndexService fusionIndexService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @ApiOperation(value = "getModels", tags = { "AI" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_id", required = false) String modelId, //
            @RequestParam(value = "model_name", required = false) String modelAlias, //
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "owner", required = false) String owner, //
            @RequestParam(value = "status", required = false) List<String> status, //
            @RequestParam(value = "table", required = false) String table, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "model_alias_or_owner", required = false) String modelAliasOrOwner,
            @RequestParam(value = "last_modify_from", required = false) Long lastModifyFrom,
            @RequestParam(value = "last_modify_to", required = false) Long lastModifyTo,
            @RequestParam(value = "only_normal_dim", required = false, defaultValue = "true") boolean onlyNormalDim) {
        String projectName = checkProjectName(project);
        return modelController.getModels(modelId, modelAlias, exactMatch, projectName, owner, status, table, offset,
                limit, sortBy, reverse, modelAliasOrOwner, Arrays.asList(ModelAttributeEnum.BATCH), lastModifyFrom,
                lastModifyTo, onlyNormalDim);
    }

    @ApiOperation(value = "getIndexes", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<OpenGetIndexResponse> getIndexes(@RequestParam(value = "project") String project,
            @PathVariable(value = "model_name") String modelAlias,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sources", required = false) List<String> sources,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "key", required = false, defaultValue = "") String key,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "batch_index_ids", required = false) List<Long> batchIndexIds) {
        String projectName = checkProjectName(project);
        NDataModel model = getModel(modelAlias, projectName);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);
        List<IndexEntity.Status> statuses = checkIndexStatus(status);
        String modifiedSortBy = checkIndexSortBy(sortBy);
        List<IndexEntity.Source> modifiedSources = checkSources(sources);
        List<IndexResponse> indexes = fusionIndexService.getIndexesWithRelatedTables(projectName, model.getUuid(), key,
                statuses, modifiedSortBy, reverse, modifiedSources, batchIndexIds);
        List<IndexResponse> listDataResult = DataResult.get(indexes, offset, limit).getValue();

        OpenGetIndexResponse response = new OpenGetIndexResponse();
        response.setModelId(model.getUuid());
        response.setModelAlias(model.getAlias());
        response.setProject(projectName);
        response.setOwner(model.getOwner());
        response.setLimit(limit);
        response.setOffset(offset);
        response.setTotalSize(indexes.size());
        List<IndexDetail> detailList = Lists.newArrayList();
        listDataResult.forEach(indexResponse -> detailList.add(IndexDetail.newIndexDetail(indexResponse)));
        response.setIndexDetailList(detailList);
        if (CollectionUtils.isNotEmpty(batchIndexIds)) {
            Set<Long> batchIndexIdsSet = indexes.stream() //
                    .filter(index -> index.getIndexRange() == null || index.getIndexRange() == IndexEntity.Range.BATCH) //
                    .map(IndexResponse::getId).collect(Collectors.toSet());

            List<Long> absentBatchIndexIds = batchIndexIds.stream() //
                    .filter(id -> !batchIndexIdsSet.contains(id)) //
                    .collect(Collectors.toList());
            response.setAbsentBatchIndexIds(absentBatchIndexIds);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    static List<IndexEntity.Status> checkIndexStatus(List<String> statusList) {
        if (statusList == null || statusList.isEmpty()) {
            return Lists.newArrayList();
        }
        List<IndexEntity.Status> statuses = Lists.newArrayList();
        statusList.forEach(status -> {
            if (status != null) {
                String s = status.toUpperCase(Locale.ROOT);
                if (INDEX_STATUS_SET.contains(s)) {
                    statuses.add(IndexEntity.Status.valueOf(s));
                } else {
                    throw new KylinException(ServerErrorCode.INVALID_INDEX_STATUS_TYPE,
                            MsgPicker.getMsg().getINDEX_STATUS_TYPE_ERROR());
                }
            }
        });
        return statuses;
    }

    static List<IndexEntity.Source> checkSources(List<String> sources) {
        if (sources == null || sources.isEmpty()) {
            return Lists.newArrayList();
        }
        List<IndexEntity.Source> sourceList = Lists.newArrayList();
        sources.forEach(source -> {
            if (source != null) {
                String s = source.toUpperCase(Locale.ROOT);
                if (INDEX_SOURCE_SET.contains(s)) {
                    sourceList.add(IndexEntity.Source.valueOf(s));
                } else {
                    throw new KylinException(ServerErrorCode.INVALID_INDEX_SOURCE_TYPE,
                            MsgPicker.getMsg().getINDEX_SOURCE_TYPE_ERROR());
                }
            }
        });
        return sourceList;
    }

    static String checkIndexSortBy(String sortBy) {
        if (sortBy == null) {
            return LAST_MODIFY;
        }
        sortBy = sortBy.toLowerCase(Locale.ROOT).trim();
        if (sortBy.length() == 0) {
            return LAST_MODIFY;
        }
        if (INDEX_SORT_BY_SET.contains(sortBy)) {
            return sortBy;
        }
        throw new KylinException(ServerErrorCode.INVALID_INDEX_SORT_BY_FIELD,
                MsgPicker.getMsg().getINDEX_SORT_BY_ERROR());
    }

    @VisibleForTesting
    public NDataModel getModel(String modelAlias, String project) {
        NDataModel model = modelService.getDataModelManager(project).listAllModels().stream() //
                .filter(dataModel -> dataModel.getUuid().equals(modelAlias) //
                        || dataModel.getAlias().equalsIgnoreCase(modelAlias))
                .findFirst().orElse(null);

        if (model == null) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        if (model.isBroken()) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBROKEN_MODEL_OPERATION_DENIED(), modelAlias));
        }
        return model;
    }

    @ApiOperation(value = "getSegments", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean reverse) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getUuid();
        return modelController.getSegments(modelId, projectName, status, offset, limit, start, end, null, null, false,
                sortBy, reverse);
    }

    @ApiOperation(value = "getMultiPartitions", tags = { "DW" })
    @GetMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SegmentPartitionResponse>>> getMultiPartitions(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam("segment_id") String segId,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset, //
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return modelController.getMultiPartition(modelId, projectName, segId, status, pageOffset, pageSize, sortBy,
                reverse);
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        String projectName = checkProjectName(buildSegmentsRequest.getProject());
        buildSegmentsRequest.setProject(projectName);
        validatePriority(buildSegmentsRequest.getPriority());
        String modelId = getModel(modelAlias, buildSegmentsRequest.getProject()).getUuid();
        return modelController.buildSegmentsManually(modelId, buildSegmentsRequest);
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model_name") String modelAlias,
            @RequestBody SegmentsRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getUuid();
        return modelController.refreshOrMergeSegments(modelId, request);
    }

    @ApiOperation(value = "deleteSegments", tags = { "DW" })
    @DeleteMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        String projectName = checkProjectName(project);
        if (purge) {
            ids = new String[0];
        }
        String modelId = getModel(modelAlias, projectName).getUuid();
        return modelController.deleteSegments(modelId, projectName, purge, force, ids, names);
    }

    @ApiOperation(value = "completeSegments", tags = { "DW" })
    @PostMapping(value = "/{model_name}/segments/completion")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> completeSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "parallel", required = false, defaultValue = "false") boolean parallel,
            @RequestParam(value = "ids", required = false) String[] ids,
            @RequestParam(value = "names", required = false) String[] names,
            @RequestParam(value = "batch_index_ids", required = false) List<Long> batchIndexIds,
            @RequestParam(value = "partial_build", required = false, defaultValue = "false") boolean partialBuild,
            @RequestParam(value = "priority", required = false, defaultValue = "3") Integer priority,
            @RequestParam(value = "yarn_queue", required = false) String yarnQueue,
            @RequestParam(value = "tag", required = false) Object tag) {
        String projectName = checkProjectName(project);
        checkSegmentParms(ids, names);
        String modelId = getModel(modelAlias, projectName).getUuid();
        Pair<String, String[]> pair = fusionModelService.convertSegmentIdWithName(modelId, projectName, ids, names);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(projectName);
        req.setParallelBuildBySegment(parallel);
        req.setSegmentIds(Lists.newArrayList(pair.getSecond()));
        req.setPartialBuild(partialBuild);
        req.setIndexIds(batchIndexIds);
        req.setPriority(priority);
        req.setYarnQueue(yarnQueue);
        req.setTag(tag);
        return modelController.addIndexesToSegments(pair.getFirst(), req);
    }

    @ApiOperation(value = "getModelDesc", tags = { "AI" })
    @GetMapping(value = "/{project}/{model}/model_desc")
    @ResponseBody
    public EnvelopeResponse<NModelDescResponse> getModelDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias) {
        String projectName = checkProjectName(project);
        val dataModel = getModel(modelAlias, projectName);
        if (dataModel.isStreaming()) {
            throw new KylinException(UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getSTREAMING_OPERATION_NOT_SUPPORT());
        }
        NModelDescResponse result = modelService.getModelDesc(dataModel.getAlias(), projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "update partition for single-partition model and forward compatible", tags = { "DW" })
    @PutMapping(value = "/{project}/{model}/partition_desc")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias,
            @RequestBody ModelParatitionDescRequest modelParatitionDescRequest) {
        String projectName = checkProjectName(project);
        String partitionDateFormat = null;
        if (modelParatitionDescRequest.getPartitionDesc() != null) {
            checkRequiredArg("partition_date_column",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateColumn());
            checkRequiredArg("partition_date_format",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateFormat());
            partitionDateFormat = modelParatitionDescRequest.getPartitionDesc().getPartitionDateFormat();
        }
        validateDataRange(modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd(),
                partitionDateFormat);
        val dataModel = getModel(modelAlias, projectName);
        modelService.updateDataModelParatitionDesc(projectName, dataModel.getAlias(), modelParatitionDescRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "couldAnsweredByExistedModel", tags = { "AI" })
    @PostMapping(value = "/validation")
    @ResponseBody
    public EnvelopeResponse<List<String>> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        AbstractContext proposeContext = modelSmartService.probeRecommendation(request.getProject(), request.getSqls());
        List<NDataModel> models = proposeContext.getProposedModels();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                models.stream().map(NDataModel::getAlias).collect(Collectors.toList()), "");
    }

    @VisibleForTesting
    public OpenValidationResponse batchSqlValidate(String project, List<String> sqls) {
        Set<String> normalSqls = Sets.newHashSet();
        Set<String> errorSqls = Sets.newHashSet();
        Set<OpenValidationResponse.ErrorSqlDetail> errorSqlDetailSet = Sets.newHashSet();

        Map<String, SQLValidateResult> validatedSqls = favoriteRuleService.batchSqlValidate(sqls, project);
        validatedSqls.forEach((sql, validateResult) -> {
            if (validateResult.isCapable()) {
                normalSqls.add(sql);
            } else {
                errorSqls.add(sql);
                errorSqlDetailSet.add(new OpenValidationResponse.ErrorSqlDetail(sql, validateResult.getSqlAdvices()));
            }
        });

        AbstractContext proposeContext = modelSmartService.probeRecommendation(project, Lists.newArrayList(normalSqls));
        Map<String, NDataModel> uuidToModelMap = proposeContext.getRelatedModels().stream()
                .collect(Collectors.toMap(NDataModel::getUuid, Function.identity()));
        Map<String, Set<String>> answeredModelAlias = Maps.newHashMap();
        proposeContext.getAccelerateInfoMap().forEach((sql, accelerationInfo) -> {
            answeredModelAlias.putIfAbsent(sql, Sets.newHashSet());
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = accelerationInfo.getRelatedLayouts();
            if (CollectionUtils.isNotEmpty(relatedLayouts)) {
                relatedLayouts.forEach(info -> {
                    String alias = uuidToModelMap.get(info.getModelId()).getAlias();
                    answeredModelAlias.get(sql).add(alias);
                });
            }
        });
        Map<String, List<String>> validSqlToModels = Maps.newHashMap();
        answeredModelAlias.forEach((sql, aliasSet) -> validSqlToModels.put(sql, Lists.newArrayList(aliasSet)));
        return new OpenValidationResponse(validSqlToModels, Lists.newArrayList(errorSqls),
                Lists.newArrayList(errorSqlDetailSet));
    }

    @ApiOperation(value = "answeredByExistedModel", tags = { "AI" })
    @PostMapping(value = "/model_validation")
    @ResponseBody
    public EnvelopeResponse<OpenValidationResponse> answeredByExistedModel(@RequestBody FavoriteRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        aclEvaluate.checkProjectWritePermission(request.getProject());
        checkNotEmpty(request.getSqls());
        OpenValidationResponse response = batchSqlValidate(request.getProject(), request.getSqls());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildIndicesManually", tags = { "AI" })
    @PostMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildIndexRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.buildIndicesManually(modelId, request);
    }

    @ApiOperation(value = "getRecommendations", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<OpenOptRecLayoutsResponse> getRecommendations(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "recActionType", required = false, defaultValue = "all") String recActionType) {
        String projectName = checkProjectName(project);
        checkProjectNotSemiAuto(projectName);
        aclEvaluate.checkProjectWritePermission(projectName);
        String modelId = getModel(modelAlias, projectName).getId();
        checkRequiredArg(NModelController.MODEL_ID, modelId);
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(projectName, modelId, recActionType);
        //open api not add recDetailResponse
        response.getLayouts().forEach(optRecLayoutResponse -> optRecLayoutResponse.setRecDetailResponse(null));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new OpenOptRecLayoutsResponse(projectName, modelId, response), "");
    }

    @ApiOperation(value = "batchApproveRecommendations", tags = { "AI" })
    @PutMapping(value = "/recommendations/batch")
    @ResponseBody
    public EnvelopeResponse<OpenRecApproveResponse> batchApproveRecommendations(
            @RequestBody OpenBatchApproveRecItemsRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkProjectNotSemiAuto(projectName);
        boolean filterByModels = request.isFilterByModes();
        if (request.getRecActionType() == null || StringUtils.isEmpty(request.getRecActionType().trim())) {
            request.setRecActionType("all");
        }
        List<RecToIndexResponse> approvedModelIndexes;
        if (filterByModels) {
            if (CollectionUtils.isEmpty(request.getModelNames())) {
                throw new KylinException(INVALID_MODEL_NAME, MsgPicker.getMsg().getEMPTY_MODEL_NAME());
            }
            List<String> modelIds = Lists.newArrayList();
            for (String modelName : request.getModelNames()) {
                modelIds.add(getModel(modelName, projectName).getUuid());
            }
            approvedModelIndexes = optRecService.batchApprove(projectName, modelIds, request.getRecActionType());
        } else {
            approvedModelIndexes = optRecService.batchApprove(projectName, request.getRecActionType());
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new OpenRecApproveResponse(projectName, approvedModelIndexes), "");
    }

    @ApiOperation(value = "/accelerateSql", tags = { "AI" })
    @PostMapping(value = {"/accelerate_sqls", "/sql_acceleration"})
    @ResponseBody
    public EnvelopeResponse<OpenAccSqlResponse> accelerateSqls(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestAndOptimizeModels(request), "");
    }

    @ApiOperation(value = "suggestModels", tags = { "AI" })
    @PostMapping(value = "/model_suggestion")
    @ResponseBody
    public EnvelopeResponse<OpenSuggestionResponse> suggestModels(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        checkProjectNotSemiAuto(request.getProject());
        request.setForce2CreateNewModel(true);
        request.setAcceptRecommendation(true);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestOrOptimizeModels(request), "");
    }

    @ApiOperation(value = "optimizeModels", tags = { "AI" })
    @PostMapping(value = "/model_optimization")
    @ResponseBody
    public EnvelopeResponse<OpenSuggestionResponse> optimizeModels(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        checkProjectNotSemiAuto(request.getProject());
        checkNotEmpty(request.getSqls());
        request.setForce2CreateNewModel(false);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestOrOptimizeModels(request), "");
    }

    @ApiOperation(value = "deleteModel", tags = { "AI" })
    @DeleteMapping(value = "/{model_name:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        return modelController.deleteModel(modelId, projectName);
    }

    @ApiOperation(value = "checkSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segments/check")
    @ResponseBody
    public EnvelopeResponse<CheckSegmentResponse> checkSegments(@PathVariable("model") String modelAlias,
            @RequestBody CheckSegmentRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        aclEvaluate.checkProjectOperationPermission(request.getProject());
        checkRequiredArg("start", request.getStart());
        checkRequiredArg("end", request.getEnd());
        validateDataRange(request.getStart(), request.getEnd());
        NDataModel model = getModel(modelAlias, projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelService.checkSegments(request.getProject(),
                model.getAlias(), request.getStart(), request.getEnd()), "");
    }

    @ApiOperation(value = "updateMultiPartitionMapping", tags = { "QE" })
    @PutMapping(value = "/{model_name:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model_name") String modelAlias,
            @RequestBody MultiPartitionMappingRequest mappingRequest) {
        String projectName = checkProjectName(mappingRequest.getProject());
        checkProjectMLP(projectName);
        mappingRequest.setProject(projectName);
        val modelId = getModel(modelAlias, mappingRequest.getProject()).getId();
        return modelController.updateMultiPartitionMapping(modelId, mappingRequest);
    }

    @ApiOperation(value = "build multi_partition", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionsBuildRequest param) {
        String projectName = checkProjectName(param.getProject());
        checkProjectMLP(projectName);
        param.setProject(projectName);
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.buildMultiPartition(modelId, param);
    }

    @ApiOperation(value = "refresh multi_partition", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionsRefreshRequest param) {
        String projectName = checkProjectName(param.getProject());
        checkProjectMLP(projectName);
        param.setProject(projectName);
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.refreshMultiPartition(modelId, param);
    }

    @ApiOperation(value = "delete multi_partition", tags = { "DW" })
    @DeleteMapping(value = "/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartition(@RequestParam("model") String modelAlias,
            @RequestParam("project") String project, @RequestParam("segment_id") String segmentId,
            @RequestParam("sub_partition_values") List<String[]> subPartitionValues) {
        String projectName = checkProjectName(project);
        checkProjectMLP(projectName);
        checkRequiredArg("sub_partition_values", subPartitionValues);
        checkMLP("sub_partition_values", subPartitionValues);
        NDataModel model = getModel(modelAlias, projectName);
        modelService.deletePartitionsByValues(project, segmentId, model.getId(), subPartitionValues);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "addMultiPartitionValues", notes = "Add URL: {model}", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> addMultiPartitionValues(@PathVariable("model_name") String modelAlias,
            @RequestBody UpdateMultiPartitionValueRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkProjectMLP(projectName);
        val modelId = getModel(modelAlias, projectName).getId();
        return modelController.addMultiPartitionValues(modelId, request);
    }

    @ApiOperation(value = "update partition for multi partition and single partition", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/partition")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionSemantic(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionColumnRequest param) throws Exception {
        String projectName = checkProjectName(param.getProject());
        if (param.getMultiPartitionDesc() != null) {
            checkProjectMLP(projectName);
        }
        param.setProject(projectName);
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.updatePartitionSemantic(modelId, param);
    }

    @ApiOperation(value = "export model", tags = { "QE" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model_name:.+}/export")
    @ResponseBody
    public void exportModel(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project, @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String host,
            @RequestParam(value = "server_port", required = false) Integer port, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        modelController.exportModel(modelId, projectName, exportAs, element, host, port, request, response);
    }

    @ApiOperation(value = "bi export", tags = { "QE" })
    @GetMapping(value = "/bi_export")
    @ResponseBody
    public void biExport(@RequestParam("model_name") String modelAlias,
                            @RequestParam(value = "project") String project, @RequestParam(value = "export_as") SyncContext.BI exportAs,
                            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
                            @RequestParam(value = "server_host", required = false) String host,
                            @RequestParam(value = "server_port", required = false) Integer port, HttpServletRequest request,
                            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        modelController.biExport(modelId, projectName, exportAs, element, host, port, request, response);
    }

    @ApiOperation(value = "updateModelName", tags = { "AI" })
    @PutMapping(value = "/{model_name}/name")
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        String projectName = checkProjectName(modelRenameRequest.getProject());
        String modelId = getModel(modelAlias, projectName).getId();
        checkRequiredArg(NModelController.MODEL_ID, modelId);
        return modelController.updateModelName(modelId, modelRenameRequest);
    }

    private void checkProjectMLP(String project) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        if (!projectInstance.getConfig().isMultiPartitionEnabled()) {
            throw new KylinException(ServerErrorCode.MULTI_PARTITION_DISABLE,
                    MsgPicker.getMsg().getPROJECT_DISABLE_MLP());
        }
    }

    static void checkNotEmpty(List<String> sqls) {
        if (CollectionUtils.isEmpty(sqls)) {
            throw new KylinException(EMPTY_SQL_EXPRESSION, MsgPicker.getMsg().getNULL_EMPTY_SQL());
        }
    }

    static void checkMLP(String fieldName, List<String[]> subPartitionValues) {
        if (subPartitionValues.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "'%s' cannot be empty.", fieldName));
        }
    }
}
