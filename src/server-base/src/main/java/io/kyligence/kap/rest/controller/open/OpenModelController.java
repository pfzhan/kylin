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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SQL_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MODEL_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.tool.bisync.SyncContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.CheckSegmentRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OpenBatchApproveRecItemsRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.CheckSegmentResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.JobInfoResponseWithFailure;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.response.OpenModelSuggestionResponse;
import io.kyligence.kap.rest.response.OpenModelValidationResponse;
import io.kyligence.kap.rest.response.OpenOptRecLayoutsResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.swagger.annotations.ApiOperation;
import lombok.val;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelController extends NBasicController {

    @Autowired
    private NModelController modelController;

    @Autowired
    private ModelService modelService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

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
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "model_alias_or_owner", required = false) String modelAliasOrOwner,
            @RequestParam(value = "last_modify_from", required = false) Long lastModifyFrom,
            @RequestParam(value = "last_modify_to", required = false) Long lastModifyTo) {
        checkProjectName(project);
        return modelController.getModels(modelAlias, exactMatch, project, owner, status, table, offset, limit, sortBy,
                reverse, modelAliasOrOwner, lastModifyFrom, lastModifyTo);
    }

    @VisibleForTesting
    public NDataModelResponse getModel(String modelAlias, String project) {
        List<NDataModelResponse> responses = modelService.getModels(modelAlias, project, true, null, null,
                "last_modify", true);
        if (CollectionUtils.isEmpty(responses)) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        return responses.get(0);
    }

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
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return modelController.getSegments(modelId, project, status, offset, limit, start, end, null, null, false,
                sortBy, reverse);
    }

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
        checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return modelController.getMultiPartition(modelId, project, segId, status, pageOffset, pageSize, sortBy, reverse);
    }

    @PostMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        validatePriority(buildSegmentsRequest.getPriority());
        NDataModel nDataModel = getModel(modelAlias, buildSegmentsRequest.getProject());
        return modelController.buildSegmentsManually(nDataModel.getId(), buildSegmentsRequest);
    }

    @PutMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model_name") String modelAlias,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.refreshOrMergeSegments(modelId, request);
    }

    @DeleteMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project, // 
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        checkProjectName(project);
        if (purge) {
            ids = new String[0];
        }
        String modelId = getModel(modelAlias, project).getId();
        return modelController.deleteSegments(modelId, project, purge, force, ids, names);
    }

    @PostMapping(value = "/{model_name}/segments/completion")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> completeSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "parallel", required = false, defaultValue = "false") boolean parallel,
            @RequestParam(value = "ids", required = false) String[] ids,
            @RequestParam(value = "names", required = false) String[] names) {
        checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        String[] segIds = modelService.convertSegmentIdWithName(modelId, project, ids, names);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(project);
        req.setParallelBuildBySegment(parallel);
        req.setSegmentIds(Lists.newArrayList(segIds));
        return modelController.addIndexesToSegments(modelId, req);
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
    public EnvelopeResponse<String> updatePartitionDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias,
            @RequestBody ModelParatitionDescRequest modelParatitionDescRequest) {
        checkProjectName(project);
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
        modelService.updateDataModelParatitionDesc(project, modelAlias, modelParatitionDescRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/validation")
    @ResponseBody
    public EnvelopeResponse<List<String>> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());

        List<NDataModel> models = modelService.couldAnsweredByExistedModels(request.getProject(), request.getSqls());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                models.stream().map(NDataModel::getAlias).collect(Collectors.toList()), "");
    }

    @VisibleForTesting
    public OpenModelValidationResponse batchSqlValidate(String project, List<String> sqls) {
        Set<String> normalSqls = Sets.newHashSet();
        Set<String> errorSqls = Sets.newHashSet();
        Set<OpenModelValidationResponse.ErrorSqlDetail> errorSqlDetailSet = Sets.newHashSet();

        val validatedSqls = favoriteRuleService.batchSqlValidate(sqls, project);
        for (val entry : validatedSqls.entrySet()) {
            String sql = entry.getKey();
            if (entry.getValue().isCapable()) {
                normalSqls.add(sql);
            } else {
                SQLValidateResult validateResult = entry.getValue();
                errorSqls.add(sql);
                errorSqlDetailSet
                        .add(new OpenModelValidationResponse.ErrorSqlDetail(sql, validateResult.getSqlAdvices()));
            }
        }

        Map<String, List<NDataModel>> answeredModels = modelService.answeredByExistedModels(project, normalSqls);
        Map<String, List<String>> validSqls = answeredModels.keySet().stream()
                .collect(Collectors.toMap(sql -> sql,
                        sql -> answeredModels.get(sql).stream().map(NDataModel::getAlias).collect(Collectors.toList()),
                        (a, b) -> b));

        return new OpenModelValidationResponse(validSqls, Lists.newArrayList(errorSqls),
                Lists.newArrayList(errorSqlDetailSet));
    }

    @PostMapping(value = "/model_validation")
    @ResponseBody
    public EnvelopeResponse<OpenModelValidationResponse> answeredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        aclEvaluate.checkProjectWritePermission(request.getProject());
        if (CollectionUtils.isEmpty(request.getSqls())) {
            throw new KylinException(EMPTY_SQL_EXPRESSION, MsgPicker.getMsg().getNULL_EMPTY_SQL());
        }
        OpenModelValidationResponse response = batchSqlValidate(request.getProject(), request.getSqls());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PostMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.buildIndicesManually(modelId, request);
    }

    @GetMapping(value = "/{model_name:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<OpenOptRecLayoutsResponse> getRecommendations(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "recActionType", required = false, defaultValue = "all") String recActionType) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        String modelId = getModel(modelAlias, project).getId();
        checkRequiredArg(NModelController.MODEL_ID, modelId);
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(project, modelId, recActionType);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                new OpenOptRecLayoutsResponse(project, modelId, response), "");
    }

    @PutMapping(value = "/recommendations/batch")
    @ResponseBody
    public EnvelopeResponse<String> batchApproveRecommendations(@RequestBody OpenBatchApproveRecItemsRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        boolean filterByModels = request.isFilterByModes();
        if (request.getRecActionType() == null || StringUtils.isEmpty(request.getRecActionType().trim())) {
            request.setRecActionType("all");
        }
        if (filterByModels) {
            if (CollectionUtils.isEmpty(request.getModelNames())) {
                throw new KylinException(INVALID_MODEL_NAME, MsgPicker.getMsg().getEMPTY_MODEL_NAME());
            }
            for (String modelName : request.getModelNames()) {
                getModel(modelName, request.getProject());
            }
            optRecService.batchApprove(request.getProject(), request.getModelNames(), request.getRecActionType());
        } else {
            optRecService.batchApprove(request.getProject(), request.getRecActionType());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/model_suggestion")
    @ResponseBody
    public EnvelopeResponse<OpenModelSuggestionResponse> suggestModels(@RequestBody OpenSqlAccelerateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        request.setForce2CreateNewModel(true);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.suggestOrOptimizeModels(request), "");
    }

    @PostMapping(value = "/model_optimization")
    @ResponseBody
    public EnvelopeResponse<OpenModelSuggestionResponse> optimizeModels(@RequestBody OpenSqlAccelerateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        request.setForce2CreateNewModel(false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, modelService.suggestOrOptimizeModels(request), "");
    }

    @DeleteMapping(value = "/{model_name:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project) {
        checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return modelController.deleteModel(modelId, project);
    }

    @ApiOperation(value = "check segment range")
    @PostMapping(value = "/{model:.+}/segments/check")
    @ResponseBody
    public EnvelopeResponse<CheckSegmentResponse> checkSegments(@PathVariable("model") String modelName,
            @RequestBody CheckSegmentRequest request) {
        checkProjectName(request.getProject());
        aclEvaluate.checkProjectOperationPermission(request.getProject());
        checkRequiredArg("start", request.getStart());
        checkRequiredArg("end", request.getEnd());
        validateDataRange(request.getStart(), request.getEnd());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                modelService.checkSegments(request.getProject(), modelName, request.getStart(), request.getEnd()), "");
    }

    @ApiOperation(value = "updateMultiPartitionMapping")
    @PutMapping(value = "/{model_name:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model_name") String modelAlias,
                                                                @RequestBody MultiPartitionMappingRequest mappingRequest) {
        checkProjectName(mappingRequest.getProject());
        val modelId = getModel(modelAlias, mappingRequest.getProject()).getId();
        modelService.updateMultiPartitionMapping(mappingRequest.getProject(), modelId, mappingRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "build multi_partition")
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model_name") String modelAlias,
                                                        @RequestBody PartitionsBuildRequest param) {
        checkProjectName(param.getProject());
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.buildMultiPartition(modelId, param);
    }

    @ApiOperation(value = "refresh multi_partition")
    @PutMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model_name") String modelAlias,
                                                          @RequestBody PartitionsRefreshRequest param) {
        checkProjectName(param.getProject());
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.refreshMultiPartition(modelId, param);
    }

    @ApiOperation(value = "delete multi_partition")
    @DeleteMapping(value = "/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartition(@RequestParam("model") String modelAlias,
                                                         @RequestParam("project") String project,
                                                         @RequestParam("segment") String segment,
                                                         @RequestParam(value = "ids") String[] ids) {
        checkProjectName(project);
        checkRequiredArg("ids", ids);
        val modelId = getModel(modelAlias, project).getId();
        return modelController.deleteMultiPartition(modelId, project, segment, ids);
    }

    @ApiOperation(value = "update partition")
    @PutMapping(value = "/{model_name:.+}/partition")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionSemantic(@PathVariable("model_name") String modelAlias,
                                                            @RequestBody PartitionColumnRequest param) throws Exception {
        checkProjectName(param.getProject());
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.updatePartitionSemantic(modelId, param);
    }

    @ApiOperation(value = "export model", notes = "Add URL: {model}")
    @GetMapping(value = "/{model_name:.+}/export")
    @ResponseBody
    public void exportModel(@PathVariable("model_name") String modelAlias, @RequestParam(value = "project") String project,
                            @RequestParam(value = "export_as") SyncContext.BI exportAs,
                            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
                            @RequestParam(value = "server_host", required = false) String host,
                            @RequestParam(value = "server_port", required = false) Integer port, HttpServletRequest request,
                            HttpServletResponse response) throws IOException {
        checkProjectName(project);
        val modelId = getModel(modelAlias, project).getId();
        modelController.exportModel(modelId, project, exportAs, element, host, port, request, response);
    }
}
