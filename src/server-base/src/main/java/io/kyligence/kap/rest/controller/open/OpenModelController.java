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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccerelateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
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

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.OpenApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.OpenBatchApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.response.OpenModelValidationResponse;
import io.kyligence.kap.rest.response.OpenModelSuggestionResponse;
import io.kyligence.kap.rest.response.OpenNRecommendationListResponse;
import io.kyligence.kap.rest.response.RecommendationStatsResponse;
import io.kyligence.kap.rest.response.NRecomendationListResponse;
import io.kyligence.kap.rest.response.OpenOptRecommendationResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.FavoriteRuleService;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelController extends NBasicController {

    @Autowired
    private NModelController modelController;

    @Autowired
    private ModelService modelService;

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
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        return modelController.getModels(modelAlias, exactMatch, project, owner, status, table, offset, limit, sortBy,
                reverse);
    }

    @VisibleForTesting
    public NDataModelResponse getModel(String modelAlias, String project) {
        List<NDataModelResponse> responses = modelService.getModels(modelAlias, project, true, null, null,
                "last_modify", true);
        if (CollectionUtils.isEmpty(responses)) {
            throw new BadRequestException(String.format("Can not find the model_name '%s'!", modelAlias));
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
        return modelController.getSegments(modelId, project, status, offset, limit, start, end, sortBy, reverse);
    }

    @PostMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        checkProjectName(buildSegmentsRequest.getProject());
        NDataModel nDataModel = getModel(modelAlias, buildSegmentsRequest.getProject());
        return modelController.buildSegmentsManually(nDataModel.getId(), buildSegmentsRequest);
    }

    @PutMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> refreshOrMergeSegmentsByIds(@PathVariable("model_name") String modelAlias,
            @RequestBody SegmentsRequest request) {
        checkProjectName(request.getProject());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.refreshOrMergeSegmentsByIds(modelId, request);
    }

    @DeleteMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project, // 
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids) {
        checkProjectName(project);
        if (purge) {
            ids = new String[0];
        }
        String modelId = getModel(modelAlias, project).getId();
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
        checkProjectName(project);
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

    @PostMapping(value = "/validation")
    @ResponseBody
    public EnvelopeResponse<List<String>> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());

        List<NDataModel> models = modelService.couldAnsweredByExistedModels(request.getProject(), request.getSqls());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                models.stream().map(NDataModel::getAlias).collect(Collectors.toList()), "");
    }

    @VisibleForTesting
    public Pair<Set<String>, Set<String>> batchSqlValidate(String project, List<String> sqls) {
        Set<String> normalSqls = Sets.newHashSet();
        Set<String> errorSqls = Sets.newHashSet();

        val validatedSqls = favoriteRuleService.batchSqlValidate(sqls, project);
        for (val entry : validatedSqls.entrySet()) {
            if (entry.getValue().isCapable()) {
                normalSqls.add(entry.getKey());
            } else {
                errorSqls.add(entry.getKey());
            }
        }
        return new Pair<>(normalSqls, errorSqls);
    }

    @PostMapping(value = "/model_validation")
    @ResponseBody
    public EnvelopeResponse<OpenModelValidationResponse> answeredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        if (!aclEvaluate.hasProjectWritePermission(getProject(request.getProject()))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        Map<String, List<String>> validSqls = Maps.newHashMap();
        List<String> errorSqls = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(request.getSqls())) {
            Pair<Set<String>, Set<String>> validatedSqls = batchSqlValidate(request.getProject(), request.getSqls());
            errorSqls.addAll(validatedSqls.getSecond());

            Map<String, List<NDataModel>> answeredModels = modelService.answeredByExistedModels(request.getProject(),
                    validatedSqls.getFirst());

            for (String sql : answeredModels.keySet()) {
                validSqls.put(sql,
                        answeredModels.get(sql).stream().map(NDataModel::getAlias).collect(Collectors.toList()));
            }
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, new OpenModelValidationResponse(validSqls, errorSqls),
                "");
    }

    @PostMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildIndexRequest request) {
        checkProjectName(request.getProject());
        if (!aclEvaluate.hasProjectOperationPermission(getProject(request.getProject()))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.buildIndicesManually(modelId, request);
    }

    @GetMapping(value = "/{model_name:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<OpenOptRecommendationResponse> getOptimizeRecommendations(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "sources", required = false, defaultValue = "") List<String> sources) {
        checkProjectName(project);
        if (!aclEvaluate.hasProjectWritePermission(getProject(project))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        String modelId = getModel(modelAlias, project).getId();

        OptRecommendationResponse optRecommendationResponse = modelController
                .getOptimizeRecommendations(modelId, project, sources).getData();

        OpenOptRecommendationResponse result = null;
        if (null != optRecommendationResponse) {
            result = OpenOptRecommendationResponse.convert(optRecommendationResponse);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @PutMapping(value = "/{model_name:.+}/recommendations")
    @ResponseBody
    public EnvelopeResponse<String> applyOptimizeRecommendations(@PathVariable("model_name") String modelAlias,
            @RequestBody OpenApplyRecommendationsRequest request) {
        checkProjectName(request.getProject());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return modelController.applyOptimizeRecommendations(modelId, OpenApplyRecommendationsRequest.convert(request));
    }

    @PostMapping(value = "/suggest_model")
    @ResponseBody
    public EnvelopeResponse<OpenNRecommendationListResponse> suggestModel(
            @RequestBody OpenSqlAccerelateRequest request) {
        checkProjectName(request.getProject());

        NRecomendationListResponse nRecomendationListResponse = modelService.suggestModel(request.getProject(),
                request.getSqls(), !request.getForce2CreateNewModel());

        List<ModelRequest> modelRequests = nRecomendationListResponse.getNewModels().stream().map(model -> {
            ModelRequest modelRequest = new ModelRequest(model);
            modelRequest.setIndexPlan(model.getIndices());
            return modelRequest;
        }).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(modelRequests)) {
            modelService.batchCreateModel(request.getProject(), modelRequests);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                OpenNRecommendationListResponse.convert(nRecomendationListResponse), "");
    }

    private OpenModelSuggestionResponse suggestOROptimizeModels(OpenSqlAccerelateRequest request) {
        NRecomendationListResponse nRecomendationListResponse = modelService.suggestModel(request.getProject(),
                request.getSqls(), !request.getForce2CreateNewModel());

        OpenModelSuggestionResponse result;
        if (request.getForce2CreateNewModel()) {
            List<ModelRequest> modelRequests = nRecomendationListResponse.getNewModels().stream().map(model -> {
                ModelRequest modelRequest = new ModelRequest(model);
                modelRequest.setIndexPlan(model.getIndices());
                return modelRequest;
            }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(modelRequests)) {
                modelService.batchCreateModel(request.getProject(), modelRequests);
            }

            result = OpenModelSuggestionResponse.convert(nRecomendationListResponse.getNewModels());
        } else {
            result = OpenModelSuggestionResponse.convert(nRecomendationListResponse.getOriginModels());
        }

        Set<String> availableSqls = result.getModels().stream()
                .map(OpenModelSuggestionResponse.RecommendationsResponse::getSqls).flatMap(List::stream)
                .collect(Collectors.toSet());

        result.setErrorSqls(Lists.newArrayList());
        for (String sql : request.getSqls()) {
            if (!availableSqls.contains(sql)) {
                result.getErrorSqls().add(sql);
            }
        }

        return result;
    }

    @PostMapping(value = "/model_suggestion")
    @ResponseBody
    public EnvelopeResponse<OpenModelSuggestionResponse> suggestModels(@RequestBody OpenSqlAccerelateRequest request) {
        checkProjectName(request.getProject());
        if (!aclEvaluate.hasProjectWritePermission(getProject(request.getProject()))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        request.setForce2CreateNewModel(true);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, suggestOROptimizeModels(request), "");
    }

    @PostMapping(value = "/model_optimization")
    @ResponseBody
    public EnvelopeResponse<OpenModelSuggestionResponse> optimizeModels(@RequestBody OpenSqlAccerelateRequest request) {
        checkProjectName(request.getProject());
        if (!aclEvaluate.hasProjectWritePermission(getProject(request.getProject()))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        request.setForce2CreateNewModel(false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, suggestOROptimizeModels(request), "");
    }

    @GetMapping(value = "/recommendations")
    @ResponseBody
    public EnvelopeResponse<RecommendationStatsResponse> getRecommendationsByProject(
            @RequestParam("project") String project) {
        checkProjectName(project);
        if (!aclEvaluate.hasProjectWritePermission(getProject(project))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        return modelController.getRecommendationsByProject(project);
    }

    @PutMapping(value = "/recommendations/batch")
    @ResponseBody
    public EnvelopeResponse<String> batchApplyRecommendations(
            @RequestBody OpenBatchApplyRecommendationsRequest request) {
        checkProjectName(request.getProject());
        if (!aclEvaluate.hasProjectWritePermission(getProject(request.getProject()))) {
            throw new BadRequestException(MsgPicker.getMsg().getPERMISSION_DENIED());
        }

        boolean filterByModels = request.isFilterByModelNames() && request.isFilterByModes();
        if (filterByModels) {
            if (CollectionUtils.isEmpty(request.getModelNames())) {
                throw new BadRequestException("Model names should not be empty when filter by model names!");
            }
            for (String modelName : request.getModelNames()) {
                getModel(modelName, request.getProject());
            }
        } else {
            request.setModelNames(null);
        }

        return modelController.batchApplyRecommendations(request.getProject(), request.getModelNames());
    }

    @DeleteMapping(value = "/{model_name:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project) {
        checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return modelController.deleteModel(modelId, project);
    }
}
