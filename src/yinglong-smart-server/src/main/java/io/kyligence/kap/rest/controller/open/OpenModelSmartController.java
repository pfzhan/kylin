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
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_EMPTY;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.OpenBatchApproveRecItemsRequest;
import io.kyligence.kap.rest.response.OpenAccSqlResponse;
import io.kyligence.kap.rest.response.OpenOptRecLayoutsResponse;
import io.kyligence.kap.rest.response.OpenRecApproveResponse;
import io.kyligence.kap.rest.response.OpenSuggestionResponse;
import io.kyligence.kap.rest.response.OpenValidationResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelSmartController extends NBasicController {

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

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
        checkRequiredArg("modelId", modelId);
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
        List<OpenRecApproveResponse.RecToIndexResponse> approvedModelIndexes;
        if (filterByModels) {
            if (CollectionUtils.isEmpty(request.getModelNames())) {
                throw new KylinException(MODEL_NAME_EMPTY);
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
    @PostMapping(value = "/accelerate_sqls")
    @ResponseBody
    public EnvelopeResponse<OpenAccSqlResponse> accelerateSqls(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestAndOptimizeModels(request),
                "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestOrOptimizeModels(request),
                "");
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelSmartService.suggestOrOptimizeModels(request),
                "");
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

    static void checkNotEmpty(List<String> sqls) {
        if (CollectionUtils.isEmpty(sqls)) {
            throw new KylinException(EMPTY_SQL_EXPRESSION, MsgPicker.getMsg().getNULL_EMPTY_SQL());
        }
    }
}
