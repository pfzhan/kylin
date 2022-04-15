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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SQL_NUMBER_EXCEEDS_LIMIT;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.InMemoryJobRunner;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.scd2.SCD2SqlConverter;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinDesc;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.OpenAccSqlResponse;
import io.kyligence.kap.rest.response.OpenSuggestionResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.response.SuggestionResponse.ModelRecResponse;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractContext.ModelContext;
import io.kyligence.kap.smart.ModelCreateContext;
import io.kyligence.kap.smart.ModelReuseContext;
import io.kyligence.kap.smart.ModelSelectContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.model.AbstractJoinRule;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("modelSmartService")
public class ModelSmartService extends BasicService implements ModelSmartSupporter {
    @Autowired
    private RawRecService rawRecService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    public AclEvaluate aclEvaluate;

    @VisibleForTesting
    void saveRecResult(SuggestionResponse modelSuggestionResponse, String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (SuggestionResponse.ModelRecResponse response : modelSuggestionResponse.getReusedModels()) {

                NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(
                        modelMgr.getDataModelDesc(response.getId()), false);
                modelMgr.updateDataModel(response.getId(), copyForWrite -> {
                    copyForWrite.setJoinTables(response.getJoinTables());
                    copyForWrite.setComputedColumnDescs(response.getComputedColumnDescs());
                    copyForWrite.setAllNamedColumns(response.getAllNamedColumns());
                    copyForWrite.setAllMeasures(response.getAllMeasures());
                });
                NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val targetIndexPlan = response.getIndexPlan();
                indexMgr.updateIndexPlan(response.getId(), copyForWrite -> {
                    copyForWrite.setIndexes(targetIndexPlan.getIndexes());
                });
                baseIndexUpdater.update(indexPlanService);
            }
            return null;
        }, project);
    }

    void saveProposedJoinRelations(List<ModelRecResponse> reusedModels, AbstractContext proposeContext) {
        if (!proposeContext.isCanCreateNewModel()) {
            return;
        }

        String project = proposeContext.getProject();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (ModelRecResponse response : reusedModels) {
                NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                modelMgr.updateDataModel(response.getId(), copyForWrite -> {
                    List<JoinTableDesc> newJoinTables = response.getJoinTables();
                    if (newJoinTables.size() != copyForWrite.getJoinTables().size()) {
                        copyForWrite.setJoinTables(newJoinTables);
                        copyForWrite.setAllNamedColumns(response.getAllNamedColumns());
                        copyForWrite.setAllMeasures(response.getAllMeasures());
                        copyForWrite.setComputedColumnDescs(response.getComputedColumnDescs());
                    }
                });
            }
            return null;
        }, project);
    }

    public OpenSuggestionResponse suggestOrOptimizeModels(OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = suggestOptimizeModels(request, false);
        return OpenSuggestionResponse.from(innerResponse, request.getSqls());
    }

    public OpenAccSqlResponse suggestAndOptimizeModels(OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = suggestOptimizeModels(request, true);
        return OpenAccSqlResponse.from(innerResponse, request.getSqls());
    }

    public SuggestionResponse suggestOptimizeModels(OpenSqlAccelerateRequest request, boolean createNewModel) {
        AbstractContext proposeContext = suggestModel(request.getProject(), request.getSqls(),
                !request.getForce2CreateNewModel(), createNewModel);
        SuggestionResponse suggestionResponse;
        if (request.isAcceptRecommendation()) {
            suggestionResponse = saveModelAndApproveRecommendations(proposeContext, request);
        } else {
            suggestionResponse = saveModelAndRecommendations(proposeContext, request);
        }
        if (request.isWithOptimalModel()) {
            fillOptimalModels(proposeContext, suggestionResponse);
        }
        return suggestionResponse;
    }

    private void fillOptimalModels(AbstractContext proposeContext, SuggestionResponse suggestionResponse) {
        List<ModelRecResponse> responseOfOptimalModels = Lists.newArrayList();
        suggestionResponse.setOptimalModels(responseOfOptimalModels);
        Map<String, AccelerateInfo> accelerateInfoMap = proposeContext.getAccelerateInfoMap();
        if (MapUtils.isEmpty(accelerateInfoMap)) {
            return;
        }
        Set<String> reusedOrNewModelSqlSets = Sets.newHashSet();
        for (ModelRecResponse reusedModel : suggestionResponse.getReusedModels()) {
            for (LayoutRecDetailResponse index : reusedModel.getIndexes()) {
                reusedOrNewModelSqlSets.addAll(index.getSqlList());
            }
        }

        for (ModelRecResponse newModel : suggestionResponse.getNewModels()) {
            for (LayoutRecDetailResponse index : newModel.getIndexes()) {
                reusedOrNewModelSqlSets.addAll(index.getSqlList());
            }
        }

        Set<String> constantSqlSet = Sets.newHashSet();
        Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap = Maps.newHashMap();
        for (Map.Entry<String, AccelerateInfo> entry : accelerateInfoMap.entrySet()) {
            String key = entry.getKey();
            AccelerateInfo value = entry.getValue();
            if (!reusedOrNewModelSqlSets.contains(key)) {
                if (!value.isNotSucceed() && CollectionUtils.isEmpty(value.getRelatedLayouts())) {
                    constantSqlSet.add(key);
                } else {
                    errorOrOptimalAccelerateInfoMap.put(key, value);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(constantSqlSet)) {
            responseOfOptimalModels.add(buildConstantSqlRecResponse(Lists.newArrayList(constantSqlSet)));
        }
        if (MapUtils.isEmpty(errorOrOptimalAccelerateInfoMap)) {
            return;
        }

        Set<String> finishedModelSets = Sets.newHashSet();
        for (ModelContext modelContext : proposeContext.getModelContexts()) {
            if (modelContext.isTargetModelMissing() || modelContext.getOriginModel() == null
                    || modelContext.getOriginModel().isStreaming()
                    || finishedModelSets.contains(modelContext.getOriginModel().getUuid())) {
                continue;
            }

            try {
                collectResponseOfOptimalModels(modelContext, errorOrOptimalAccelerateInfoMap, responseOfOptimalModels);
                finishedModelSets.add(modelContext.getOriginModel().getUuid());
            } catch (Exception e) {
                log.error("Error occurs when collecting optimal models ", e);
            }
        }
    }

    private ModelRecResponse buildConstantSqlRecResponse(List<String> constantSqlSet) {
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        LayoutRecDetailResponse recDetailResponse = new LayoutRecDetailResponse();
        recDetailResponse.setSqlList(Lists.newArrayList(constantSqlSet));
        recDetailResponse.setIndexId(-1L);
        indexRecItems.add(recDetailResponse);
        ModelRecResponse modelRecResponse = new ModelRecResponse();
        modelRecResponse.setIndexes(indexRecItems);
        modelRecResponse.setAlias("CONSTANT");
        return modelRecResponse;
    }

    private void collectResponseOfOptimalModels(ModelContext modelContext,
            Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap,
            List<ModelRecResponse> responseOfOptimalModels) {
        Map<Long, Set<String>> layoutIdToSqlSetMap = mapLayoutToErrorOrOptimalSqlSet(modelContext,
                errorOrOptimalAccelerateInfoMap);
        if (MapUtils.isEmpty(layoutIdToSqlSetMap)) {
            return;
        }

        NDataModel originModel = modelContext.getOriginModel();
        Map<String, ComputedColumnDesc> oriComputedColumnMap = originModel.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getFullName, v -> v));
        Map<Integer, NDataModel.NamedColumn> colsOfOriginModelMap = originModel.getAllNamedColumns().stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, v -> v));
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), originModel.getProject())
                .getIndexPlan(originModel.getUuid());
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        layoutIdToSqlSetMap.forEach(((layoutId, optimalSqlSet) -> {
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            LayoutRecDetailResponse response = new LayoutRecDetailResponse();
            ImmutableList<Integer> colOrder = layoutEntity.getColOrder();
            Map<String, ComputedColumnDesc> computedColumnsMap = Maps.newHashMap();
            colOrder.forEach(idx -> {
                if (idx < NDataModel.MEASURE_ID_BASE) {
                    ImmutableBiMap<Integer, TblColRef> effectiveDimensions = originModel.getEffectiveDimensions();
                    NDataModel.NamedColumn col = colsOfOriginModelMap.get(idx);
                    TblColRef tblColRef = originModel.getEffectiveCols().get(idx);
                    if (!effectiveDimensions.containsKey(idx) || null == col || null == tblColRef) {
                        return;
                    }
                    String dataType = effectiveDimensions.get(idx).getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, false, dataType));
                    if (tblColRef.getColumnDesc().isComputedColumn()
                            && oriComputedColumnMap.containsKey(tblColRef.getAliasDotName())) {
                        computedColumnsMap.put(tblColRef.getAliasDotName(),
                                oriComputedColumnMap.get(tblColRef.getAliasDotName()));
                    }
                } else if (originModel.getEffectiveMeasures().containsKey(idx)) {
                    NDataModel.Measure measure = originModel.getEffectiveMeasures().get(idx);
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, false));
                    List<TblColRef> colRefs = measure.getFunction().getColRefs();
                    colRefs.forEach(colRef -> {
                        if (colRef.getColumnDesc().isComputedColumn()
                                && oriComputedColumnMap.containsKey(colRef.getAliasDotName())) {
                            computedColumnsMap.put(colRef.getAliasDotName(),
                                    oriComputedColumnMap.get(colRef.getAliasDotName()));
                        }
                    });
                }
            });
            List<LayoutRecDetailResponse.RecComputedColumn> computedColumnDescList = computedColumnsMap.values()
                    .stream().map(e -> new LayoutRecDetailResponse.RecComputedColumn(e, false))
                    .collect(Collectors.toList());
            response.setComputedColumns(computedColumnDescList);
            response.setIndexId(layoutEntity.getId());
            response.setSqlList(Lists.newArrayList(optimalSqlSet));
            indexRecItems.add(response);
        }));

        ModelRecResponse response = new ModelRecResponse(originModel);
        response.setIndexPlan(indexPlan);
        response.setIndexes(indexRecItems);
        responseOfOptimalModels.add(response);
    }

    private Map<Long, Set<String>> mapLayoutToErrorOrOptimalSqlSet(ModelContext modelContext,
            Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap) {
        if (modelContext == null || MapUtils.isEmpty(errorOrOptimalAccelerateInfoMap)) {
            return Maps.newHashMap();
        }
        Map<Long, Set<String>> layoutToSqlSetMap = Maps.newHashMap();
        errorOrOptimalAccelerateInfoMap.forEach((sql, info) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : info.getRelatedLayouts()) {
                if (!StringUtils.equalsIgnoreCase(relation.getModelId(), modelContext.getOriginModel().getUuid())) {
                    continue;
                }
                layoutToSqlSetMap.putIfAbsent(relation.getLayoutId(), Sets.newHashSet());
                layoutToSqlSetMap.get(relation.getLayoutId()).add(relation.getSql());
            }
        });
        return layoutToSqlSetMap;
    }

    private SuggestionResponse saveModelAndRecommendations(AbstractContext proposeContext,
            OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = buildModelSuggestionResponse(proposeContext);
        List<ModelRequest> modelRequests = convertToModelRequest(innerResponse.getNewModels(), request);
        modelService.checkNewModels(request.getProject(), modelRequests);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if (request.isSaveNewModel()) {
                modelService.saveNewModelsAndIndexes(request.getProject(), modelRequests);
            }
            saveProposedJoinRelations(innerResponse.getReusedModels(), proposeContext);
            rawRecService.transferAndSaveRecommendations(proposeContext);
            Set<String> modelIds = proposeContext.getModelContexts().stream() //
                    .map(AbstractContext.ModelContext::getTargetModel) //
                    .filter(Objects::nonNull).map(NDataModel::getId) //
                    .collect(Collectors.toSet());
            optRecService.updateRecommendationCount(proposeContext.getProject(), modelIds);
            return null;
        }, request.getProject());
        return innerResponse;
    }

    private SuggestionResponse saveModelAndApproveRecommendations(AbstractContext proposeContext,
            OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = buildModelSuggestionResponse(proposeContext);
        List<ModelRequest> modelRequests = convertToModelRequest(innerResponse.getNewModels(), request);
        modelService.checkNewModels(request.getProject(), modelRequests);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelService.saveNewModelsAndIndexes(request.getProject(), modelRequests);
            saveRecResult(innerResponse, request.getProject());
            return null;
        }, request.getProject());

        return innerResponse;
    }

    private List<ModelRequest> convertToModelRequest(List<ModelRecResponse> newModels,
            OpenSqlAccelerateRequest request) {
        return newModels.stream().map(modelResponse -> {
            ModelRequest modelRequest = new ModelRequest(modelResponse);
            modelRequest.setIndexPlan(modelResponse.getIndexPlan());
            modelRequest.setWithEmptySegment(request.isWithEmptySegment());
            modelRequest.setWithModelOnline(request.isWithModelOnline());
            modelRequest.setWithBaseIndex(request.isWithBaseIndex());
            return modelRequest;
        }).collect(Collectors.toList());
    }

    public AbstractContext probeRecommendation(String project, List<String> sqls) {
        if (modelService.isProjectNotExist(project)) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        KylinConfig kylinConfig = getManager(NProjectManager.class).getProject(project).getConfig();
        AbstractContext proposeContext = new ModelSelectContext(kylinConfig, project, sqls.toArray(new String[0]));
        ProposerJob.propose(proposeContext,
                (config, runnerType, projectName, resources) -> new InMemoryJobRunner(config, projectName, resources));
        return proposeContext;
    }

    public boolean couldAnsweredByExistedModel(String project, List<String> sqls) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return true;
        }

        AbstractContext proposeContext = probeRecommendation(project, sqls);
        List<NDataModel> models = proposeContext.getProposedModels().stream().filter(model -> !model.isStreaming())
                .collect(Collectors.toList());
        return CollectionUtils.isNotEmpty(models);
    }

    public AbstractContext suggestModel(String project, List<String> sqls, boolean reuseExistedModel,
            boolean createNewModel) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return null;
        }
        KylinConfig kylinConfig = getManager(NProjectManager.class).getProject(project).getConfig();
        checkBatchSqlSize(kylinConfig, sqls);
        AbstractContext proposeContext;
        String[] sqlArray = sqls.toArray(new String[0]);
        if (SmartConfig.wrap(kylinConfig).getModelOptRule().equalsIgnoreCase(AbstractJoinRule.APPEND)) {
            proposeContext = new ModelReuseContext(kylinConfig, project, sqlArray, true);
        } else if (reuseExistedModel) {
            proposeContext = new ModelReuseContext(kylinConfig, project, sqlArray, createNewModel);
        } else {
            proposeContext = new ModelCreateContext(kylinConfig, project, sqlArray);
        }
        return ProposerJob.propose(proposeContext,
                (config, runnerType, projectName, resources) -> new InMemoryJobRunner(config, projectName, resources));
    }

    public SuggestionResponse buildModelSuggestionResponse(AbstractContext context) {
        List<ModelRecResponse> responseOfNewModels = Lists.newArrayList();
        List<ModelRecResponse> responseOfReusedModels = Lists.newArrayList();

        for (AbstractContext.ModelContext modelContext : context.getModelContexts()) {
            if (modelContext.isTargetModelMissing()) {
                continue;
            }

            if (modelContext.getOriginModel() != null) {
                collectResponseOfReusedModels(modelContext, responseOfReusedModels);
            } else {
                collectResponseOfNewModels(context, modelContext, responseOfNewModels);
            }
        }
        responseOfReusedModels.removeIf(ModelRecResponse::isStreaming);
        return new SuggestionResponse(responseOfReusedModels, responseOfNewModels);
    }

    private void checkBatchSqlSize(KylinConfig kylinConfig, List<String> sqls) {
        val msg = MsgPicker.getMsg();
        int limit = kylinConfig.getSuggestModelSqlLimit();
        if (sqls.size() > limit) {
            throw new KylinException(SQL_NUMBER_EXCEEDS_LIMIT,
                    String.format(Locale.ROOT, msg.getSQL_NUMBER_EXCEEDS_LIMIT(), limit));
        }
    }

    private void collectResponseOfReusedModels(AbstractContext.ModelContext modelContext,
            List<ModelRecResponse> responseOfReusedModels) {
        Map<Long, Set<String>> layoutToSqlSet = mapLayoutToSqlSet(modelContext);
        Map<String, ComputedColumnDesc> oriCCMap = Maps.newHashMap();
        List<ComputedColumnDesc> oriCCList = modelContext.getOriginModel().getComputedColumnDescs();
        oriCCList.forEach(cc -> oriCCMap.put(cc.getFullName(), cc));
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        List<ComputedColumnDesc> ccList = modelContext.getTargetModel().getComputedColumnDescs();
        ccList.forEach(cc -> ccMap.put(cc.getFullName(), cc));
        NDataModel targetModel = modelContext.getTargetModel();
        NDataModel originModel = modelContext.getOriginModel();
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        modelContext.getIndexRexItemMap().forEach((key, layoutRecItemV2) -> {
            LayoutRecDetailResponse response = new LayoutRecDetailResponse();
            LayoutEntity layout = layoutRecItemV2.getLayout();
            ImmutableList<Integer> colOrder = layout.getColOrder();
            Map<ComputedColumnDesc, Boolean> ccStateMap = Maps.newHashMap();
            Map<Integer, NDataModel.NamedColumn> colsOfTargetModelMap = Maps.newHashMap();
            targetModel.getAllNamedColumns().forEach(col -> colsOfTargetModelMap.put(col.getId(), col));
            colOrder.forEach(idx -> {
                if (idx < NDataModel.MEASURE_ID_BASE && originModel.getEffectiveDimensions().containsKey(idx)) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    String dataType = originModel.getEffectiveDimensions().get(idx).getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, false, dataType));
                } else if (idx < NDataModel.MEASURE_ID_BASE) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    TblColRef tblColRef = targetModel.getEffectiveCols().get(idx);
                    String colRefAliasDotName = tblColRef.getAliasDotName();
                    if (tblColRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                        ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                    }
                    String dataType = tblColRef.getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, true, dataType));
                } else if (originModel.getEffectiveMeasures().containsKey(idx)) {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, false));
                } else {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    List<TblColRef> colRefs = measure.getFunction().getColRefs();
                    colRefs.forEach(colRef -> {
                        String colRefAliasDotName = colRef.getAliasDotName();
                        if (colRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                            ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                        }
                    });
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, true));
                }
            });
            List<LayoutRecDetailResponse.RecComputedColumn> newCCList = Lists.newArrayList();
            ccStateMap.forEach((k, v) -> newCCList.add(new LayoutRecDetailResponse.RecComputedColumn(k, v)));
            response.setComputedColumns(newCCList);
            response.setIndexId(layout.getId());
            Set<String> sqlSet = layoutToSqlSet.get(layout.getId());
            if (CollectionUtils.isNotEmpty(sqlSet)) {
                response.setSqlList(Lists.newArrayList(sqlSet));
            }
            indexRecItems.add(response);
        });

        ModelRecResponse response = new ModelRecResponse(targetModel);
        response.setIndexPlan(modelContext.getTargetIndexPlan());
        response.setIndexes(indexRecItems);
        responseOfReusedModels.add(response);
    }

    private Map<Long, Set<String>> mapLayoutToSqlSet(AbstractContext.ModelContext modelContext) {
        if (modelContext == null) {
            return Maps.newHashMap();
        }
        Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
        Map<Long, Set<String>> layoutToSqlSet = Maps.newHashMap();
        accelerateInfoMap.forEach((sql, info) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : info.getRelatedLayouts()) {
                if (!StringUtils.equalsIgnoreCase(relation.getModelId(), modelContext.getTargetModel().getUuid())) {
                    continue;
                }
                layoutToSqlSet.putIfAbsent(relation.getLayoutId(), Sets.newHashSet());
                layoutToSqlSet.get(relation.getLayoutId()).add(relation.getSql());
            }
        });
        return layoutToSqlSet;
    }

    private void collectResponseOfNewModels(AbstractContext context, AbstractContext.ModelContext modelContext,
            List<ModelRecResponse> responseOfNewModels) {
        val sqlList = context.getAccelerateInfoMap().entrySet().stream()//
                .filter(entry -> entry.getValue().getRelatedLayouts().stream()//
                        .anyMatch(relation -> relation.getModelId().equals(modelContext.getTargetModel().getId())))
                .map(Map.Entry::getKey).collect(Collectors.toList());
        NDataModel model = modelContext.getTargetModel();
        IndexPlan indexPlan = modelContext.getTargetIndexPlan();
        ImmutableBiMap<Integer, TblColRef> effectiveDimensions = model.getEffectiveDimensions();
        List<LayoutRecDetailResponse.RecDimension> recDims = model.getAllNamedColumns().stream() //
                .filter(NDataModel.NamedColumn::isDimension) //
                .map(c -> {
                    String datatype = effectiveDimensions.get(c.getId()).getDatatype();
                    return new LayoutRecDetailResponse.RecDimension(c, true, datatype);
                }) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecMeasure> recMeasures = model.getAllMeasures().stream() //
                .map(measure -> new LayoutRecDetailResponse.RecMeasure(measure, true)) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecComputedColumn> recCCList = model.getComputedColumnDescs().stream() //
                .map(cc -> new LayoutRecDetailResponse.RecComputedColumn(cc, true)) //
                .collect(Collectors.toList());
        LayoutRecDetailResponse virtualResponse = new LayoutRecDetailResponse();
        virtualResponse.setIndexId(-1L);
        virtualResponse.setDimensions(recDims);
        virtualResponse.setMeasures(recMeasures);
        virtualResponse.setComputedColumns(recCCList);
        virtualResponse.setSqlList(sqlList);

        ModelRecResponse response = new ModelRecResponse(model);
        response.setIndexPlan(indexPlan);
        response.setIndexes(Lists.newArrayList(virtualResponse));
        responseOfNewModels.add(response);
    }

    @Override
    public JoinDesc suggNonEquiJoinModel(final KylinConfig kylinConfig, final String project,
            final JoinDesc modelJoinDesc, final SimplifiedJoinDesc requestJoinDesc) {
        String nonEquiSql = SCD2SqlConverter.INSTANCE.genSCD2SqlStr(modelJoinDesc,
                requestJoinDesc.getSimplifiedNonEquiJoinConditions());

        BackdoorToggles.addToggle(BackdoorToggles.QUERY_NON_EQUI_JOIN_MODEL_ENABLED, "true");
        AbstractContext context = new ModelCreateContext(kylinConfig, project, new String[] { nonEquiSql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.executePropose();

        List<AbstractContext.ModelContext> suggModelContexts = smartMaster.getContext().getModelContexts();
        if (CollectionUtils.isEmpty(suggModelContexts) || Objects.isNull(suggModelContexts.get(0).getTargetModel())) {

            AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(nonEquiSql);
            if (Objects.nonNull(accelerateInfo)) {
                log.error("scd2 suggest error, sql:{}", nonEquiSql, accelerateInfo.getFailedCause());
            }
            throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR, "it has illegal join condition");
        }

        if (suggModelContexts.size() != 1) {
            throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR,
                    "scd2 suggest more than one model:" + nonEquiSql);
        }

        return suggModelContexts.get(0).getTargetModel().getJoinTables().get(0).getJoin();
    }
}
