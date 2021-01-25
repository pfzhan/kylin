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

import static io.kyligence.kap.common.util.CollectionUtil.intersection;
import static org.apache.kylin.common.exception.ServerErrorCode.REC_LIST_OUT_OF_DATE;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_REC_OPERATION_TYPE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.cube.optimization.IndexOptimizerFactory;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.ref.CCRef;
import io.kyligence.kap.metadata.recommendation.ref.DimensionRef;
import io.kyligence.kap.metadata.recommendation.ref.LayoutRef;
import io.kyligence.kap.metadata.recommendation.ref.MeasureRef;
import io.kyligence.kap.metadata.recommendation.ref.ModelColumnRef;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;
import io.kyligence.kap.metadata.recommendation.ref.RecommendationRef;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.OpenRecApproveResponse.RecToIndexResponse;
import io.kyligence.kap.rest.response.OptRecDepResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("optRecService")
public class OptRecService extends BasicService implements ModelUpdateListener {

    public static final int V2 = 2;
    public static final String RECOMMENDATION_SOURCE = "recommendation_source";
    public static final String OPERATION_ERROR_MSG = "The operation types of recommendation includes: add_index, removal_index and all(by default)";
    public static final String ALL = OptRecService.RecActionType.ALL.name();

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    private static final class RecApproveContext {
        private final Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        private final Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        private final Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();
        private final List<Long> addedLayoutIdList = Lists.newArrayList();
        private final List<Long> removedLayoutIdList = Lists.newArrayList();
        private final Map<String, NDataModel.Measure> functionToMeasureMap = Maps.newHashMap();

        @Getter
        private final OptRecV2 recommendation;
        private final Map<Integer, String> userDefinedRecNameMap;
        private final OptRecManagerV2 recManagerV2;
        private final String project;

        private RecApproveContext(String project, String modelId, Map<Integer, String> userDefinedRecNameMap) {
            this.project = project;
            this.userDefinedRecNameMap = userDefinedRecNameMap;
            this.recManagerV2 = OptRecManagerV2.getInstance(project);
            this.recommendation = recManagerV2.loadOptRecV2(modelId);
        }

        public List<RawRecItem> approveRawRecItems(List<Integer> recItemIds, boolean isAdd) {
            recItemIds.forEach(id -> checkRecItemIsValidAndReturn(recommendation, id, isAdd));
            List<RawRecItem> rawRecItems = getAllRelatedRecItems(recItemIds, isAdd);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                if (isAdd) {
                    rewriteModel(rawRecItems);
                    List<Long> addedLayouts = rewriteIndexPlan(rawRecItems);
                    addedLayoutIdList.addAll(addedLayouts);
                } else {
                    shiftLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), rawRecItems);
                    List<Long> removedLayouts = reduceIndexPlan(rawRecItems);
                    removedLayoutIdList.addAll(removedLayouts);
                }
                return null;
            }, project);
            return rawRecItems;
        }

        public List<RawRecItem> getAllRelatedRecItems(List<Integer> layoutIds, boolean isAdd) {
            Set<RawRecItem> allRecItems = Sets.newLinkedHashSet();
            Map<Integer, LayoutRef> layoutRefs = isAdd //
                    ? recommendation.getAdditionalLayoutRefs()
                    : recommendation.getRemovalLayoutRefs();
            layoutIds.forEach(id -> {
                if (layoutRefs.containsKey(-id)) {
                    collect(allRecItems, layoutRefs.get(-id));
                }
            });
            return Lists.newArrayList(allRecItems);
        }

        private void collect(Set<RawRecItem> recItemsCollector, RecommendationRef ref) {
            if (ref instanceof ModelColumnRef) {
                return;
            }

            RawRecItem recItem = recommendation.getRawRecItemMap().get(-ref.getId());
            if (recItem == null || recItemsCollector.contains(recItem)) {
                return;
            }
            if (!ref.isBroken() && !ref.isExisted()) {
                ref.getDependencies().forEach(dep -> collect(recItemsCollector, dep));
                recItemsCollector.add(recItem);
            }
        }

        private void shiftLayoutHitCount(String project, String modelUuid, List<RawRecItem> rawRecItems) {
            if (CollectionUtils.isEmpty(rawRecItems)) {
                return;
            }
            Set<Long> layoutsToRemove = Sets.newHashSet();
            rawRecItems.forEach(rawRecItem -> {
                long layoutId = RawRecUtil.getLayout(rawRecItem).getId();
                layoutsToRemove.add(layoutId);
            });

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            NDataflow originDf = dfMgr.getDataflow(modelUuid);
            NDataflow copiedDf = originDf.copy();
            IndexOptimizerFactory.getOptimizer(copiedDf, true).getGarbageLayoutMap(copiedDf);
            Map<Long, FrequencyMap> layoutHitCount = copiedDf.getLayoutHitCount();
            layoutHitCount.forEach((id, freqMap) -> {
                if (!layoutsToRemove.contains(id)) {
                    FrequencyMap oriMap = originDf.getLayoutHitCount().get(id);
                    if (oriMap != null) {
                        layoutHitCount.put(id, oriMap);
                    }
                }
            });
            dfMgr.updateDataflow(copiedDf.getUuid(), copyForWrite -> copyForWrite.setLayoutHitCount(layoutHitCount));
        }

        private void rewriteModel(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return;
            }
            logBeginRewrite("Model");
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            modelManager.updateDataModel(recommendation.getUuid(), copyForWrite -> {
                copyForWrite.getAllNamedColumns().forEach(column -> {
                    if (column.isExist()) {
                        columns.putIfAbsent(column.getId(), column);
                    }
                    if (column.isDimension()) {
                        dimensions.putIfAbsent(column.getId(), column);
                    }
                });

                copyForWrite.getAllMeasures().forEach(measure -> {
                    if (!measure.isTomb()) {
                        measures.putIfAbsent(measure.getId(), measure);
                    }
                });

                copyForWrite.getAllMeasures().forEach(measure -> {
                    if (!measure.isTomb()) {
                        functionToMeasureMap.put(measure.getFunction().toString(), measure);
                    }
                });

                for (RawRecItem rawRecItem : recItems) {
                    switch (rawRecItem.getType()) {
                    case DIMENSION:
                        writeDimensionToModel(rawRecItem);
                        break;
                    case COMPUTED_COLUMN:
                        writeCCToModel(copyForWrite, rawRecItem);
                        break;
                    case MEASURE:
                        writeMeasureToModel(copyForWrite, rawRecItem);
                        break;
                    case ADDITIONAL_LAYOUT:
                    case REMOVAL_LAYOUT:
                    default:
                        break;
                    }
                }

                // Protect the model from being damaged
                log.info(copyForWrite.getUuid());
                copyForWrite.keepColumnOrder();
                copyForWrite.keepMeasureOrder();
                List<NDataModel.NamedColumn> existedColumns = copyForWrite.getAllNamedColumns().stream()
                        .filter(NDataModel.NamedColumn::isExist).collect(Collectors.toList());
                List<NDataModel.Measure> existedMeasure = copyForWrite.getAllMeasures().stream()
                        .filter(measure -> !measure.isTomb()).collect(Collectors.toList());
                NDataModel.changeNameIfDup(existedColumns);
                NDataModel.checkDuplicateColumn(existedColumns);
                NDataModel.checkDuplicateMeasure(existedMeasure);
                NDataModel.checkDuplicateCC(copyForWrite.getComputedColumnDescs());
            });
            logFinishRewrite("Model");
        }

        private void writeMeasureToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> measureRefs = recommendation.getMeasureRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef recommendationRef = measureRefs.get(negRecItemId);
            if (recommendationRef.isExisted()) {
                return;
            }

            MeasureRef measureRef = (MeasureRef) recommendationRef;
            NDataModel.Measure measure = measureRef.getMeasure();
            if (functionToMeasureMap.containsKey(measure.getFunction().toString())) {
                log.error("Fail to rewrite RawRecItem({}) for conflicting function ({})", rawRecItem.getId(),
                        measure.getFunction().toString());
                return;
            }
            int maxMeasureId = model.getMaxMeasureId();
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                measureRef.rebuild(userDefinedRecNameMap.get(negRecItemId));
                measure = measureRef.getMeasure();
                measure.setId(++maxMeasureId);
                recManagerV2.checkMeasureName(model, measure);
            } else {
                measure.setId(++maxMeasureId);
            }
            model.getAllMeasures().add(measure);
            measures.put(negRecItemId, measure);
            measures.put(measure.getId(), measure);
            functionToMeasureMap.put(measure.getFunction().toString(), measure);
            logWriteProperty(rawRecItem, measure);
        }

        private void writeDimensionToModel(RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> dimensionRefs = recommendation.getDimensionRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef dimensionRef = dimensionRefs.get(negRecItemId);
            if (dimensionRef.isExisted()) {
                return;
            }
            DimensionRef dimRef = (DimensionRef) dimensionRef;
            NDataModel.NamedColumn column = null;
            if (dimRef.getEntity() instanceof ModelColumnRef) {
                ModelColumnRef columnRef = (ModelColumnRef) dimensionRef.getEntity();
                column = columnRef.getColumn();
            } else if (dimRef.getEntity() instanceof CCRef) {
                CCRef ccRef = (CCRef) dimensionRef.getEntity();
                column = columns.get(ccRef.getId());
            }
            Preconditions.checkArgument(column != null,
                    "Dimension can only depend on a computed column or an existing column");
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                column.setName(userDefinedRecNameMap.get(negRecItemId));
            }
            column.setStatus(NDataModel.ColumnStatus.DIMENSION);
            recManagerV2.checkDimensionName(columns);
            dimensions.putIfAbsent(negRecItemId, column);
            columns.get(column.getId()).setStatus(column.getStatus());
            columns.get(column.getId()).setName(column.getName());
            logWriteProperty(rawRecItem, column);
        }

        private void writeCCToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> ccRefs = recommendation.getCcRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef recommendationRef = ccRefs.get(negRecItemId);
            if (recommendationRef.isExisted()) {
                return;
            }
            CCRef ccRef = (CCRef) recommendationRef;
            ComputedColumnDesc cc = ccRef.getCc();
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                ccRef.rebuild(userDefinedRecNameMap.get(negRecItemId));
                cc = ccRef.getCc();
                recManagerV2.checkCCName(model, cc);
            }
            int lastColumnId = model.getMaxColumnId();
            NDataModel.NamedColumn columnInModel = new NDataModel.NamedColumn();
            columnInModel.setId(++lastColumnId);
            columnInModel.setName(cc.getTableAlias() + "_" + cc.getColumnName());
            columnInModel.setAliasDotColumn(cc.getTableAlias() + "." + cc.getColumnName());
            columnInModel.setStatus(NDataModel.ColumnStatus.EXIST);
            model.getAllNamedColumns().add(columnInModel);
            model.getComputedColumnDescs().add(cc);
            columns.put(negRecItemId, columnInModel);
            columns.put(lastColumnId, columnInModel);
            logWriteProperty(rawRecItem, columnInModel);
        }

        private List<Long> rewriteIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return Lists.newArrayList();
            }
            List<Long> layoutIds = Lists.newArrayList();
            logBeginRewrite("augment IndexPlan");
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            indexMgr.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (!rawRecItem.isAddLayoutRec()) {
                        continue;
                    }
                    LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
                    List<Integer> colOrder = layout.getColOrder();
                    List<Integer> shardBy = Lists.newArrayList(layout.getShardByColumns());
                    List<Integer> sortBy = Lists.newArrayList(layout.getSortByColumns());
                    List<Integer> partitionBy = Lists.newArrayList(layout.getPartitionByColumns());

                    List<Integer> nColOrder = translateToRealIds(colOrder, "ColOrder");
                    List<Integer> nShardBy = translateToRealIds(shardBy, "ShardByColumns");
                    List<Integer> nSortBy = translateToRealIds(sortBy, "SortByColumns");
                    List<Integer> nPartitionBy = translateToRealIds(partitionBy, "PartitionByColumns");

                    if (Sets.newHashSet(nColOrder).size() != colOrder.size()) {
                        log.error("Fail to rewrite illegal RawRecItem({})", rawRecItem.getId());
                        continue;
                    }

                    layout.setColOrder(nColOrder);
                    layout.setShardByColumns(nShardBy);
                    layout.setSortByColumns(nSortBy);
                    layout.setPartitionByColumns(nPartitionBy);
                    updateHandler.add(layout, rawRecItem.isAgg());

                    log.info("RawRecItem({}) rewrite colOrder({}) to ({})", rawRecItem.getId(), colOrder, nColOrder);
                    log.info("RawRecItem({}) rewrite shardBy({}) to ({})", rawRecItem.getId(), shardBy, nShardBy);
                    log.info("RawRecItem({}) rewrite sortBy({}) to ({})", rawRecItem.getId(), sortBy, nSortBy);
                    log.info("RawRecItem({}) rewrite partitionBy({}) to ({})", rawRecItem.getId(), partitionBy,
                            nPartitionBy);
                }
                updateHandler.complete();
                layoutIds.addAll(updateHandler.getAddedLayouts());
            });
            logFinishRewrite("augment IndexPlan");
            return layoutIds;
        }

        private List<Integer> translateToRealIds(List<Integer> virtualIds, String layoutPropType) {
            List<Integer> realIds = Lists.newArrayList();
            virtualIds.forEach(virtualId -> {
                int realId;
                if (recommendation.getDimensionRefs().containsKey(virtualId)) {
                    int refId = recommendation.getDimensionRefs().get(virtualId).getId();
                    realId = dimensions.get(refId).getId();
                } else if (recommendation.getMeasureRefs().containsKey(virtualId)) {
                    int refId = recommendation.getMeasureRefs().get(virtualId).getId();
                    realId = measures.get(refId).getId();
                } else if (recommendation.getColumnRefs().containsKey(virtualId)) {
                    realId = recommendation.getColumnRefs().get(virtualId).getId();
                } else {
                    String translateErrorMsg = String.format(Locale.ROOT,
                            "virtual id(%s) in %s(%s) cannot map to real id in model(%s/%s)", //
                            virtualId, layoutPropType, virtualIds.toString(), recommendation.getProject(),
                            recommendation.getUuid());
                    throw new IllegalStateException(translateErrorMsg);
                }
                realIds.add(realId);
            });
            return realIds;
        }

        private List<Long> reduceIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return Lists.newArrayList();
            }
            logBeginRewrite("reduce IndexPlan");
            List<Long> removedLayoutIds = Lists.newArrayList();
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            indexMgr.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (!rawRecItem.isRemoveLayoutRec()) {
                        continue;
                    }
                    LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
                    updateHandler.remove(layout, rawRecItem.isAgg(), layout.isManual());
                    removedLayoutIds.add(layout.getId());
                }
                updateHandler.complete();
            });
            logFinishRewrite("reduce IndexPlan");
            return removedLayoutIds;
        }

        private void logBeginRewrite(String rewriteInfo) {
            log.info("Start to rewrite RawRecItems to {}({}/{})", rewriteInfo, recommendation.getProject(),
                    recommendation.getUuid());
        }

        private void logFinishRewrite(String rewrite) {
            log.info("Rewrite RawRecItems to {}({}/{}) successfully", rewrite, recommendation.getProject(),
                    recommendation.getUuid());
        }

        private void logWriteProperty(RawRecItem recItem, Object obj) {
            if (obj instanceof NDataModel.NamedColumn) {
                NDataModel.NamedColumn column = (NDataModel.NamedColumn) obj;
                log.info("Write RawRecItem({}) to model as Column with id({}), name({}), isDimension({})", //
                        recItem.getId(), column.getId(), column.getName(), column.isDimension());
            } else if (obj instanceof NDataModel.Measure) {
                NDataModel.Measure measure = (NDataModel.Measure) obj;
                log.info("Write RawRecItem({}) to model as Measure with id({}), name({}) ", //
                        recItem.getId(), measure.getId(), measure.getName());
            }
        }
    }

    public void approve(String project, OptRecRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        String modelId = request.getModelId();
        Map<Integer, String> userDefinedRecNameMap = request.getNames();
        RecApproveContext approveContext = new RecApproveContext(project, modelId, userDefinedRecNameMap);
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
        updateRecommendationCount(project, modelId);
    }

    /**
     * approve all recommendations in specified models
     */
    public List<RecToIndexResponse> batchApprove(String project, List<String> modelIds, String recActionType) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(modelIds)) {
            return Lists.newArrayList();
        }
        modelIds.forEach(modelId -> modelService.checkModelPermission(project, modelId));
        List<RecToIndexResponse> responseList = Lists.newArrayList();
        List<NDataflow> dataflowList = getDataflowManager(project).listAllDataflows();
        for (NDataflow df : dataflowList) {
            if (df.getStatus() != RealizationStatusEnum.ONLINE || df.getModel().isBroken()) {
                continue;
            }
            NDataModel model = df.getModel();
            if (modelIds.contains(model.getUuid())) {
                RecToIndexResponse response = approveAllRecItems(project, model.getUuid(), model.getAlias(),
                        recActionType);
                responseList.add(response);
            }
        }
        return responseList;
    }

    /**
     * approve by project & approveType
     */
    public List<RecToIndexResponse> batchApprove(String project, String recActionType) {
        aclEvaluate.checkProjectWritePermission(project);
        List<RecToIndexResponse> responseList = Lists.newArrayList();
        List<NDataflow> dataflowList = getDataflowManager(project).listAllDataflows();
        for (NDataflow df : dataflowList) {
            if (df.getStatus() != RealizationStatusEnum.ONLINE || df.getModel().isBroken()) {
                continue;
            }
            NDataModel model = df.getModel();
            RecToIndexResponse response = approveAllRecItems(project, model.getUuid(), model.getAlias(), recActionType);
            responseList.add(response);
        }
        return responseList;
    }

    private RecToIndexResponse approveAllRecItems(String project, String modelId, String modelAlias,
            String recActionType) {
        aclEvaluate.checkProjectWritePermission(project);
        RecApproveContext approveContext = new RecApproveContext(project, modelId, Maps.newHashMap());
        OptRecRequest request = new OptRecRequest();
        request.setProject(project);
        request.setModelId(modelId);
        OptRecV2 recommendation = approveContext.getRecommendation();
        List<Integer> recItemsToAddLayout = Lists.newArrayList();
        List<Integer> recItemsToRemoveLayout = Lists.newArrayList();
        recommendation.getAdditionalLayoutRefs().forEach((key, value) -> recItemsToAddLayout.add(-value.getId()));
        recommendation.getRemovalLayoutRefs().forEach((key, value) -> recItemsToRemoveLayout.add(-value.getId()));
        if (recActionType.equalsIgnoreCase(RecActionType.ALL.name())) {
            request.setRecItemsToAddLayout(recItemsToAddLayout);
            request.setRecItemsToRemoveLayout(recItemsToRemoveLayout);
        } else if (recActionType.equalsIgnoreCase(RecActionType.ADD_INDEX.name())) {
            request.setRecItemsToAddLayout(recItemsToAddLayout);
            request.setRecItemsToRemoveLayout(Lists.newArrayList());
        } else if (recActionType.equalsIgnoreCase(RecActionType.REMOVE_INDEX.name())) {
            request.setRecItemsToAddLayout(Lists.newArrayList());
            request.setRecItemsToRemoveLayout(recItemsToRemoveLayout);
        } else {
            throw new KylinException(UNSUPPORTED_REC_OPERATION_TYPE, OptRecService.OPERATION_ERROR_MSG);
        }
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
        updateRecommendationCount(project, modelId);

        RecToIndexResponse response = new RecToIndexResponse();
        response.setModelId(modelId);
        response.setModelAlias(modelAlias);
        response.setAddedIndexes(approveContext.addedLayoutIdList);
        response.setRemovedIndexes(approveContext.removedLayoutIdList);
        return response;
    }

    private void approveRecItemsToRemoveLayout(OptRecRequest request, RecApproveContext approveContext) {
        List<Integer> recItemsToRemoveLayout = request.getRecItemsToRemoveLayout();
        List<RawRecItem> recItems = approveContext.approveRawRecItems(recItemsToRemoveLayout, false);
        updateStatesOfApprovedRecItems(request.getProject(), recItems);
    }

    private void approveRecItemsToAddLayout(OptRecRequest request, RecApproveContext approveContext) {
        List<Integer> recItemsToAddLayout = request.getRecItemsToAddLayout();
        List<RawRecItem> recItems = approveContext.approveRawRecItems(recItemsToAddLayout, true);
        updateStatesOfApprovedRecItems(request.getProject(), recItems);
    }

    private void updateStatesOfApprovedRecItems(String project, List<RawRecItem> recItems) {
        List<Integer> nonAppliedItemIds = Lists.newArrayList();
        recItems.forEach(recItem -> {
            if (recItem.getState() == RawRecItem.RawRecState.APPLIED) {
                return;
            }
            nonAppliedItemIds.add(recItem.getId());
        });
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.applyByIds(nonAppliedItemIds);
    }

    public void discard(String project, OptRecRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(request.getModelId());
        RawRecManager rawManager = RawRecManager.getInstance(project);
        Set<Integer> allToHandle = Sets.newHashSet();
        allToHandle.addAll(request.getRecItemsToAddLayout());
        allToHandle.addAll(request.getRecItemsToRemoveLayout());
        rawManager.discardByIds(intersection(optRecV2.getRawIds(), Lists.newArrayList(allToHandle)));
        updateRecommendationCount(project, request.getModelId());
    }

    public void clean(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecManagerV2 managerV2 = OptRecManagerV2.getInstance(project);
        managerV2.discardAll(modelId);
        updateRecommendationCount(project, modelId);
    }

    private static OptRecDepResponse convert(RecommendationRef ref) {
        OptRecDepResponse response = new OptRecDepResponse();
        response.setVersion(V2);
        response.setContent(ref.getContent());
        response.setName(ref.getName());
        response.setAdd(!ref.isExisted());
        response.setCrossModel(ref.isCrossModel());
        response.setItemId(ref.getId());
        return response;
    }

    public OptRecDetailResponse validateSelectedRecItems(String project, String modelId,
            List<Integer> recListOfAddLayouts, List<Integer> recListOfRemoveLayouts) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        List<Integer> layoutRecsToAdd = validate(recListOfAddLayouts, optRecV2, detailResponse, true);
        List<Integer> layoutRecsToRemove = validate(recListOfRemoveLayouts, optRecV2, detailResponse, false);
        detailResponse.setRecItemsToAddLayout(layoutRecsToAdd);
        detailResponse.setRecItemsToRemoveLayout(layoutRecsToRemove);
        return detailResponse;
    }

    private List<Integer> validate(List<Integer> layoutRecList, OptRecV2 optRecV2, OptRecDetailResponse detailResponse,
            boolean isAdd) {
        List<Integer> healthyList = Lists.newArrayList();
        Set<OptRecDepResponse> dimensionRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> measureRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> ccRefResponse = Sets.newHashSet();
        layoutRecList.forEach(recItemId -> {
            LayoutRef layoutRef = checkRecItemIsValidAndReturn(optRecV2, recItemId, isAdd);
            layoutRef.getDependencies().forEach(ref -> {
                if (ref instanceof DimensionRef) {
                    dimensionRefResponse.add(OptRecService.convert(ref));
                }
                if (ref instanceof MeasureRef) {
                    measureRefResponse.add(OptRecService.convert(ref));
                }

                for (RecommendationRef innerRef : ref.getDependencies()) {
                    if (innerRef instanceof CCRef) {
                        ccRefResponse.add(OptRecService.convert(innerRef));
                    }
                }
            });
            healthyList.add(recItemId);
        });

        detailResponse.getDimensionItems().addAll(dimensionRefResponse);
        detailResponse.getMeasureItems().addAll(measureRefResponse);
        detailResponse.getCcItems().addAll(ccRefResponse);
        return healthyList;
    }

    public OptRecDetailResponse getSingleOptRecDetail(String project, String modelId, int recItemId, boolean isAdd) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        List<Integer> validList = validate(Lists.newArrayList(recItemId), optRecV2, detailResponse, isAdd);
        if (isAdd) {
            detailResponse.getRecItemsToAddLayout().addAll(validList);
        } else {
            detailResponse.getRecItemsToRemoveLayout().addAll(validList);
        }
        return detailResponse;
    }

    private static LayoutRef checkRecItemIsValidAndReturn(OptRecV2 optRecV2, int recItemId, boolean isAdd) {
        Set<Integer> allRecItemIds = Sets.newHashSet(optRecV2.getRawIds());
        Set<Integer> brokenRefIds = optRecV2.getBrokenLayoutRefIds();
        if (!allRecItemIds.contains(recItemId) || brokenRefIds.contains(recItemId)) {
            log.info("all recommendation ids {}, broken ref ids {}", allRecItemIds, brokenRefIds);
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getREC_LIST_OUT_OF_DATE());
        }
        Map<Integer, LayoutRef> layoutRefs = isAdd //
                ? optRecV2.getAdditionalLayoutRefs() //
                : optRecV2.getRemovalLayoutRefs();
        LayoutRef layoutRef = layoutRefs.get(-recItemId);
        if (layoutRef == null) {
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getREC_LIST_OUT_OF_DATE());
        }
        return layoutRef;
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId, List<String> recTypeList,
            String key, boolean desc, String orderBy, int offset, int limit) {
        Set<RawRecItem.IndexRecType> userDefinedTypes = Sets.newHashSet();
        recTypeList.forEach(type -> {
            if (RawRecItem.IndexRecType.ADD_TABLE_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.ADD_TABLE_INDEX);
            } else if (RawRecItem.IndexRecType.ADD_AGG_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.ADD_AGG_INDEX);
            } else if (RawRecItem.IndexRecType.REMOVE_AGG_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.REMOVE_AGG_INDEX);
            } else {
                userDefinedTypes.add(RawRecItem.IndexRecType.REMOVE_TABLE_INDEX);
            }
        });

        Set<Integer> brokenRecs = Sets.newHashSet();
        List<OptRecLayoutResponse> recList = getRecLayoutResponses(project, modelId, OptRecService.ALL, brokenRecs);
        if (userDefinedTypes.size() != RawRecItem.IndexRecType.values().length) {
            recList.removeIf(resp -> !userDefinedTypes.isEmpty() && !userDefinedTypes.contains(resp.getType()));
        }
        if (StringUtils.isNotEmpty(orderBy)) {
            recList.sort(propertyComparator(orderBy, !desc));
        }
        OptRecLayoutsResponse response = new OptRecLayoutsResponse();
        response.setLayouts(PagingUtil.cutPage(recList, offset, limit));
        response.setSize(recList.size());
        response.setBrokenRecs(brokenRecs);
        return response;
    }

    /**
     * get recommendations by recommendation type:
     * 1. additional, only fetch recommendations can create new layouts on IndexPlan
     * 2. removal, only fetch recommendations can remove layouts of IndexPlan
     * 3. all, fetch all recommendations
     */
    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId, String recActionType) {
        aclEvaluate.checkProjectReadPermission(project);
        OptRecLayoutsResponse layoutsResponse = new OptRecLayoutsResponse();
        List<OptRecLayoutResponse> responses = getRecLayoutResponses(project, modelId, recActionType,
                layoutsResponse.getBrokenRecs());
        layoutsResponse.getLayouts().addAll(responses);
        layoutsResponse.setSize(layoutsResponse.getLayouts().size());
        return layoutsResponse;
    }

    /**
     * get layout to be approved
     */
    public List<RawRecItem> getRecLayout(OptRecV2 optRecV2, RecActionType recActionType) {
        if (optRecV2 == null) {
            return Collections.emptyList();
        }

        Map<Integer, LayoutRef> layoutRefMap = new HashMap<>();
        switch (recActionType) {
        case ALL:
            layoutRefMap.putAll(optRecV2.getAdditionalLayoutRefs());
            layoutRefMap.putAll(optRecV2.getRemovalLayoutRefs());
            break;
        case ADD_INDEX:
            layoutRefMap.putAll(optRecV2.getAdditionalLayoutRefs());
            break;
        case REMOVE_INDEX:
            layoutRefMap.putAll(optRecV2.getRemovalLayoutRefs());
            break;
        default:
            // do nothing
        }

        return layoutRefMap.entrySet().stream().filter(entry -> {
            val recId = entry.getKey();
            val layoutRef = entry.getValue();
            return !layoutRef.isBroken() && !layoutRef.isExisted() && recId < 0;
        }).map(entry -> {
            val recId = entry.getKey();
            return optRecV2.getRawRecItemMap().get(-recId);
        }).collect(Collectors.toList());
    }

    private List<OptRecLayoutResponse> getRecLayoutResponses(String project, String modelId, String recActionType,
            Set<Integer> brokenRecCollector) {
        aclEvaluate.checkProjectReadPermission(project);
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        if (recActionType.equalsIgnoreCase(RecActionType.ALL.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.ALL));
        } else if (recActionType.equalsIgnoreCase(RecActionType.ADD_INDEX.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.ADD_INDEX));
        } else if (recActionType.equalsIgnoreCase(RecActionType.REMOVE_INDEX.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.REMOVE_INDEX));
        } else {
            throw new KylinException(UNSUPPORTED_REC_OPERATION_TYPE, OptRecService.OPERATION_ERROR_MSG);
        }

        brokenRecCollector.addAll(optRecV2.getBrokenLayoutRefIds());
        return convertToV2RecResponse(project, modelId, rawRecItems);
    }

    /**
     * convert RawRecItem response:
     * if type=Layout, layout will be added to IndexPlan when user approves this RawRecItem
     * if type=REMOVAL_LAYOUT, layout will be removed from IndexPlan when user approves this RawRecItem
     */
    private List<OptRecLayoutResponse> convertToV2RecResponse(String project, String modelId,
            List<RawRecItem> recItems) {
        List<OptRecLayoutResponse> layoutRecResponseList = Lists.newArrayList();

        NDataflowManager dfManager = getDataflowManager(project);
        NDataflow dataflow = dfManager.getDataflow(modelId);
        recItems.forEach(rawRecItem -> {
            boolean isAdd = rawRecItem.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT;
            OptRecLayoutResponse response = new OptRecLayoutResponse();
            response.setId(rawRecItem.getId());
            response.setAdd(isAdd);
            response.setAgg(rawRecItem.isAgg());
            response.setDataSize(-1);
            response.setUsage(rawRecItem.getHitCount());
            response.setType(rawRecItem.getLayoutRecType());
            response.setCreateTime(rawRecItem.getCreateTime());
            response.setLastModified(rawRecItem.getUpdateTime());
            HashMap<String, String> memoInfo = Maps.newHashMap();
            memoInfo.put(OptRecService.RECOMMENDATION_SOURCE, rawRecItem.getRecSource());
            response.setMemoInfo(memoInfo);
            if (!isAdd) {
                long layoutId = RawRecUtil.getLayout(rawRecItem).getId();
                response.setIndexId(layoutId);
                response.setDataSize(dataflow.getByteSize(layoutId));
            }
            layoutRecResponseList.add(response);
        });
        return layoutRecResponseList;
    }

    public void updateRecommendationCount(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            int size = getOptRecLayoutsResponse(project, modelId, OptRecService.ALL).getSize();
            NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(size));
            return null;
        }, project);
    }

    @Override
    public void onUpdate(String project, String modelId) {
        ProjectInstance prjInstance = getProjectManager().getProject(project);
        if (prjInstance.isSemiAutoMode()) {
            getOptRecManagerV2(project).loadOptRecV2(modelId);
        }
    }

    public enum RecActionType {
        ALL, ADD_INDEX, REMOVE_INDEX
    }
}
