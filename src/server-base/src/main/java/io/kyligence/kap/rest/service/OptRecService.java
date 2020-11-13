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

import java.util.HashMap;
import java.util.List;
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

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
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
import io.kyligence.kap.rest.response.OptRecDepResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("optRecService")
public class OptRecService extends BasicService implements ModelUpdateListener {

    public static final int V2 = 2;
    public static final String RECOMMENDATION_SOURCE = "recommendation_source";
    public static final String OPERATION_ERROR_MSG = "The operation types of recommendation includes: add_index, removal_index and both(by default)";

    @Autowired
    public AclEvaluate aclEvaluate;

    private static final class RecApproveContext {
        private final Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        private final Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        private final Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();

        @Getter
        private final OptRecV2 recommendation;
        private final Map<Integer, String> userDefinedRecNameMap;
        private final NDataModelManager modelManager;
        private final NIndexPlanManager indexPlanManager;
        private final OptRecManagerV2 recManagerV2;

        private RecApproveContext(String project, String modelId, Map<Integer, String> userDefinedRecNameMap) {
            this.userDefinedRecNameMap = userDefinedRecNameMap;
            this.recManagerV2 = OptRecManagerV2.getInstance(project);
            this.recommendation = recManagerV2.loadOptRecV2(modelId);
            this.modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            this.indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        }

        public List<RawRecItem> approveRawRecItems(List<Integer> layoutIds, boolean isAdd) {
            layoutIds.forEach(id -> checkRecItemIsValidAndReturn(recommendation, id, isAdd));
            List<RawRecItem> rawRecItems = getAllRelatedRecItems(layoutIds, isAdd);
            if (isAdd) {
                rewriteModel(rawRecItems);
                rewriteIndexPlan(rawRecItems);
            } else {
                shiftLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), rawRecItems);
                reduceIndexPlan(rawRecItems);
            }
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
                List<NDataModel.NamedColumn> existedColumns = copyForWrite.getAllNamedColumns().stream()
                        .filter(NDataModel.NamedColumn::isExist).collect(Collectors.toList());
                List<NDataModel.Measure> existedMeasure = copyForWrite.getAllMeasures().stream()
                        .filter(measure -> !measure.isTomb()).collect(Collectors.toList());
                NDataModel.checkDuplicateColumn(existedColumns);
                NDataModel.checkIdOrderOfColumn(existedColumns);
                NDataModel.checkDuplicateMeasure(existedMeasure);
                NDataModel.checkIdOrderOfMeasure(existedMeasure);
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

        private void rewriteIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return;
            }
            logBeginRewrite("augment IndexPlan");
            indexPlanManager.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
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

                    layout.setColOrder(translateToRealIds(colOrder, "ColOrder"));
                    layout.setShardByColumns(translateToRealIds(shardBy, "ShardByColumns"));
                    layout.setSortByColumns(translateToRealIds(sortBy, "SortByColumns"));
                    layout.setPartitionByColumns(translateToRealIds(partitionBy, "PartitionByColumns"));
                    updateHandler.add(layout, rawRecItem.isAgg());
                }
                updateHandler.complete();
            });
            logFinishRewrite("augment IndexPlan");
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
                    String translateErrorMsg = String.format(
                            "virtual id(%s) in %s(%s) cannot map to real id in model(%s/%s)", //
                            virtualId, layoutPropType, virtualIds.toString(), recommendation.getProject(),
                            recommendation.getUuid());
                    throw new IllegalStateException(translateErrorMsg);
                }
                realIds.add(realId);
            });
            return realIds;
        }

        private void reduceIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return;
            }
            logBeginRewrite("reduce IndexPlan");
            indexPlanManager.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (!rawRecItem.isRemoveLayoutRec()) {
                        continue;
                    }
                    LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
                    updateHandler.remove(layout, rawRecItem.isAgg(), layout.isManual());
                }
                updateHandler.complete();
            });
            logFinishRewrite("reduce IndexPlan");
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

    @Transaction(project = 0)
    public void approve(String project, OptRecRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);
        String modelId = request.getModelId();
        Map<Integer, String> userDefinedRecNameMap = request.getNames();
        RecApproveContext approveContext = new RecApproveContext(project, modelId, userDefinedRecNameMap);
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
    }

    /**
     * approve all recommendations in specified models
     */
    @Transaction(project = 0)
    public void batchApprove(String project, List<String> modelAlias, String recActionType) {
        aclEvaluate.checkProjectOperationPermission(project);
        if (CollectionUtils.isEmpty(modelAlias)) {
            return;
        }
        Set<String> targetModelNames = modelAlias.stream().map(String::toLowerCase).collect(Collectors.toSet());

        List<NDataflow> dataflowList = getDataflowManager(project).listAllDataflows();
        for (NDataflow df : dataflowList) {
            if (df.getStatus() != RealizationStatusEnum.ONLINE || df.getModel().isBroken()) {
                continue;
            }
            NDataModel model = df.getModel();
            if (targetModelNames.contains(model.getAlias())) {
                approveAllRecItems(project, model.getUuid(), recActionType);
            }
        }
    }

    /**
     * approve by project & approveType
     */
    @Transaction(project = 0)
    public void batchApprove(String project, String recActionType) {
        aclEvaluate.checkProjectOperationPermission(project);
        List<NDataflow> dataflowList = getDataflowManager(project).listAllDataflows();
        for (NDataflow df : dataflowList) {
            if (df.getStatus() != RealizationStatusEnum.ONLINE || df.getModel().isBroken()) {
                continue;
            }
            NDataModel model = df.getModel();
            approveAllRecItems(project, model.getUuid(), recActionType);
        }
    }

    private void approveAllRecItems(String project, String modelId, String recActionType) {
        aclEvaluate.checkProjectOperationPermission(project);
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
        UnitOfWork.get().doAfterUpdate(() -> {
            List<Integer> nonAppliedItemIds = Lists.newArrayList();
            recItems.forEach(recItem -> {
                if (recItem.getState() == RawRecItem.RawRecState.APPLIED) {
                    return;
                }
                nonAppliedItemIds.add(recItem.getId());
            });
            RawRecManager rawManager = RawRecManager.getInstance(project);
            rawManager.applyByIds(nonAppliedItemIds);
        });
    }

    @Transaction(project = 0)
    public void discard(String project, OptRecRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(request.getModelId());
        UnitOfWork.get().doAfterUpdate(() -> {
            RawRecManager rawManager = RawRecManager.getInstance(project);
            Set<Integer> allToHandle = Sets.newHashSet();
            allToHandle.addAll(request.getRecItemsToAddLayout());
            allToHandle.addAll(request.getRecItemsToRemoveLayout());
            rawManager.discardByIds(intersection(optRecV2.getRawIds(), Lists.newArrayList(allToHandle)));
        });
    }

    @Transaction(project = 0)
    public void clean(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecManagerV2 managerV2 = OptRecManagerV2.getInstance(project);
        managerV2.discardAll(modelId);
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
        if (isAdd) {
            detailResponse.getDimensionItems().addAll(dimensionRefResponse);
            detailResponse.getMeasureItems().addAll(measureRefResponse);
            detailResponse.getCcItems().addAll(ccRefResponse);
        }
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
            log.warn("all recommendation ids {}, broken ref ids {}", allRecItemIds, brokenRefIds);
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

        List<OptRecLayoutResponse> allRecRespList = getRecLayoutResponses(project, modelId, RecActionType.ALL.name());
        List<OptRecLayoutResponse> filterOutRecList = recTypeList.isEmpty()
                || userDefinedTypes.size() == RawRecItem.IndexRecType.values().length //
                        ? allRecRespList
                        : allRecRespList.stream().filter(resp -> userDefinedTypes.contains(resp.getType()))
                                .collect(Collectors.toList());
        if (StringUtils.isNotEmpty(orderBy)) {
            filterOutRecList.sort(propertyComparator(orderBy, !desc));
        }
        OptRecLayoutsResponse response = new OptRecLayoutsResponse();
        response.setLayouts(PagingUtil.cutPage(filterOutRecList, offset, limit));
        response.setSize(filterOutRecList.size());
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
        layoutsResponse.getLayouts().addAll(getRecLayoutResponses(project, modelId, recActionType));
        layoutsResponse.setSize(layoutsResponse.getLayouts().size());
        return layoutsResponse;
    }

    private List<OptRecLayoutResponse> getRecLayoutResponses(String project, String modelId, String recActionType) {
        aclEvaluate.checkProjectReadPermission(project);
        List<OptRecLayoutResponse> recLayoutResponses = Lists.newArrayList();
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        if (recActionType.equalsIgnoreCase(RecActionType.ALL.name())) {
            recLayoutResponses.addAll(convertToV2RecResponse(optRecV2, true));
            recLayoutResponses.addAll(convertToV2RecResponse(optRecV2, false));
        } else if (recActionType.equalsIgnoreCase(RecActionType.ADD_INDEX.name())) {
            recLayoutResponses.addAll(convertToV2RecResponse(optRecV2, true));
        } else if (recActionType.equalsIgnoreCase(RecActionType.REMOVE_INDEX.name())) {
            recLayoutResponses.addAll(convertToV2RecResponse(optRecV2, false));
        } else {
            throw new KylinException(UNSUPPORTED_REC_OPERATION_TYPE, OptRecService.OPERATION_ERROR_MSG);
        }
        return recLayoutResponses;
    }

    /**
     * convert RawRecItem response:
     * if type=Layout, layout will be added to IndexPlan when user approves this RawRecItem
     * if type=REMOVAL_LAYOUT, layout will be removed from IndexPlan when user approves this RawRecItem
     */
    private List<OptRecLayoutResponse> convertToV2RecResponse(OptRecV2 optRecV2, boolean isAdd) {
        List<OptRecLayoutResponse> layoutRecResponseList = Lists.newArrayList();
        if (optRecV2 == null) {
            return layoutRecResponseList;
        }

        NDataflowManager dfManager = NDataflowManager.getInstance(optRecV2.getConfig(), optRecV2.getProject());
        NDataflow dataflow = dfManager.getDataflow(optRecV2.getUuid());
        Map<Integer, LayoutRef> layoutRefs = isAdd //
                ? optRecV2.getAdditionalLayoutRefs() //
                : optRecV2.getRemovalLayoutRefs();
        layoutRefs.forEach((recId, layoutRef) -> {
            if (!layoutRef.isBroken() && !layoutRef.isExisted() && recId < 0) {
                RawRecItem rawRecItem = optRecV2.getRawRecItemMap().get(-recId);
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
            }
        });
        return layoutRecResponseList;
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
