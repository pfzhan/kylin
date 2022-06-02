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
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_REC_OPERATION_TYPE;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.model.FuzzyKeySearcher;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.logging.SetLogCategory;
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
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
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
import io.kyligence.kap.rest.response.OptRecResponse;
import lombok.Getter;

@Component("optRecService")
public class OptRecService extends BasicService {
    private static final Logger log = LoggerFactory.getLogger("smart");
    public static final int V2 = 2;
    public static final String RECOMMENDATION_SOURCE = "recommendation_source";
    public static final String OPERATION_ERROR_MSG = "The operation types of recommendation includes: add_index, removal_index and all(by default)";
    public static final String ALL = OptRecService.RecActionType.ALL.name();

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    private static final class RecApproveContext {
        private final Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        private final Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        private final Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();
        private final List<Long> addedLayoutIdList = Lists.newArrayList();
        private final List<Long> removedLayoutIdList = Lists.newArrayList();
        private final Map<String, NDataModel.Measure> functionToMeasureMap = Maps.newHashMap();
        private Set<Integer> abnormalRecIds = Sets.newHashSet();

        @Getter
        private final OptRecV2 recommendation;
        private final Map<Integer, String> userDefinedRecNameMap;
        private final String project;

        private RecApproveContext(String project, String modelId, Map<Integer, String> userDefinedRecNameMap) {
            this.project = project;
            this.userDefinedRecNameMap = userDefinedRecNameMap;
            this.recommendation = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
            RecNameChecker.checkCCRecConflictWithColumn(recommendation, project, userDefinedRecNameMap);
            RecNameChecker.checkUserDefinedRecNames(recommendation, project, userDefinedRecNameMap);
        }

        public List<RawRecItem> approveRawRecItems(List<Integer> recItemIds, boolean isAdd) {
            recItemIds.forEach(id -> checkRecItemIsValidAndReturn(recommendation, id, isAdd));
            List<RawRecItem> rawRecItems = getAllRelatedRecItems(recItemIds, isAdd);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                if (isAdd) {
                    rawRecItems.sort(Comparator.comparing(RawRecItem::getId));
                    abnormalRecIds = reduceDupCCRecItems(rawRecItems, abnormalRecIds);
                    rewriteModel(rawRecItems);
                    Map<Long, RawRecItem> addedLayouts = rewriteIndexPlan(rawRecItems);
                    addedLayouts = checkAndRemoveDirtyLayouts(addedLayouts);
                    addedLayoutIdList.addAll(addedLayouts.keySet());
                    addLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), addedLayouts);
                } else {
                    shiftLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), rawRecItems);
                    List<Long> removedLayouts = reduceIndexPlan(rawRecItems);
                    removedLayoutIdList.addAll(removedLayouts);
                }
                return null;
            }, project);
            return rawRecItems;
        }

        private void addLayoutHitCount(String project, String modelUuid, Map<Long, RawRecItem> rawRecItems) {
            if (MapUtils.isEmpty(rawRecItems)) {
                return;
            }
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            dfMgr.updateDataflow(modelUuid, copyForWrite -> {
                Map<Long, FrequencyMap> layoutHitCount = copyForWrite.getLayoutHitCount();
                rawRecItems.forEach((id, rawRecItem) -> {
                    if (rawRecItem.getLayoutMetric() != null) {
                        layoutHitCount.put(id, rawRecItem.getLayoutMetric().getFrequencyMap());
                    }
                });
            });
        }

        private Map<Long, RawRecItem> checkAndRemoveDirtyLayouts(Map<Long, RawRecItem> addedLayouts) {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());
            Set<Integer> queryScopes = Sets.newHashSet();
            model.getEffectiveDimensions().forEach((id, col) -> queryScopes.add(id));
            model.getEffectiveMeasures().forEach((id, col) -> queryScopes.add(id));

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(recommendation.getUuid());
            Map<Long, LayoutEntity> allLayoutsMap = indexPlan.getAllLayoutsMap();

            return addedLayouts.entrySet().stream().filter(entry -> {
                long layoutId = entry.getKey();
                return allLayoutsMap.containsKey(layoutId)
                        && queryScopes.containsAll(allLayoutsMap.get(layoutId).getColOrder());
            }).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
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
                prepareColsMeasData(copyForWrite);

                for (RawRecItem rawRecItem : recItems) {
                    if (isAbnormal(rawRecItem)) {
                        abnormalRecIds.add(rawRecItem.getId());
                        continue;
                    }
                    switch (rawRecItem.getType()) {
                    case DIMENSION:
                        writeDimensionToModel(copyForWrite, rawRecItem);
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
                try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                    log.info(copyForWrite.getUuid());
                }
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

        private void prepareColsMeasData(NDataModel copyForWrite) {
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
        }

        private Set<Integer> reduceDupCCRecItems(List<RawRecItem> recItems, Set<Integer> abnormalRecIds) {
            List<RawRecItem> ccRecItems = recItems.stream()
                    .filter(rawRecItem -> rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN)
                    .collect(Collectors.toList());
            if (ccRecItems.isEmpty()) {
                return abnormalRecIds;
            }

            HashMap<Integer, Integer> dupCCRecItemMap = Maps.newHashMap();
            int resId = ccRecItems.get(0).getId();
            for (int i = 1; i < ccRecItems.size(); i++) {
                RawRecItem curRecItem = ccRecItems.get(i);
                RawRecItem prevRecItem = ccRecItems.get(i - 1);
                String expr1 = ((CCRecItemV2) curRecItem.getRecEntity()).getCc().getInnerExpression();
                String expr2 = ((CCRecItemV2) prevRecItem.getRecEntity()).getCc().getInnerExpression();
                if (expr1.equalsIgnoreCase(expr2)) {
                    int curRecItemId = curRecItem.getId();
                    dupCCRecItemMap.put(curRecItemId, resId);
                    abnormalRecIds.add(curRecItemId);
                } else {
                    resId = curRecItem.getId();
                }
            }

            return abnormalRecIds;
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
                try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                    log.error("Fail to rewrite RawRecItem({}) for conflicting function ({})", rawRecItem.getId(),
                            measure.getFunction().toString());
                }
                return;
            }
            int maxMeasureId = model.getMaxMeasureId();
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                measureRef.rebuild(userDefinedRecNameMap.get(negRecItemId));
            } else {
                measureRef.rebuild(measure.getName());
            }
            measure = measureRef.getMeasure();
            measure.setId(++maxMeasureId);
            model.getAllMeasures().add(measure);
            measures.put(negRecItemId, measure);
            measures.put(measure.getId(), measure);
            functionToMeasureMap.put(measure.getFunction().toString(), measure);
            logWriteProperty(rawRecItem, measure);
        }

        private void writeDimensionToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, NDataModel.NamedColumn> mapIdToColumn = model.getAllNamedColumns().stream()
                    .collect(Collectors.toMap(NDataModel.NamedColumn::getId, Function.identity()));
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
            mapIdToColumn.get(column.getId()).setName(column.getName());
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

        private Map<Long, RawRecItem> rewriteIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return Maps.newHashMap();
            }
            List<Long> layoutIds = Lists.newArrayList();
            logBeginRewrite("augment IndexPlan");
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDesc(recommendation.getUuid());

            HashMap<Long, RawRecItem> approvedLayouts = Maps.newHashMap();
            indexMgr.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (isAbnormal(rawRecItem)) {
                        abnormalRecIds.add(rawRecItem.getId());
                        continue;
                    }
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

                    if (isInvalidColId(nColOrder, model) || isInvalidColId(nShardBy, model)
                            || isInvalidColId(nSortBy, model) || isInvalidColId(nPartitionBy, model)
                            || (Sets.newHashSet(nColOrder).size() != colOrder.size())) {
                        try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                            log.error("Fail to rewrite illegal RawRecItem({})", rawRecItem.getId());
                        }
                        continue;
                    }

                    layout.setColOrder(nColOrder);
                    layout.setShardByColumns(nShardBy);
                    layout.setPartitionByColumns(nPartitionBy);
                    updateHandler.add(layout, rawRecItem.isAgg());
                    try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                        approvedLayouts.put(layout.getId(), rawRecItem);
                        log.info("RawRecItem({}) rewrite colOrder({}) to ({})", rawRecItem.getId(), colOrder,
                                nColOrder);
                        log.info("RawRecItem({}) rewrite shardBy({}) to ({})", rawRecItem.getId(), shardBy, nShardBy);
                        log.info("RawRecItem({}) rewrite sortBy({}) to ({})", rawRecItem.getId(), sortBy, nSortBy);
                        log.info("RawRecItem({}) rewrite partitionBy({}) to ({})", rawRecItem.getId(), partitionBy,
                                nPartitionBy);
                    }
                }
                updateHandler.complete();
                layoutIds.addAll(updateHandler.getAddedLayouts());
            });
            logFinishRewrite("augment IndexPlan");

            return layoutIds.stream().filter(id -> approvedLayouts.containsKey(id))
                    .collect(Collectors.toMap(Function.identity(), id -> approvedLayouts.get(id)));
        }

        private boolean isAbnormal(RawRecItem rawRecItem) {
            return Arrays.stream(rawRecItem.getDependIDs()).anyMatch(id -> abnormalRecIds.contains(-id))
                    || abnormalRecIds.contains(rawRecItem.getId());
        }

        private boolean isInvalidColId(List<Integer> cols, NDataModel model) {
            for (Integer colId : cols) {
                if (!model.getEffectiveCols().containsKey(colId) && !model.getEffectiveMeasures().containsKey(colId)) {
                    return true;
                }
            }
            return false;
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
            try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                log.info("Start to rewrite RawRecItems to {}({}/{})", rewriteInfo, recommendation.getProject(),
                        recommendation.getUuid());
            }
        }

        private void logFinishRewrite(String rewrite) {
            try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                log.info("Rewrite RawRecItems to {}({}/{}) successfully", rewrite, recommendation.getProject(),
                        recommendation.getUuid());

            }
        }

        private void logWriteProperty(RawRecItem recItem, Object obj) {
            if (obj instanceof NDataModel.NamedColumn) {
                NDataModel.NamedColumn column = (NDataModel.NamedColumn) obj;
                try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                    log.info("Write RawRecItem({}) to model as Column with id({}), name({}), isDimension({})", //
                            recItem.getId(), column.getId(), column.getName(), column.isDimension());
                }
            } else if (obj instanceof NDataModel.Measure) {
                NDataModel.Measure measure = (NDataModel.Measure) obj;
                try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                    log.info("Write RawRecItem({}) to model as Measure with id({}), name({}) ", //
                            recItem.getId(), measure.getId(), measure.getName());
                }
            }
        }
    }

    static class RecNameChecker {

        /**
         * check whether computed column conflict with column of fact table
         */
        private static void checkCCRecConflictWithColumn(OptRecV2 recommendation, String project,
                Map<Integer, String> userDefinedRecNameMap) {
            Map<String, Set<Integer>> checkedTableColumnMap = Maps.newHashMap();

            userDefinedRecNameMap.forEach((id, name) -> {
                if (id >= 0) {
                    return;
                }
                RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
                if (rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                    checkedTableColumnMap.putIfAbsent(name.toUpperCase(Locale.ROOT), Sets.newHashSet());
                    checkedTableColumnMap.get(name.toUpperCase(Locale.ROOT)).add(id);
                }
            });

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());

            String factTableName = model.getRootFactTableName().split("\\.").length < 2 //
                    ? model.getRootFactTableName() //
                    : model.getRootFactTableName().split("\\.")[1];

            model.getAllNamedColumns().forEach(column -> {
                if (!column.isExist()) {
                    return;
                }

                String[] tableAndColumn = column.getAliasDotColumn().split("\\.");
                if (!tableAndColumn[0].equalsIgnoreCase(factTableName)) {
                    return;
                }
                if (checkedTableColumnMap.containsKey(tableAndColumn[1].toUpperCase(Locale.ROOT))) {
                    checkedTableColumnMap.get(tableAndColumn[1]).add(column.getId());
                }

            });

            checkedTableColumnMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            if (!checkedTableColumnMap.isEmpty()) {
                throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                        MsgPicker.getMsg().getAliasConflictOfApprovingRecommendation() + "\n"
                                + JsonUtil.writeValueAsStringQuietly(checkedTableColumnMap));
            }
        }

        /**
         * check user defined name conflict with existing dimension & measure
         */
        private static void checkUserDefinedRecNames(OptRecV2 recommendation, String project,
                Map<Integer, String> userDefinedRecNameMap) {
            Map<String, Set<Integer>> checkedDimensionMap = Maps.newHashMap();
            Map<String, Set<Integer>> checkedMeasureMap = Maps.newHashMap();
            Map<String, Set<Integer>> checkedCCMap = Maps.newHashMap();

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());
            model.getAllNamedColumns().forEach(column -> {
                if (column.isDimension()) {
                    checkedDimensionMap.putIfAbsent(column.getName(), Sets.newHashSet());
                    checkedDimensionMap.get(column.getName()).add(column.getId());
                }
            });
            model.getAllMeasures().forEach(measure -> {
                if (measure.isTomb()) {
                    return;
                }
                checkedMeasureMap.putIfAbsent(measure.getName(), Sets.newHashSet());
                checkedMeasureMap.get(measure.getName()).add(measure.getId());
            });

            userDefinedRecNameMap.forEach((id, name) -> {
                if (id >= 0) {
                    return;
                }
                RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
                if (rawRecItem.getType() == RawRecItem.RawRecType.DIMENSION) {
                    checkedDimensionMap.putIfAbsent(name, Sets.newHashSet());
                    Set<Integer> idSet = checkedDimensionMap.get(name);
                    idSet.remove(rawRecItem.getDependIDs()[0]);
                    idSet.add(id);
                }
                if (rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                    checkedCCMap.putIfAbsent(name, Sets.newHashSet());
                    Set<Integer> idSet = checkedCCMap.get(name);
                    idSet.add(id);
                }
                if (rawRecItem.getType() == RawRecItem.RawRecType.MEASURE) {
                    checkedMeasureMap.putIfAbsent(name, Sets.newHashSet());
                    checkedMeasureMap.get(name).add(id);
                }
            });
            checkedDimensionMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            checkedMeasureMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            checkedCCMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            Map<String, Set<Integer>> conflictMap = Maps.newHashMap();
            checkedDimensionMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            checkedMeasureMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            checkedCCMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            conflictMap.entrySet().removeIf(entry -> entry.getValue().stream().allMatch(id -> id >= 0));
            if (!conflictMap.isEmpty()) {
                throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                        MsgPicker.getMsg().getAliasConflictOfApprovingRecommendation() + "\n"
                                + JsonUtil.writeValueAsStringQuietly(conflictMap));
            }
        }
    }

    public OptRecResponse approve(String project, OptRecRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        String modelId = request.getModelId();

        if (!FusionIndexService.checkUpdateIndexEnabled(project, modelId)) {
            throw new KylinException(STREAMING_INDEX_UPDATE_DISABLE, MsgPicker.getMsg().getStreamingIndexesApprove());
        }

        BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(
                getManager(NDataModelManager.class, project).getDataModelDesc(modelId), false);
        Map<Integer, String> userDefinedRecNameMap = request.getNames();
        RecApproveContext approveContext = new RecApproveContext(project, modelId, userDefinedRecNameMap);
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
        updateRecommendationCount(project, modelId);

        OptRecResponse response = new OptRecResponse();
        response.setProject(request.getProject());
        response.setModelId(request.getModelId());
        response.setAddedLayouts(approveContext.addedLayoutIdList);
        response.setRemovedLayouts(approveContext.removedLayoutIdList);
        response.setBaseIndexInfo(baseIndexUpdater.update(indexPlanService));

        return response;
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
        List<NDataflow> dataflowList = getManager(NDataflowManager.class, project).listAllDataflows();
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
        List<NDataflow> dataflowList = getManager(NDataflowManager.class, project).listAllDataflows();
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
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(model, false);
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
        updateRecommendationCount(project, modelId);

        RecToIndexResponse response = new RecToIndexResponse();
        response.setModelId(modelId);
        response.setModelAlias(modelAlias);
        response.setAddedIndexes(approveContext.addedLayoutIdList);
        response.setRemovedIndexes(approveContext.removedLayoutIdList);
        response.setBaseIndexInfo(baseIndexUpdater.update(indexPlanService));

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
        Set<Integer> brokenRefIds = optRecV2.getBrokenRefIds();
        if (!allRecItemIds.contains(recItemId) || brokenRefIds.contains(recItemId)) {
            try (SetLogCategory logCategory = new SetLogCategory("smart")) {
                log.info("all recommendation ids {}, broken ref ids {}", allRecItemIds, brokenRefIds);
            }
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getRecListOutOfDate());
        }
        Map<Integer, LayoutRef> layoutRefs = isAdd //
                ? optRecV2.getAdditionalLayoutRefs() //
                : optRecV2.getRemovalLayoutRefs();
        LayoutRef layoutRef = layoutRefs.get(-recItemId);
        if (layoutRef == null) {
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getRecListOutOfDate());
        }
        return layoutRef;
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId, List<String> recTypeList,
            String key, boolean desc, String orderBy, int offset, int limit) {
        aclEvaluate.checkProjectReadPermission(project);
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
        List<OptRecLayoutResponse> recList = getRecLayoutResponses(project, modelId, key, OptRecService.ALL,
                brokenRecs);
        if (userDefinedTypes.size() != RawRecItem.IndexRecType.values().length) {
            recList.removeIf(resp -> !userDefinedTypes.isEmpty() && !userDefinedTypes.contains(resp.getType()));
        }
        if (StringUtils.isNotEmpty(orderBy)) {
            recList.sort(BasicService.propertyComparator(orderBy, !desc));
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
        return getOptRecLayoutsResponseInner(project, modelId, recActionType);
    }

    private OptRecLayoutsResponse getOptRecLayoutsResponseInner(String project, String modelId, String recActionType) {
        OptRecLayoutsResponse layoutsResponse = new OptRecLayoutsResponse();
        List<OptRecLayoutResponse> responses = getRecLayoutResponses(project, modelId, null, recActionType,
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

        List<RawRecItem> result = Lists.newArrayList();
        layoutRefMap.forEach((recId, layoutRef) -> {
            if (layoutRef.isBroken() || layoutRef.isExcluded() || layoutRef.isExisted() || recId >= 0) {
                return;
            }
            result.add(optRecV2.getRawRecItemMap().get(-recId));
        });

        return result;
    }

    private List<OptRecLayoutResponse> getRecLayoutResponses(String project, String modelId, String key,
            String recActionType, Set<Integer> brokenRecCollector) {
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

        brokenRecCollector.addAll(optRecV2.getBrokenRefIds());
        List<RawRecItem> filterRecItems = Lists.newArrayList();
        if (!StringUtils.isBlank(key)) {
            Set<String> ccFullNames = FuzzyKeySearcher.searchComputedColumns(optRecV2.getModel(), key);
            Set<Integer> columnRefs = FuzzyKeySearcher.searchColumnRefs(optRecV2, ccFullNames, key);
            Set<Integer> ccRefIds = FuzzyKeySearcher.searchCCRecRefs(optRecV2, key);
            Set<Integer> dependRefs = Sets.newHashSet(ccRefIds);
            dependRefs.addAll(columnRefs);
            Set<Integer> relatedRecIds = FuzzyKeySearcher.searchDependRefIds(optRecV2, dependRefs, key);

            rawRecItems.forEach(recItem -> {
                if (recItem.isRemoveLayoutRec()) {
                    final long id = RawRecUtil.getLayout(recItem).getId();
                    if (String.valueOf(id).equalsIgnoreCase(key.trim())) {
                        filterRecItems.add(recItem);
                        return;
                    }
                }
                for (int dependID : recItem.getDependIDs()) {
                    if (relatedRecIds.contains(dependID)) {
                        filterRecItems.add(recItem);
                        return;
                    }
                }
            });
        } else {
            filterRecItems.addAll(rawRecItems);
        }
        return convertToV2RecResponse(project, modelId, filterRecItems, optRecV2);
    }

    /**
     * convert RawRecItem response:
     * if type=Layout, layout will be added to IndexPlan when user approves this RawRecItem
     * if type=REMOVAL_LAYOUT, layout will be removed from IndexPlan when user approves this RawRecItem
     */
    private List<OptRecLayoutResponse> convertToV2RecResponse(String project, String modelId, List<RawRecItem> recItems,
            OptRecV2 optRecV2) {
        List<OptRecLayoutResponse> layoutRecResponseList = Lists.newArrayList();

        NDataflowManager dfManager = getManager(NDataflowManager.class, project);
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
            OptRecDetailResponse detailResponse = new OptRecDetailResponse();
            List<Integer> validList = validate(Lists.newArrayList(rawRecItem.getId()), optRecV2, detailResponse, isAdd);
            if (isAdd) {
                detailResponse.getRecItemsToAddLayout().addAll(validList);
            } else {
                detailResponse.getRecItemsToRemoveLayout().addAll(validList);
            }
            response.setRecDetailResponse(detailResponse);
            layoutRecResponseList.add(response);
        });
        return layoutRecResponseList;
    }

    public void updateRecommendationCount(String project, Set<String> modelList) {
        if (CollectionUtils.isEmpty(modelList)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelList.forEach(modelId -> {
                NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                int size = getOptRecLayoutsResponseInner(project, modelId, OptRecService.ALL).getSize();
                NDataModel dataModel = mgr.getDataModelDesc(modelId);
                if (dataModel != null && !dataModel.isBroken() && dataModel.getRecommendationsCount() != size) {
                    mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(size));
                }
            });
            return null;
        }, project);
    }

    public void updateRecommendationCount(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            int size = getOptRecLayoutsResponseInner(project, modelId, OptRecService.ALL).getSize();
            NDataModel dataModel = mgr.getDataModelDesc(modelId);
            if (dataModel != null && !dataModel.isBroken() && dataModel.getRecommendationsCount() != size) {
                mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(size));
            }
            return null;
        }, project);
    }

    public enum RecActionType {
        ALL, ADD_INDEX, REMOVE_INDEX
    }
}
