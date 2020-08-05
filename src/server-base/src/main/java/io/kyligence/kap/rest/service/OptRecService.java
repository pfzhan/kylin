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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationVerifier;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.v2.CCRef;
import io.kyligence.kap.metadata.recommendation.v2.DimensionRef;
import io.kyligence.kap.metadata.recommendation.v2.LayoutRef;
import io.kyligence.kap.metadata.recommendation.v2.MeasureRef;
import io.kyligence.kap.metadata.recommendation.v2.ModelColumnRef;
import io.kyligence.kap.metadata.recommendation.v2.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.v2.OptRecV2;
import io.kyligence.kap.metadata.recommendation.v2.RecommendationRef;
import io.kyligence.kap.metadata.recommendation.v2.RecommendationUtil;
import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.LayoutRecommendationResponse;
import io.kyligence.kap.rest.response.OptRecDepResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("optRecService")
public class OptRecService extends BasicService implements ModelUpdateListener {

    public static final int V1 = 1;
    public static final int V2 = 2;

    private static final String REC_ABSENT_ERROR = "RawRecItem id {} is null";

    @Autowired
    public AclEvaluate aclEvaluate;

    private static final class RecApproveContext {
        private final Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        private final Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        private final Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();

        private final OptRecV2 recommendation;
        private final Map<Integer, String> userDefinedRecNameMap;
        private final NDataModelManager modelManager;
        private final NIndexPlanManager indexPlanManager;
        private final OptRecManagerV2 recManagerV2;

        private RecApproveContext(String project, String modelId, Map<Integer, String> userDefinedRecNameMap) {
            this.userDefinedRecNameMap = userDefinedRecNameMap;
            this.recManagerV2 = OptRecManagerV2.getInstance(project);
            this.recommendation = recManagerV2.getOptimizeRecommendationV2(modelId);
            this.modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            this.indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        }

        public List<RawRecItem> approveRawRecItems(List<Integer> layoutIds) {
            List<RawRecItem> rawRecItems = recommendation.getAllRelatedRecItems(layoutIds);
            rewriteModel(rawRecItems);
            rewriteIndexPlan(rawRecItems);
            return rawRecItems;
        }

        private void rewriteModel(List<RawRecItem> recItems) {
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
                    case LAYOUT:
                    default:
                        break;
                    }
                }
            });
            logFinishRewrite("Model");
        }

        private void writeMeasureToModel(NDataModel model, RawRecItem rawRecItem) {
            int lastMeasureId = model.getMaxMeasureId();
            Map<Integer, RecommendationRef> measureRefs = recommendation.getMeasureRefs();
            RecommendationRef recommendationRef = measureRefs.get(-rawRecItem.getId());
            if (recommendationRef.isExisted()) {
                return;
            }
            // copy more better
            MeasureRef measureRef = (MeasureRef) recommendationRef;
            NDataModel.Measure measure = measureRef.getMeasure();
            if (userDefinedRecNameMap.containsKey(rawRecItem.getId())) {
                measure.setName(userDefinedRecNameMap.get(rawRecItem.getId()));
            }
            recManagerV2.checkMeasureName(model, measure);
            measure.setId(++lastMeasureId);
            model.getAllMeasures().add(measure);
            measures.put(-rawRecItem.getId(), measure);
            measures.put(lastMeasureId, measure);
            logWriteProperty(rawRecItem, measure);
        }

        private void writeDimensionToModel(RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> dimensionRefs = recommendation.getDimensionRefs();
            RecommendationRef dimensionRef = dimensionRefs.get(-rawRecItem.getId());
            if (dimensionRef.isExisted()) {
                return;
            }
            DimensionRef dimRef = (DimensionRef) dimensionRef;
            Preconditions.checkArgument(dimRef.getEntity() instanceof ModelColumnRef);
            ModelColumnRef columnRef = (ModelColumnRef) dimensionRef.getEntity();
            NDataModel.NamedColumn column = columnRef.getColumn();
            if (userDefinedRecNameMap.containsKey(rawRecItem.getId())) {
                column.setName(userDefinedRecNameMap.get(rawRecItem.getId()));
            }
            column.setStatus(NDataModel.ColumnStatus.DIMENSION);
            recManagerV2.checkDimensionName(columns);
            dimensions.putIfAbsent(-rawRecItem.getId(), column);
            columns.get(column.getId()).setStatus(NDataModel.ColumnStatus.DIMENSION);
            logWriteProperty(rawRecItem, column);
        }

        private void writeCCToModel(NDataModel model, RawRecItem rawRecItem) {
            int lastColumnId = model.getMaxColumnId();
            Map<Integer, RecommendationRef> ccRefs = recommendation.getCcRefs();
            RecommendationRef recommendationRef = ccRefs.get(-rawRecItem.getId());
            if (recommendationRef.isExisted()) {
                return;
            }
            CCRef ccRef = (CCRef) recommendationRef;
            ComputedColumnDesc cc = ccRef.getCc();
            if (userDefinedRecNameMap.containsKey(rawRecItem.getId())) {
                cc.setColumnName(userDefinedRecNameMap.get(rawRecItem.getId()));
            }
            recManagerV2.checkCCName(model, cc);
            NDataModel.NamedColumn columnInModel = new NDataModel.NamedColumn();
            columnInModel.setId(++lastColumnId);
            columnInModel.setName(cc.getTableAlias() + "_" + cc.getColumnName());
            columnInModel.setAliasDotColumn(cc.getTableAlias() + "." + cc.getColumnName());
            columnInModel.setStatus(NDataModel.ColumnStatus.EXIST);
            model.getAllNamedColumns().add(columnInModel);
            model.getComputedColumnDescs().add(cc);
            columns.put(-rawRecItem.getId(), columnInModel);
            columns.put(lastColumnId, columnInModel);
            logWriteProperty(rawRecItem, columnInModel);
        }

        private void rewriteIndexPlan(List<RawRecItem> recItems) {
            logBeginRewrite("IndexPlan");
            indexPlanManager.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (rawRecItem.getType() != RawRecItem.RawRecType.LAYOUT) {
                        continue;
                    }
                    LayoutEntity layout = RecommendationUtil.getLayout(rawRecItem);
                    List<Integer> colOrder = layout.getColOrder();
                    List<Integer> shardBy = Lists.newArrayList(layout.getShardByColumns());
                    List<Integer> sortBy = Lists.newArrayList(layout.getSortByColumns());
                    List<Integer> partitionBy = Lists.newArrayList(layout.getPartitionByColumns());

                    layout.setColOrder(translateToRealIds(colOrder));
                    layout.setShardByColumns(translateToRealIds(shardBy));
                    layout.setSortByColumns(translateToRealIds(sortBy));
                    layout.setPartitionByColumns(translateToRealIds(partitionBy));
                    layout.setPartitionByColumns(partitionBy);
                    updateHandler.add(layout, RecommendationUtil.isAgg(rawRecItem));
                }
                updateHandler.complete();
            });
            logFinishRewrite("IndexPlan");
        }

        private List<Integer> translateToRealIds(List<Integer> virtualIds) {
            List<Integer> realIds = Lists.newArrayList();
            virtualIds.forEach(virtualId -> {
                int realId;
                recommendation.getDimensionRefs().get(virtualId);
                if (dimensions.containsKey(virtualId)) {
                    realId = dimensions.get(virtualId).getId();
                } else if (measures.containsKey(virtualId)) {
                    realId = measures.get(virtualId).getId();
                } else {
                    throw new IllegalStateException("");
                }
                realIds.add(realId);
            });
            return realIds;
        }

        private void logBeginRewrite(String rewrite) {
            log.info("Start to rewrite RawRecItems to {}({}/{})", rewrite, recommendation.getProject(),
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
        approveV1RecItems(request);
        approveV2RecItems(request);
    }

    private void approveV1RecItems(OptRecRequest request) {
        OptimizeRecommendationVerifier verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(),
                request.getProject(), request.getModelId());
        verifier.setPassLayoutItems(
                request.getLayoutIdsToRemove().stream().map(Long::valueOf).collect(Collectors.toSet()));
        verifier.verify();
    }

    private void approveV2RecItems(OptRecRequest request) {
        String project = request.getProject();
        String modelId = request.getModelId();
        Map<Integer, String> userDefinedRecNameMap = request.getNames();
        List<Integer> layoutIdsToAdd = request.getLayoutIdsToAdd();
        List<RawRecItem> recItems = new RecApproveContext(project, modelId, userDefinedRecNameMap)
                .approveRawRecItems(layoutIdsToAdd);

        UnitOfWork.get().doAfterUpdate(() -> {
            RawRecManager rawManager = RawRecManager.getInstance(project);
            rawManager.applyByIds(recItems.stream().map(RawRecItem::getId).collect(Collectors.toList()));
        });
    }

    @Transaction(project = 0)
    public void delete(String project, OptRecRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptimizeRecommendationVerifier verifier = new OptimizeRecommendationVerifier(KylinConfig.getInstanceFromEnv(),
                project, request.getModelId());
        verifier.setFailLayoutItems(
                request.getLayoutIdsToRemove().stream().map(Long::valueOf).collect(Collectors.toSet()));
        verifier.verify();
        OptRecManagerV2 managerV2 = OptRecManagerV2.getInstance(project);
        managerV2.cleanInEffective(request.getModelId());
        OptRecV2 recommendationV2 = managerV2.getOptimizeRecommendationV2(request.getModelId());
        if (recommendationV2 == null) {
            return;
        }
        UnitOfWork.get().doAfterUpdate(() -> {
            RawRecManager rawManager = RawRecManager.getInstance(project);
            rawManager.discardByIds(intersection(recommendationV2.getRawIds(), request.getLayoutIdsToAdd()));
        });
    }

    @Transaction(project = 0)
    public void clean(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptimizeRecommendationManager manager = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        manager.cleanAll(modelId);
        OptRecManagerV2 managerV2 = OptRecManagerV2.getInstance(project);
        managerV2.discardAll(modelId);
    }

    private static OptRecDepResponse convert(RecommendationRef ref) {
        OptRecDepResponse response = new OptRecDepResponse();
        response.setVersion(V2);
        response.setContent(ref.getContent());
        response.setName(ref.getName());
        response.setAdd(!ref.isExisted());
        if (response.isAdd()) {
            response.setItemId(-ref.getId());
        }
        return response;
    }

    public OptRecDetailResponse getOptRecDetail(String project, String modelId, List<Integer> selectedIds) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        OptRecV2 recommendationV2 = new OptRecV2(project, modelId);
        Set<Integer> allRecItemIds = Sets.newHashSet(recommendationV2.getRawIds());
        List<Integer> existingIds = Lists.newArrayList();
        List<Integer> brokenIds = Lists.newArrayList();
        selectedIds.forEach(id -> {
            if (!allRecItemIds.contains(id)) {
                brokenIds.add(id);
            } else {
                existingIds.add(id);
            }
        });

        Set<OptRecDepResponse> dimensionRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> measureRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> ccRefResponse = Sets.newHashSet();
        existingIds.forEach(recItemId -> {
            RecommendationRef layoutRef = recommendationV2.getLayoutRefs().get(-recItemId);
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
        });
        detailResponse.setDimensionItems(Lists.newArrayList(dimensionRefResponse));
        detailResponse.setMeasureItems(Lists.newArrayList(measureRefResponse));
        detailResponse.setCcItems(Lists.newArrayList(ccRefResponse));
        detailResponse.setLayoutItemIds(existingIds);
        detailResponse.setBrokenLayoutItemIds(brokenIds);
        return detailResponse;
    }

    public OptRecDetailResponse getSingleOptRecDetail(String project, String modelId, int recItemId, boolean isAdd) {
        if (isAdd) {
            return getSingleOptRecDetail(project, modelId, recItemId);
        } else {
            // is delete
            OptRecDetailResponse detailResponse = new OptRecDetailResponse();
            List<OptRecDepResponse> dimensionItems = Lists.newArrayList();
            List<OptRecDepResponse> measureItems = Lists.newArrayList();

            for (LayoutRecommendationItem item : OptimizeRecommendationManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).getOptimizeRecommendation(modelId)
                    .getLayoutRecommendations()) {
                if (item.getItemId() == recItemId) {
                    collectDimAndMeaItems(item, dimensionItems, measureItems, project, modelId);
                    break;
                }
            }
            detailResponse.setLayoutItemIds(Lists.newArrayList(recItemId));
            detailResponse.setDimensionItems(dimensionItems);
            detailResponse.setMeasureItems(measureItems);
            detailResponse.setCcItems(Lists.newArrayList());
            return detailResponse;
        }
    }

    private OptRecDetailResponse getSingleOptRecDetail(String project, String modelId, int recItemId) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecV2 recommendationV2 = new OptRecV2(project, modelId);
        Set<Integer> originRawIds = Sets.newHashSet(recommendationV2.getRawIds());
        Preconditions.checkArgument(originRawIds.contains(recItemId), REC_ABSENT_ERROR, recItemId);
        LayoutRef layoutRef = recommendationV2.getLayoutRefs().get(-recItemId);
        Preconditions.checkArgument(layoutRef != null, REC_ABSENT_ERROR, recItemId);

        List<OptRecDepResponse> dimensionRefResponse = Lists.newArrayList();
        List<OptRecDepResponse> measureRefResponse = Lists.newArrayList();
        List<OptRecDepResponse> ccRefResponse = Lists.newArrayList();
        layoutRef.getDependencies().forEach(dependRef -> {
            OptRecDepResponse convert = OptRecService.convert(dependRef);
            if (dependRef instanceof DimensionRef) {
                dimensionRefResponse.add(convert);
            } else if (dependRef instanceof MeasureRef) {
                measureRefResponse.add(convert);
            }

            dependRef.getDependencies().forEach(ref -> {
                if (ref instanceof CCRef) {
                    ccRefResponse.add(OptRecService.convert(ref));
                }
            });
        });

        // cc, dimension, measure, layout
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        detailResponse.setDimensionItems(dimensionRefResponse);
        detailResponse.setMeasureItems(measureRefResponse);
        detailResponse.setCcItems(ccRefResponse);
        detailResponse.setLayoutItemIds(Lists.newArrayList(recItemId));
        return detailResponse;
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecLayoutsResponse layoutsResponse = new OptRecLayoutsResponse();
        layoutsResponse.getLayouts().addAll(convertToV1RecResponse(project, modelId));
        layoutsResponse.getLayouts().addAll(convertToV2RecResponse(project, modelId));
        layoutsResponse.setSize(layoutsResponse.getLayouts().size());
        return layoutsResponse;
    }

    /**
     * convert v1 recommendations to response
     */
    private List<OptRecLayoutResponse> convertToV1RecResponse(String project, String uuid) {
        OptimizeRecommendation recV1 = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getOptimizeRecommendation(uuid);
        List<OptRecLayoutResponse> result = Lists.newArrayList();
        if (recV1 == null) {
            return result;
        }

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<Long, LayoutEntity> layouts = indexPlanManager.getIndexPlan(uuid).getAllLayouts() //
                .stream().collect(Collectors.toMap(LayoutEntity::getId, Function.identity()));
        recV1.getLayoutRecommendations().forEach(item -> {
            if (!item.isAdd()) {
                LayoutRecommendationResponse response = OptRecommendationResponse
                        .convertToIndexRecommendationResponse(project, uuid, item);
                OptRecLayoutResponse optRecLayoutResponse = new OptRecLayoutResponse();
                BeanUtils.copyProperties(response, optRecLayoutResponse);
                optRecLayoutResponse.setVersion(V1);
                LayoutEntity layout = layouts.get(item.getLayout().getId());
                optRecLayoutResponse.setLastModifyTime(layout.getUpdateTime());
                result.add(optRecLayoutResponse);
            }
        });
        return result;
    }

    /**
     * convert v2 recommendations to response
     */
    private List<OptRecLayoutResponse> convertToV2RecResponse(String project, String uuid) {
        OptRecV2 recommendationV2 = OptRecManagerV2.getInstance(project).getOptimizeRecommendationV2(uuid);
        List<OptRecLayoutResponse> layoutRecResponseList = Lists.newArrayList();
        if (recommendationV2 == null) {
            return layoutRecResponseList;
        }

        recommendationV2.getLayoutRefs().forEach((recId, layoutRef) -> {
            if (!layoutRef.isBroken() && !layoutRef.isExisted() && recId < 0) {
                RawRecItem rawRecItem = recommendationV2.getRawRecItemMap().get(-recId);
                OptRecLayoutResponse response = new OptRecLayoutResponse();
                response.setItemId(rawRecItem.getId());
                response.setCreateTime(rawRecItem.getCreateTime());
                response.setUsage(rawRecItem.getHitCount());
                LayoutEntity layout = RecommendationUtil.getLayout(rawRecItem);
                response.setColumnsAndMeasuresSize(layout.getColOrder().size());
                if (RecommendationUtil.isAgg(rawRecItem)) {
                    response.setType(LayoutRecommendationResponse.Type.ADD_AGG);
                } else {
                    response.setType(LayoutRecommendationResponse.Type.ADD_TABLE);
                }
                response.setSource(LayoutRecommendationItem.QUERY_HISTORY);
                response.setVersion(V2);
                response.setLastModifyTime(rawRecItem.getUpdateTime());
                response.setAdd(true);
                layoutRecResponseList.add(response);
            }
        });
        return layoutRecResponseList;
    }

    @Override
    public void onUpdate(String project, String modelId) {
        val prjManager = getProjectManager();
        val prjInstance = prjManager.getProject(project);
        if (prjInstance.isSemiAutoMode()) {
            val recommendationManager = getOptimizeRecommendationManager(project);
            recommendationManager.cleanInEffective(modelId);
            val recommendationManagerV2 = getOptimizeRecommendationManagerV2(project);
            recommendationManagerV2.cleanInEffective(modelId);
        }
    }

    private void collectDimAndMeaItems(LayoutRecommendationItem item, List<OptRecDepResponse> dimensionItems,
            List<OptRecDepResponse> measureItems, String project, String modelId) {
        NDataModel dataModelDesc = getDataModelManager(project).getDataModelDesc(modelId);
        for (int colId : item.getLayout().getColOrder()) {
            NDataModel.NamedColumn namedColumn = dataModelDesc.getEffectiveNamedColumns().get(colId);
            if (namedColumn != null && namedColumn.isDimension()) {
                dimensionItems.add(new OptRecDepResponse(2, namedColumn.getName(), false));
                continue;
            }
            String measureName = dataModelDesc.getMeasureNameByMeasureId(colId);
            if (measureName != null) {
                measureItems.add(new OptRecDepResponse(2, measureName, false));
            }
        }
    }
}
