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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.v2.ColumnRef;
import io.kyligence.kap.metadata.recommendation.v2.DimensionRef;
import io.kyligence.kap.metadata.recommendation.v2.LayoutRef;
import io.kyligence.kap.metadata.recommendation.v2.MeasureRef;
import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationManagerV2;
import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationV2;
import io.kyligence.kap.metadata.recommendation.v2.RecommendationRef;
import io.kyligence.kap.metadata.recommendation.v2.RecommendationUtil;
import io.kyligence.kap.rest.request.OptRecPassRequest;
import io.kyligence.kap.rest.response.LayoutRecommendationResponse;
import io.kyligence.kap.rest.response.OptRecDepResponse;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.response.OptRecommendationResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("optRecService")
public class OptRecService extends BasicService {

    public static final int V1 = 1;
    public static final int V2 = 1;

    private static final Logger logger = LoggerFactory.getLogger(OptRecService.class);

    @Autowired
    public AclEvaluate aclEvaluate;

    private void classify(List<RawRecItem> rawRecItems, List<RawRecItem> ccItems, List<RawRecItem> dimensionItems,
            List<RawRecItem> measureItems, List<RawRecItem> layoutItems) {
        rawRecItems.forEach(rawRecItem -> {
            switch (rawRecItem.getType()) {
            case COMPUTED_COLUMN:
                ccItems.add(rawRecItem);
                return;
            case DIMENSION:
                dimensionItems.add(rawRecItem);
                return;
            case MEASURE:
                measureItems.add(rawRecItem);
                return;
            case LAYOUT:
                layoutItems.add(rawRecItem);
                return;
            default:
            }
        });
    }

    @Transaction(project = 0)
    public void pass(String project, OptRecPassRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);

        val modelId = request.getModelId();

        // TODO handle v1
        val managerV2 = OptimizeRecommendationManagerV2.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val recommendationV2 = managerV2.getOptimizeRecommendationV2(request.getModelId());

        val v2Names = request.getV2Names();

        List<RawRecItem> rawRecItems = recommendationV2.getAllRawItems(request.getLayoutV2ItemIds());
        List<RawRecItem> ccItems = Lists.newArrayList();
        List<RawRecItem> dimensionItems = Lists.newArrayList();
        List<RawRecItem> measureItems = Lists.newArrayList();
        List<RawRecItem> layoutItems = Lists.newArrayList();
        classify(rawRecItems, ccItems, dimensionItems, measureItems, layoutItems);

        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();
        modelManager.updateDataModel(modelId, model -> {
            int lastColumnId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max()
                    .orElse(0);
            int lastMeasureId = model.getAllMeasures().stream().mapToInt(NDataModel.Measure::getId).max().orElse(0);
            val factTable = model.getRootFactTableAlias() != null ? model.getRootFactTableAlias()
                    : model.getRootFactTableName().split("\\.")[1];
            model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                    .forEach(c -> columns.put(c.getId(), c));
            model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                    .forEach(c -> dimensions.put(c.getId(), c));
            model.getAllMeasures().stream().filter(m -> !m.isTomb()).forEach(m -> measures.put(m.getId(), m));
            for (val rawRecItem : ccItems) {
                val cc = RecommendationUtil.getCC(rawRecItem);
                if (v2Names.containsKey(rawRecItem.getId())) {
                    cc.setColumnName(v2Names.get(rawRecItem.getId()));
                    managerV2.checkCCName(model, cc);
                }
                val columnInModel = new NDataModel.NamedColumn();
                columnInModel.setId(++lastColumnId);
                columnInModel.setName(factTable + "_" + cc.getColumnName());
                columnInModel.setAliasDotColumn(factTable + "." + cc.getColumnName());
                columnInModel.setStatus(NDataModel.ColumnStatus.EXIST);
                model.getAllNamedColumns().add(columnInModel);
                model.getComputedColumnDescs().add(cc);
                columns.put(rawRecItem.getId() * -1, columnInModel);
                columns.put(lastColumnId, columnInModel);
            }

            for (val rawRecItem : dimensionItems) {
                int id = rawRecItem.getDependIDs()[0];
                columns.get(id).setStatus(NDataModel.ColumnStatus.DIMENSION);
                if (v2Names.containsKey(rawRecItem.getId())) {
                    columns.get(id).setName(v2Names.get(rawRecItem.getId()));
                    managerV2.checkDimensionName(columns);
                }
                dimensions.put(rawRecItem.getId() * -1, columns.get(id));
            }

            for (val rawRecItem : measureItems) {
                val measure = RecommendationUtil.getMeasure(rawRecItem);
                if (v2Names.containsKey(rawRecItem.getId())) {
                    measure.setName(v2Names.get(rawRecItem.getId()));
                    managerV2.checkMeasureName(model, measure);
                }
                int[] depId = rawRecItem.getDependIDs();
                val measureInModel = new NDataModel.Measure();
                measureInModel.setId(++lastMeasureId);
                measureInModel.setFunction(measure.getFunction());
                measureInModel.setName(measure.getName());
                for (int i = 0; i < depId.length; i++) {
                    if (depId[i] < 0) {
                        measureInModel.getFunction().getParameters().get(i)
                                .setValue(columns.get(depId[i]).getAliasDotColumn());
                    }
                }
                model.getAllMeasures().add(measureInModel);
                measures.put(rawRecItem.getId() * -1, measureInModel);
                measures.put(lastMeasureId, measureInModel);
            }

        });

        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(modelId, indexPlan -> {
            val handler = indexPlan.createUpdateHandler();
            for (val rawRecItem : layoutItems) {
                val layout = RecommendationUtil.getLayout(rawRecItem);
                boolean isAgg = RecommendationUtil.isAgg(rawRecItem);
                List<Integer> colOrder = Lists.newArrayList(layout.getColOrder());
                List<Integer> shardBy = Lists.newArrayList(layout.getShardByColumns());
                List<Integer> sortBy = Lists.newArrayList(layout.getSortByColumns());
                List<Integer> partitionBy = Lists.newArrayList(layout.getPartitionByColumns());
                val depColumns = isAgg ? dimensions : columns;
                translate(colOrder, depColumns, measures);
                layout.setColOrder(colOrder);
                translate(shardBy, depColumns, measures);
                layout.setShardByColumns(shardBy);
                translate(sortBy, depColumns, measures);
                layout.setSortByColumns(sortBy);
                translate(partitionBy, depColumns, measures);
                layout.setPartitionByColumns(partitionBy);
                handler.add(layout, isAgg);
            }
            handler.complete();
        });

        RawRecManager rawManager = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        rawManager.applyRecommendations(rawRecItems.stream().map(RawRecItem::getId).collect(Collectors.toList()));
    }

    private void translate(List<Integer> colOrder, Map<Integer, NDataModel.NamedColumn> columns,
            Map<Integer, NDataModel.Measure> measures) {
        for (int i = 0; i < colOrder.size(); i++) {
            if (colOrder.get(i) < 0) {
                if (columns.containsKey(colOrder.get(i))) {
                    colOrder.set(i, columns.get(colOrder.get(i)).getId());
                }
                if (measures.containsKey(colOrder.get(i))) {
                    colOrder.set(i, measures.get(colOrder.get(i)).getId());
                }
            }
        }
    }

    private static <T extends RecommendationRef> OptRecDepResponse convert(T ref) {
        val response = new OptRecDepResponse();
        response.setVersion(2);
        response.setExpression(ref.getContent());
        response.setName(ref.getName());
        response.setAdd(!ref.isExisted());
        if (response.isAdd()) {
            response.setItemId(ref.getId() * -1);
        }
        return response;
    }

    public OptRecDetailResponse getOptRecDetail(String project, String modelId, List<Integer> rawIds) {
        aclEvaluate.checkProjectReadPermission(project);
        val recommendationV2 = new OptimizeRecommendationV2();
        recommendationV2.setUuid(modelId);
        recommendationV2.setRawIds(rawIds);
        recommendationV2.init(KylinConfig.getInstanceFromEnv(), project);

        Predicate<Map.Entry<Integer, ? extends RecommendationRef>> filter = e -> !e.getValue().isDeleted()
                && !e.getValue().isExisted() && e.getKey() < 0;
        List<ColumnRef> columnRefs = recommendationV2.getColumnRefs().entrySet().stream().filter(filter)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        List<DimensionRef> dimensionRefs = recommendationV2.getDimensionRefs().entrySet().stream().filter(filter)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        List<MeasureRef> measureRefs = recommendationV2.getMeasureRefs().entrySet().stream().filter(filter)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        List<LayoutRef> layoutRefs = recommendationV2.getLayoutRefs().entrySet().stream().filter(filter)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        detailResponse.setColumnItems(columnRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse
                .setDimensionItems(dimensionRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse.setMeasureItems(measureRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse.setLayoutItemIds(layoutRefs.stream().map(RecommendationRef::getId).collect(Collectors.toList()));
        return detailResponse;
    }

    public OptRecDetailResponse getSingleOptRecDetail(String project, String modelId, int rawId) {
        aclEvaluate.checkProjectReadPermission(project);
        val recommendationV2 = new OptimizeRecommendationV2();
        recommendationV2.setUuid(modelId);
        recommendationV2.setRawIds(Lists.newArrayList(rawId));
        recommendationV2.init(KylinConfig.getInstanceFromEnv(), project);
        val layoutRef = recommendationV2.getLayoutRefs().get(rawId * -1);
        if (layoutRef == null) {
            throw new IllegalStateException("Raw item id " + rawId + " is null.");
        }
        List<ColumnRef> columnRefs = Lists.newArrayList();
        List<DimensionRef> dimensionRefs = Lists.newArrayList();
        List<MeasureRef> measureRefs = Lists.newArrayList();
        layoutRef.getDependencies().forEach(depRef -> {
            if (depRef instanceof ColumnRef) {
                columnRefs.add((ColumnRef) depRef);
            } else if (depRef instanceof DimensionRef) {
                dimensionRefs.add((DimensionRef) depRef);
                columnRefs.add(((DimensionRef) depRef).getColumnRef());
            } else if (depRef instanceof MeasureRef) {
                measureRefs.add((MeasureRef) depRef);
            }
        });
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        detailResponse.setColumnItems(columnRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse
                .setDimensionItems(dimensionRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse.setMeasureItems(measureRefs.stream().map(OptRecService::convert).collect(Collectors.toList()));
        detailResponse.setLayoutItemIds(Lists.newArrayList(rawId));
        return detailResponse;
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId) {
        aclEvaluate.checkProjectReadPermission(project);
        OptRecLayoutsResponse layoutsResponse = new OptRecLayoutsResponse();

        val managerV1 = OptimizeRecommendationManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val recommendationV1 = managerV1.getOptimizeRecommendation(modelId);

        val layouts = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getIndexPlan(modelId)
                .getAllLayouts().stream().collect(Collectors.toMap(LayoutEntity::getId, Function.identity()));

        layoutsResponse.getLayouts()
                .addAll(recommendationV1.getLayoutRecommendations().stream().filter(item -> !item.isAdd())
                        .map(item -> convert(layouts, project, modelId, item)).collect(Collectors.toList()));

        val managerV2 = OptimizeRecommendationManagerV2.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val recommendationV2 = managerV2.getOptimizeRecommendationV2(modelId);

        Predicate<Map.Entry<Integer, ? extends RecommendationRef>> filter = e -> !e.getValue().isDeleted()
                && !e.getValue().isExisted() && e.getKey() < 0;
        List<RawRecItem> layoutRefs = recommendationV2.getLayoutRefs().entrySet().stream().filter(filter)
                .map(e -> recommendationV2.getRawRecItemMap().get(-1 * e.getKey())).collect(Collectors.toList());

        layoutsResponse.getLayouts().addAll(layoutRefs.stream().map(this::convert).collect(Collectors.toList()));
        layoutsResponse.setSize(layoutsResponse.getLayouts().size());
        return layoutsResponse;
    }

    private OptRecLayoutResponse convert(RawRecItem raw) {
        val response = new OptRecLayoutResponse();
        response.setItemId(raw.getId());
        response.setCreateTime(raw.getCreateTime());
        val layout = RecommendationUtil.getLayout(raw);
        response.setColumnsAndMeasuresSize(layout.getColOrder().size());
        if (RecommendationUtil.isAgg(raw)) {
            response.setType(LayoutRecommendationResponse.Type.ADD_AGG);
        } else {
            response.setType(LayoutRecommendationResponse.Type.ADD_TABLE);
        }
        response.setSource(LayoutRecommendationItem.QUERY_HISTORY);
        response.setVersion(V2);
        response.setLastModifyTime(raw.getUpdateTime());
        return response;
    }

    private OptRecLayoutResponse convert(Map<Long, LayoutEntity> layouts, String project, String modelId,
            LayoutRecommendationItem item) {
        val layoutRecommendationResponse = OptRecommendationResponse.convertToIndexRecommendationResponse(project,
                modelId, item);
        val optRecLayoutResponse = new OptRecLayoutResponse();
        BeanUtils.copyProperties(layoutRecommendationResponse, optRecLayoutResponse);
        optRecLayoutResponse.setVersion(V1);
        val layout = layouts.get(item.getLayout().getId());
        optRecLayoutResponse.setLastModifyTime(layout.getUpdateTime());
        return optRecLayoutResponse;
    }

}
