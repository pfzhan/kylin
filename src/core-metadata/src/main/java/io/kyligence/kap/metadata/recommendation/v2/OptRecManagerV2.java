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

package io.kyligence.kap.metadata.recommendation.v2;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.recommendation.CCRecommendationItem;
import io.kyligence.kap.metadata.recommendation.CCVisitor;
import io.kyligence.kap.metadata.recommendation.DimensionRecommendationItem;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.MeasureRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecManagerV2 {

    private static class CCConflictHandlerV2 extends ComputedColumnUtil.BasicCCConflictHandler {
        @Override
        public void handleOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
                ComputedColumnDesc newCC) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_EXPRESSION_CONFLICT(newCC.getExpression(), newCC.getColumnName(),
                            existingCC.getColumnName()));
        }

        @Override
        public void handleOnSameNameDiffExpr(NDataModel existingModel, NDataModel newModel,
                ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_NAME_CONFLICT(newCC.getColumnName()));
        }
    }

    // for user defined
    public void checkCCName(NDataModel model, ComputedColumnDesc cc) {
        val otherModels = NDataModelManager.getInstance(config, project).listAllModels().stream()
                .filter(m -> !m.getId().equals(model.getId()) && !m.isBroken()).collect(Collectors.toList());

        for (NDataModel otherModel : otherModels) {
            for (ComputedColumnDesc existCC : otherModel.getComputedColumnDescs()) {
                ComputedColumnUtil.singleCCConflictCheck(otherModel, model, existCC, cc, new CCConflictHandlerV2());
            }
        }
        val contains = model.getComputedColumnDescs().stream()
                .anyMatch(c -> c.getColumnName().equals(cc.getColumnName()));
        if (contains) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getCC_NAME_CONFLICT(cc.getColumnName()));
        }
    }

    public void checkMeasureName(NDataModel model, NDataModel.Measure measure) {
        val contains = model.getAllMeasures().stream().anyMatch(c -> c.getName().equals(measure.getName()));
        if (contains) {
            throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                    MsgPicker.getMsg().getMEASURE_CONFLICT(measure.getName()));
        }
    }

    public void checkDimensionName(Map<Integer, NDataModel.NamedColumn> columns) {
        columns.values().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getName, Function.identity(), (u, v) -> {
                    if (u == v) {
                        return u;
                    }
                    throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                            MsgPicker.getMsg().getDIMENSION_CONFLICT(v.getName()));
                }));
    }

    public static OptRecManagerV2 getInstance(String project) {
        return Singletons.getInstance(project, OptRecManagerV2.class);
    }

    private final KylinConfig config;
    private final String project;

    OptRecManagerV2(String project) {
        this.config = KylinConfig.getInstanceFromEnv();
        this.project = project;
    }

    public OptRecV2 getOptimizeRecommendationV2(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return new OptRecV2(project, id);
    }

    public void discardAll(String modelId) {
        OptRecV2 recommendationV2 = getOptimizeRecommendationV2(modelId);
        if (recommendationV2 == null) {
            return;
        }
        List<Integer> rawIds = recommendationV2.getRawIds();
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.discardByIds(rawIds);
    }

    @VisibleForTesting
    public List<RawRecItem> convertFromV1(OptimizeRecommendation recommendation) {

        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), recommendation.getProject())
                .getDataModelDesc(recommendation.getId());

        // from v1 virtual id to v2 raw id.
        Map<Integer, Integer> columnIdMap = Maps.newHashMap();
        // from v1 dimension id to v2 raw id.
        Map<Integer, Integer> dimensionIdMap = Maps.newHashMap();
        // from v1 measure id to v2 raw id.
        Map<Integer, Integer> measureIdMap = Maps.newHashMap();
        // from column full name to column id.
        Map<String, Integer> columnNameMap = Maps.newHashMap();

        model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .forEach(c -> columnNameMap.put(c.getAliasDotColumn(), c.getId()));

        val rawItems = Lists.<RawRecItem> newArrayList();
        int id = 1;

        for (CCRecommendationItem item : recommendation.getCcRecommendations()) {
            val rawItem = convert(id++, columnIdMap, columnNameMap, item);
            rawItems.add(rawItem);
        }

        for (DimensionRecommendationItem item : recommendation.getDimensionRecommendations()) {
            val rawItem = convert(id++, columnIdMap, dimensionIdMap, item);
            rawItems.add(rawItem);
        }

        for (MeasureRecommendationItem item : recommendation.getMeasureRecommendations()) {
            val rawItem = convert(id++, measureIdMap, columnNameMap, item);
            rawItems.add(rawItem);
        }

        for (LayoutRecommendationItem item : recommendation.getLayoutRecommendations()) {
            if (!item.isAdd()) {
                continue;
            }
            val rawItem = convert(id++, columnIdMap, dimensionIdMap, measureIdMap, item);
            rawItems.add(rawItem);
        }

        return rawItems;

    }

    private RawRecItem convert(int id, Map<Integer, Integer> idMap, Map<String, Integer> columnNameMap,
            CCRecommendationItem cc) {
        RawRecItem rawRecItem = createBasicRawRecItem(id, RawRecItem.RawRecType.COMPUTED_COLUMN);
        columnNameMap.put(cc.getCc().getFullName(), -1 * id);
        idMap.put(cc.getCcColumnId(), -id);
        CCRecItemV2 itemV2 = new CCRecItemV2();
        rawRecItem.setRecEntity(itemV2);
        itemV2.setCc(cc.getCc());
        itemV2.setCreateTime(System.currentTimeMillis());
        List<Integer> dependId = Lists.newArrayList();
        CCVisitor ccVisitor = new CCVisitor() {
            @Override
            public SqlIdentifier visit(SqlIdentifier identifier) {
                if (identifier.names.size() == 2) {
                    final String[] values = identifier.names.toArray(new String[0]);
                    final String fullName = values[0] + "." + values[1];
                    dependId.add(columnNameMap.get(fullName));
                }
                return null;
            }
        };
        ccVisitor.visitExpr(cc.getCc().getExpression());
        rawRecItem.setDependIDs(dependId.stream().mapToInt(Integer::intValue).toArray());
        return rawRecItem;
    }

    private RawRecItem convert(int id, Map<Integer, Integer> idMap, Map<Integer, Integer> dimensionIdMap,
            DimensionRecommendationItem dimension) {
        RawRecItem rawRecItem = createBasicRawRecItem(id, RawRecItem.RawRecType.DIMENSION);
        int colId = dimension.getColumn().getId();
        dimensionIdMap.put(colId, -1 * id);

        int dependId = idMap.getOrDefault(colId, colId);
        DimensionRecItemV2 itemV2 = new DimensionRecItemV2();
        itemV2.setDataType(dimension.getDataType());
        rawRecItem.setRecEntity(itemV2);
        itemV2.setCreateTime(System.currentTimeMillis());
        rawRecItem.setDependIDs(new int[] { dependId });
        return rawRecItem;
    }

    private RawRecItem convert(int id, Map<Integer, Integer> measureIdMap, Map<String, Integer> columnNameMap,
            MeasureRecommendationItem measure) {
        RawRecItem rawRecItem = createBasicRawRecItem(id, RawRecItem.RawRecType.MEASURE);
        measureIdMap.put(measure.getMeasureId(), -1 * id);
        MeasureRecItemV2 itemV2 = new MeasureRecItemV2();
        itemV2.setMeasure(measure.getMeasure());
        rawRecItem.setRecEntity(itemV2);
        List<Integer> dependId = Lists.newArrayList();
        List<ParameterDesc> parameterDescs = measure.getMeasure().getFunction().getParameters();
        for (ParameterDesc parameterDesc : parameterDescs) {
            if (parameterDesc.isColumnType()) {
                dependId.add(columnNameMap.get(parameterDesc.getValue()));
            }
        }
        rawRecItem.setDependIDs(dependId.stream().mapToInt(Integer::intValue).toArray());
        return rawRecItem;
    }

    private RawRecItem convert(int id, Map<Integer, Integer> columnIdMap, Map<Integer, Integer> dimensionIdMap,
            Map<Integer, Integer> measureIdMap, LayoutRecommendationItem layout) {
        RawRecItem rawRecItem = createBasicRawRecItem(id, RawRecItem.RawRecType.LAYOUT);
        LayoutRecItemV2 recItemV2 = new LayoutRecItemV2();
        rawRecItem.setRecEntity(recItemV2);
        recItemV2.setAgg(layout.isAggIndex());
        List<Integer> colOrder = Lists.newArrayList(layout.getLayout().getColOrder());
        List<Integer> sortBy = Lists.newArrayList(layout.getLayout().getSortByColumns());
        List<Integer> shardBy = Lists.newArrayList(layout.getLayout().getShardByColumns());
        List<Integer> partitionBy = Lists.newArrayList(layout.getLayout().getPartitionByColumns());
        translate(colOrder, dimensionIdMap, measureIdMap);
        translate(sortBy, dimensionIdMap, measureIdMap);
        translate(shardBy, dimensionIdMap, measureIdMap);
        translate(partitionBy, dimensionIdMap, measureIdMap);
        LayoutEntity layoutEntity = JsonUtil.deepCopyQuietly(layout.getLayout(), LayoutEntity.class);
        layoutEntity.setColOrder(colOrder);
        layoutEntity.setSortByColumns(sortBy);
        layoutEntity.setShardByColumns(shardBy);
        layoutEntity.setPartitionByColumns(partitionBy);
        recItemV2.setLayout(layoutEntity);
        rawRecItem.setDependIDs(colOrder.stream().mapToInt(Integer::intValue).toArray());
        return rawRecItem;
    }

    private void translate(List<Integer> colOrder, Map<Integer, Integer> columnIdMap,
            Map<Integer, Integer> measureIdMap) {
        for (int i = 0; i < colOrder.size(); i++) {
            int col = colOrder.get(i);
            if (columnIdMap.containsKey(col)) {
                colOrder.set(i, columnIdMap.get(col));
            }
            if (measureIdMap.containsKey(col)) {
                colOrder.set(i, measureIdMap.get(col));
            }
        }
    }

    private RawRecItem createBasicRawRecItem(int id, RawRecItem.RawRecType type) {
        RawRecItem rawRecItem = new RawRecItem();
        rawRecItem.setId(id);
        rawRecItem.setCreateTime(System.currentTimeMillis());
        rawRecItem.setUpdateTime(System.currentTimeMillis());
        rawRecItem.setType(type);
        return rawRecItem;
    }

    public void cleanInEffective(String id) {
        OptRecV2 recommendation = getOptimizeRecommendationV2(id);
        if (recommendation == null) {
            return;
        }

        List<Integer> inEffective = recommendation.filterBrokenRefs();
        RawRecManager.getInstance(project).removeByIds(inEffective);
    }
}
