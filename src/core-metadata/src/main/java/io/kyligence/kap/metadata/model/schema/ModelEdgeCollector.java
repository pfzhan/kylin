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
package io.kyligence.kap.metadata.model.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableBiMap;

import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.MutableGraph;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
class ModelEdgeCollector {

    @NonNull
    private IndexPlan indexPlan;

    @NonNull
    private MutableGraph<SchemaNode> graph;

    private NDataModel model;
    private Map<Integer, NDataModel.NamedColumn> effectiveNamedColumns;
    private Map<String, NDataModel.NamedColumn> nameColumnIdMap;
    private ImmutableBiMap<Integer, TblColRef> effectiveCols;
    private ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures;
    private Map<Integer, String> modelColumnMeasureIdNameMap = new HashMap<>();

    public Graph<SchemaNode> collect() {
        model = indexPlan.getModel();
        effectiveNamedColumns = model.getEffectiveNamedColumns();
        effectiveCols = model.getEffectiveCols();
        effectiveMeasures = model.getEffectiveMeasures();
        nameColumnIdMap = effectiveNamedColumns.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().getAliasDotColumn(), Map.Entry::getValue));

        modelColumnMeasureIdNameMap.putAll(model.getAllMeasures().stream().filter(measure -> !measure.isTomb())
                .collect(Collectors.toMap(NDataModel.Measure::getId, NDataModel.Measure::getName)));
        modelColumnMeasureIdNameMap.putAll(model.getAllNamedColumns().stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn)));

        collectModelColumns();
        collectModelSignificant();
        collectDimensionAndMeasure();

        collectIndex(indexPlan.getWhitelistLayouts(), SchemaNodeType.WHITE_LIST_INDEX, Lists.newArrayList());
        collectIndex(indexPlan.getToBeDeletedIndexes().stream().flatMap(index -> index.getLayouts().stream())
                .collect(Collectors.toList()), SchemaNodeType.TO_BE_DELETED_INDEX, Lists.newArrayList());
        collectIndex(indexPlan.getRuleBaseLayouts(), SchemaNodeType.RULE_BASED_INDEX, indexPlan.getAggShardByColumns());

        collectAggGroup();
        collectIndexPlan();
        return graph;
    }

    private void collectModelColumns() {
        val ccs = model.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getColumnName, Function.identity()));
        effectiveCols.forEach((id, tblColRef) -> {
            val namedColumn = effectiveNamedColumns.get(id);
            if (tblColRef.getColumnDesc().isComputedColumn()) {
                return;
            }
            graph.putEdge(SchemaNode.ofTableColumn(tblColRef.getColumnDesc()),
                    SchemaNode.ofModelColumn(namedColumn, model.getAlias()));
        });
        effectiveCols.forEach((id, tblColRef) -> {
            if (!tblColRef.getColumnDesc().isComputedColumn()) {
                return;
            }
            val namedColumn = effectiveNamedColumns.get(id);
            val cc = ccs.get(tblColRef.getName());
            val ccNode = SchemaNode.ofModelCC(cc, model.getAlias());
            collectExprWithModel(cc.getExpression(), ccNode);
            graph.putEdge(ccNode, SchemaNode.ofModelColumn(namedColumn, model.getAlias()));
        });
    }

    private void collectModelSignificant() {
        if (model.getPartitionDesc() != null && model.getPartitionDesc().getPartitionDateColumnRef() != null) {
            val colRef = model.getPartitionDesc().getPartitionDateColumnRef();
            val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()),
                    SchemaNode.ofPartition(model.getPartitionDesc(), model.getAlias()));
        }

        // fact table
        graph.putEdge(SchemaNode.ofTable(model.getRootFactTable()),
                SchemaNode.ofModelFactTable(model.getRootFactTable(), model.getAlias()));

        for (JoinTableDesc joinTable : model.getJoinTables()) {
            // dim table
            graph.putEdge(SchemaNode.ofTable(joinTable.getTableRef()),
                    SchemaNode.ofModelDimensionTable(joinTable.getTableRef(), model.getAlias()));

            for (int i = 0; i < joinTable.getJoin().getPrimaryKey().length; i++) {
                SchemaNode join = SchemaNode.ofJoin(joinTable.getJoin().getFKSide(), joinTable.getJoin().getPKSide(),
                        joinTable.getJoin(), model.getAlias());

                String fkCol = joinTable.getJoin().getForeignKey()[i];
                val fkNameColumn = nameColumnIdMap.get(fkCol);
                graph.putEdge(SchemaNode.ofModelColumn(fkNameColumn, model.getAlias()), join);

                String pkCol = joinTable.getJoin().getPrimaryKey()[i];
                val pkNameColumn = nameColumnIdMap.get(pkCol);
                graph.putEdge(SchemaNode.ofModelColumn(pkNameColumn, model.getAlias()), join);
            }
        }

        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            collectExprWithModel(model.getFilterCondition(),
                    SchemaNode.ofFilter(model.getAlias(), model.getFilterCondition()));
        }
    }

    private void collectDimensionAndMeasure() {
        model.getEffectiveDimensions().forEach((id, dimension) -> {
            val nameColumn = nameColumnIdMap.get(dimension.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()),
                    SchemaNode.ofDimension(nameColumn, model.getAlias()));
        });
        model.getEffectiveMeasures().forEach((id, measure) -> {
            val params = measure.getFunction().getParameters();
            if (CollectionUtils.isEmpty(params)) {
                return;
            }
            for (ParameterDesc param : params) {
                if (param.isConstant()) {
                    continue;
                }
                val colRef = param.getColRef();
                val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
                graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()),
                        SchemaNode.ofMeasure(measure, model.getAlias()));
            }
        });
    }

    private void collectIndex(List<LayoutEntity> allLayouts, SchemaNodeType type, List<Integer> aggShardByColumns) {
        for (LayoutEntity layout : allLayouts) {
            if (layout.getIndex().isTableIndex()) {
                for (Integer col : layout.getColOrder()) {
                    val namedColumn = effectiveNamedColumns.get(col);
                    if (type == SchemaNodeType.RULE_BASED_INDEX) {
                        graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getAlias()), SchemaNode.ofIndex(type,
                                layout, model, modelColumnMeasureIdNameMap, aggShardByColumns));
                    } else {
                        graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getAlias()),
                                SchemaNode.ofIndex(type, layout, model, modelColumnMeasureIdNameMap));
                    }
                }
                continue;
            }
            for (Integer col : layout.getColOrder()) {
                if (col < NDataModel.MEASURE_ID_BASE) {
                    val namedColumn = effectiveNamedColumns.get(col);
                    if (type == SchemaNodeType.RULE_BASED_INDEX) {
                        graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getAlias()), SchemaNode.ofIndex(type,
                                layout, model, modelColumnMeasureIdNameMap, aggShardByColumns));
                    } else {
                        graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getAlias()),
                                SchemaNode.ofIndex(type, layout, model, modelColumnMeasureIdNameMap));
                    }
                } else {
                    NDataModel.Measure measure = effectiveMeasures.get(col);
                    if (measure == null) {
                        continue;
                    }
                    if (type == SchemaNodeType.RULE_BASED_INDEX) {
                        graph.putEdge(SchemaNode.ofMeasure(measure, model.getAlias()), SchemaNode.ofIndex(type, layout,
                                model, modelColumnMeasureIdNameMap, aggShardByColumns));
                    } else {
                        graph.putEdge(SchemaNode.ofMeasure(measure, model.getAlias()),
                                SchemaNode.ofIndex(type, layout, model, modelColumnMeasureIdNameMap));
                    }
                }
            }
        }
    }

    private void collectExprWithModel(String expr, SchemaNode target) {
        val pairs = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(expr);
        for (Pair<String, String> pair : pairs) {
            graph.putEdge(SchemaNode.ofModelColumn(nameColumnIdMap.get(pair.getFirst() + "." + pair.getSecond()),
                    model.getAlias()), target);
        }
    }

    private void collectAggGroup() {
        val ruleBasedIndex = indexPlan.getRuleBasedIndex();
        if (ruleBasedIndex == null) {
            return;
        }
        int index = 0;
        for (NAggregationGroup aggGroup : ruleBasedIndex.getAggregationGroups()) {
            val aggGroupNode = SchemaNodeType.AGG_GROUP.withKey(model.getAlias() + "/" + index);
            for (Integer col : aggGroup.getIncludes()) {
                val namedColumn = effectiveNamedColumns.get(col);
                graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getAlias()), aggGroupNode);

            }
            for (Integer measure : aggGroup.getMeasures()) {
                graph.putEdge(SchemaNode.ofMeasure(effectiveMeasures.get(measure), model.getAlias()), aggGroupNode);
            }
            index++;
        }
    }

    private void collectIndexPlan() {
        val aggShardNode = SchemaNodeType.INDEX_AGG_SHARD.withKey(model.getAlias());
        collectAggExpertColumns(indexPlan.getAggShardByColumns(), aggShardNode);

        val aggPartitionNode = SchemaNodeType.INDEX_AGG_EXTEND_PARTITION.withKey(model.getAlias());
        collectAggExpertColumns(indexPlan.getExtendPartitionColumns(), aggPartitionNode);
    }

    private void collectAggExpertColumns(List<Integer> cols, SchemaNode node) {
        if (CollectionUtils.isEmpty(cols)) {
            return;
        }
        for (Integer col : cols) {
            val namedColumn = effectiveNamedColumns.get(col);
            graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getAlias()), node);
        }
        for (LayoutEntity layout : indexPlan.getRuleBaseLayouts()) {
            if (layout.getColOrder().containsAll(indexPlan.getAggShardByColumns())) {
                graph.putEdge(node, SchemaNode.ofIndex(SchemaNodeType.RULE_BASED_INDEX, layout, model,
                        modelColumnMeasureIdNameMap));
            }
        }
    }

}
