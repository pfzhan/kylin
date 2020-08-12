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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public Graph<SchemaNode> collect() {
        model = indexPlan.getModel();
        effectiveNamedColumns = model.getEffectiveNamedColumns();
        effectiveCols = model.getEffectiveCols();
        effectiveMeasures = model.getEffectiveMeasures();
        nameColumnIdMap = effectiveNamedColumns.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().getAliasDotColumn(), Map.Entry::getValue));

        collectModelColumns();
        collectModelSignificant();
        collectDimensionAndMeasure();

        collectIndex(indexPlan.getWhitelistLayouts(), SchemaNodeType.WHITE_LIST_INDEX);
        collectIndex(indexPlan.getToBeDeletedIndexes().stream().flatMap(index -> index.getLayouts().stream())
                .collect(Collectors.toList()), SchemaNodeType.TO_BE_DELETED_INDEX);
        collectIndex(indexPlan.getRuleBaseLayouts(), SchemaNodeType.RULE_BASED_INDEX);

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
            graph.putEdge(SchemaNode.ofTableColRef(tblColRef), SchemaNode.ofModelColumn(namedColumn, model.getId()));
        });
        effectiveCols.forEach((id, tblColRef) -> {
            if (!tblColRef.getColumnDesc().isComputedColumn()) {
                return;
            }
            val namedColumn = effectiveNamedColumns.get(id);
            val cc = ccs.get(tblColRef.getName());
            val ccNode = SchemaNode.ofModelCC(cc, model.getId());
            collectExprWithModel(cc.getExpression(), ccNode);
            graph.putEdge(ccNode, SchemaNode.ofModelColumn(namedColumn, model.getId()));
        });
    }

    private void collectModelSignificant() {
        if (model.getPartitionDesc() != null && model.getPartitionDesc().getPartitionDateColumnRef() != null) {
            val colRef = model.getPartitionDesc().getPartitionDateColumnRef();
            val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getId()),
                    SchemaNode.ofPartition(model.getPartitionDesc(), model.getId()));
        }
        for (JoinTableDesc joinTable : model.getJoinTables()) {
            Stream.concat(Stream.of(joinTable.getJoin().getPrimaryKey()),
                    Stream.of(joinTable.getJoin().getForeignKey())).forEach(col -> {
                        val nameColumn = nameColumnIdMap.get(col);
                        graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getId()),
                                SchemaNode.ofJoin(nameColumn, model.getId()));
                    });
        }

        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            collectExprWithModel(model.getFilterCondition(), SchemaNode.ofFilter(model.getId()));
        }
    }

    private void collectDimensionAndMeasure() {
        model.getEffectiveDimensions().forEach((id, dimension) -> {
            val nameColumn = nameColumnIdMap.get(dimension.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getId()),
                    SchemaNode.ofDimension(nameColumn, model.getId()));
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
                graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getId()),
                        SchemaNode.ofMeasure(measure, model.getId()));
            }
        });
    }

    private void collectIndex(List<LayoutEntity> allLayouts, SchemaNodeType type) {
        for (LayoutEntity layout : allLayouts) {
            if (layout.getIndex().isTableIndex()) {
                for (Integer col : layout.getColOrder()) {
                    val namedColumn = effectiveNamedColumns.get(col);
                    graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getId()),
                            SchemaNode.ofIndex(type, layout, model.getId()));
                }
                continue;
            }
            for (Integer col : layout.getColOrder()) {
                if (col < NDataModel.MEASURE_ID_BASE) {
                    val namedColumn = effectiveNamedColumns.get(col);
                    graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getId()),
                            SchemaNode.ofIndex(type, layout, model.getId()));
                } else {
                    graph.putEdge(SchemaNode.ofMeasure(effectiveMeasures.get(col), model.getId()),
                            SchemaNode.ofIndex(type, layout, model.getId()));
                }
            }
        }
    }

    private void collectExprWithModel(String expr, SchemaNode target) {
        val pairs = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(expr);
        for (Pair<String, String> pair : pairs) {
            graph.putEdge(SchemaNodeType.MODEL_COLUMN.withKey(
                    model.getId() + "/" + nameColumnIdMap.get(pair.getFirst() + "." + pair.getSecond()).getId()),
                    target);
        }
    }

    private void collectAggGroup() {
        val ruleBasedIndex = indexPlan.getRuleBasedIndex();
        if (ruleBasedIndex == null) {
            return;
        }
        int index = 0;
        for (NAggregationGroup aggGroup : ruleBasedIndex.getAggregationGroups()) {
            val aggGroupNode = SchemaNodeType.AGG_GROUP.withKey(model.getId() + "/" + index);
            for (Integer col : aggGroup.getIncludes()) {
                val namedColumn = effectiveNamedColumns.get(col);
                graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getId()), aggGroupNode);

            }
            for (Integer measure : aggGroup.getMeasures()) {
                graph.putEdge(SchemaNode.ofMeasure(effectiveMeasures.get(measure), model.getId()), aggGroupNode);
            }
            index++;
        }
    }

    private void collectIndexPlan() {
        val aggShardNode = SchemaNodeType.INDEX_AGG_SHARD.withKey(model.getId());
        collectAggExpertColumns(indexPlan.getAggShardByColumns(), aggShardNode);

        val aggPartitionNode = SchemaNodeType.INDEX_AGG_EXTEND_PARTITION.withKey(model.getId());
        collectAggExpertColumns(indexPlan.getExtendPartitionColumns(), aggPartitionNode);
    }

    private void collectAggExpertColumns(List<Integer> cols, SchemaNode node) {
        if (CollectionUtils.isEmpty(cols)) {
            return;
        }
        for (Integer col : cols) {
            val namedColumn = effectiveNamedColumns.get(col);
            graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getId()), node);
        }
        for (LayoutEntity layout : indexPlan.getRuleBaseLayouts()) {
            if (layout.getColOrder().containsAll(indexPlan.getAggShardByColumns())) {
                graph.putEdge(node, SchemaNode.ofIndex(SchemaNodeType.RULE_BASED_INDEX, layout, model.getId()));
            }
        }
    }

}
