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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import lombok.experimental.Delegate;

@Data
public class SchemaNode {

    @NonNull
    @Delegate
    SchemaNodeType type;

    @NonNull
    final String key;

    @Setter(value = AccessLevel.PRIVATE)
    Map<String, Object> attributes;

    @Setter(value = AccessLevel.PRIVATE)
    List<String> ignoreAttributes;

    @Setter(value = AccessLevel.PRIVATE)
    Map<String, Object> keyAttributes;

    private final int hashcode;

    public SchemaNode(SchemaNodeType type, String key) {
        this(type, key, Maps.newHashMap());
    }

    public SchemaNode(SchemaNodeType type, String key, Map<String, Object> attributes, String... ignore) {
        this.type = type;
        this.key = key;
        this.attributes = attributes;
        ignoreAttributes = Arrays.asList(ignore);
        keyAttributes = attributes.keySet().stream().filter(attribute -> !ignoreAttributes.contains(attribute))
                .collect(Collectors.toMap(Function.identity(), attributes::get));
        hashcode = Objects.hash(key);
    }

    /**
     * table columns node with identity as {DATABASE NAME}.{TABLE NAME}.{COLUMN NAME}
     * @param columnDesc
     * @return
     */
    public static SchemaNode ofTableColumn(ColumnDesc columnDesc) {
        return new SchemaNode(SchemaNodeType.TABLE_COLUMN,
                columnDesc.getTable().getIdentity() + "." + columnDesc.getName(),
                ImmutableMap.of("datatype", columnDesc.getDatatype()));
    }

    /**
     * table node with identity as {DATABASE NAME}.{TABLE NAME}
     * @param tableDesc
     * @return
     */
    public static SchemaNode ofTable(TableDesc tableDesc) {
        return new SchemaNode(SchemaNodeType.MODEL_TABLE, tableDesc.getIdentity());
    }

    /**
     * table node with identity as {DATABASE NAME}.{TABLE NAME}
     * @param tableRef
     * @return
     */
    public static SchemaNode ofTable(TableRef tableRef) {
        return new SchemaNode(SchemaNodeType.MODEL_TABLE, tableRef.getTableIdentity());
    }

    public static SchemaNode ofModelFactTable(TableRef tableRef, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_FACT, modelAlias + "/" + tableRef.getAlias());
    }

    public static SchemaNode ofModelDimensionTable(TableRef tableRef, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_DIM, modelAlias + "/" + tableRef.getAlias());
    }

    public static SchemaNode ofModelColumn(NDataModel.NamedColumn namedColumn, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_COLUMN, modelAlias + "/" + namedColumn.getName(),
                ImmutableMap.of("id", String.valueOf(namedColumn.getId())), "id");
    }

    public static SchemaNode ofModelCC(ComputedColumnDesc cc, String modelAlias, String factTable) {
        return new SchemaNode(SchemaNodeType.MODEL_CC, modelAlias + "/" + cc.getColumnName(),
                ImmutableMap.of("expression", cc.getExpression().replace("\r\n", "\n"), "fact_table", factTable),
                "fact_table");
    }

    public static SchemaNode ofDimension(NDataModel.NamedColumn namedColumn, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_DIMENSION, modelAlias + "/" + namedColumn.getName(),
                ImmutableMap.of("name", namedColumn.getName(), "alias_dot_column", namedColumn.getAliasDotColumn(),
                        "id", String.valueOf(namedColumn.getId())),
                "id");
    }

    public static SchemaNode ofMeasure(NDataModel.Measure measure, String modelAlias) {
        List<FunctionParameter> parameters = new ArrayList<>();
        if (measure.getFunction().getParameters() != null) {
            parameters = measure.getFunction().getParameters().stream()
                    .map(parameterDesc -> new FunctionParameter(parameterDesc.getType(), parameterDesc.getValue()))
                    .collect(Collectors.toList());
        }
        return new SchemaNode(SchemaNodeType.MODEL_MEASURE, modelAlias + "/" + measure.getName(),
                ImmutableMap.of("name", measure.getName(), "expression", measure.getFunction().getExpression(),
                        "returntype", measure.getFunction().getReturnType(), "parameters", parameters, "id",
                        String.valueOf(measure.getId())),
                "id");
    }

    public static SchemaNode ofPartition(PartitionDesc partitionDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_PARTITION, modelAlias,
                ImmutableMap.of("column", partitionDesc.getPartitionDateColumn(), "format",
                        partitionDesc.getPartitionDateFormat() != null ? partitionDesc.getPartitionDateFormat() : ""));
    }

    public static SchemaNode ofMultiplePartition(MultiPartitionDesc multiPartitionDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_MULTIPLE_PARTITION, modelAlias,
                ImmutableMap.of("columns", multiPartitionDesc.getColumns(), "partitions",
                        multiPartitionDesc.getPartitions().stream().map(MultiPartitionDesc.PartitionInfo::getValues)
                                .map(Arrays::asList).collect(Collectors.toList())));
    }

    // join type
    public static SchemaNode ofJoin(JoinTableDesc joinTableDesc, TableRef fkTableRef, TableRef pkTableRef,
            JoinDesc joinDesc, String modelAlias) {
        return new SchemaNode(SchemaNodeType.MODEL_JOIN,
                modelAlias + "/" + fkTableRef.getAlias() + "-" + pkTableRef.getAlias(),
                ImmutableMap.<String, Object> builder()
                        .put("join_relation_type", joinTableDesc.getJoinRelationTypeEnum())
                        .put("primary_table", pkTableRef.getAlias()).put("foreign_table", fkTableRef.getAlias())
                        .put("join_type", joinDesc.getType())
                        .put("primary_keys", Arrays.asList(joinDesc.getPrimaryKey()))
                        .put("foreign_keys", Arrays.asList(joinDesc.getForeignKey()))
                        .put("non_equal_join_condition",
                                joinDesc.getNonEquiJoinCondition() != null
                                        ? joinDesc.getNonEquiJoinCondition().getExpr()
                                        : "")
                        .build());
    }

    public static SchemaNode ofFilter(String modelAlias, String condition) {
        return new SchemaNode(SchemaNodeType.MODEL_FILTER, modelAlias, ImmutableMap.of("condition", condition));
    }

    public static SchemaNode ofIndex(SchemaNodeType type, LayoutEntity layout, NDataModel model,
            Map<Integer, String> modelColumnMeasureIdMap, List<Integer> aggShardByColumns) {
        val colOrders = getLayoutIdColumn(layout, modelColumnMeasureIdMap);
        val shardBy = aggShardByColumns == null ? getLayoutShardByColumn(layout, modelColumnMeasureIdMap)
                : getColumnMeasureName(aggShardByColumns, modelColumnMeasureIdMap);
        val sortBy = getLayoutSortByColumn(layout, modelColumnMeasureIdMap);
        val key = model.getAlias() + "/" + String.join(",", colOrders) //
                + "/" + String.join(",", shardBy) //
                + "/" + String.join(",", sortBy);

        return new SchemaNode(type, key, ImmutableMap.of("col_orders", colOrders, "shard_by", shardBy, "sort_by",
                sortBy, "id", String.valueOf(layout.getId())), "id");
    }

    private static List<String> getLayoutIdColumn(LayoutEntity layout, Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getColOrder(), modelColumnMeasureIdMap);
    }

    private static List<String> getLayoutShardByColumn(LayoutEntity layout,
            Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getShardByColumns(), modelColumnMeasureIdMap);
    }

    private static List<String> getLayoutSortByColumn(LayoutEntity layout,
            Map<Integer, String> modelColumnMeasureIdMap) {
        return getColumnMeasureName(layout.getSortByColumns(), modelColumnMeasureIdMap);
    }

    private static List<String> getColumnMeasureName(List<Integer> columnIds,
            Map<Integer, String> modelColumnMeasureIdMap) {
        if (columnIds == null) {
            return Lists.newArrayList();
        }

        return columnIds.stream().map(modelColumnMeasureIdMap::get).collect(Collectors.toList());
    }

    public String getSubject() {
        return type.getSubject(key);
    }

    public String getDetail() {
        return type.getDetail(key, attributes);
    }

    public SchemaNodeIdentifier getIdentifier() {
        return new SchemaNodeIdentifier(this.getType(), this.getKey());
    }

    @Data
    @AllArgsConstructor
    public static class SchemaNodeIdentifier {
        private SchemaNodeType type;
        private String key;
    }

    @Data
    @AllArgsConstructor
    public static class FunctionParameter {
        private String type;
        private String value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SchemaNode that = (SchemaNode) o;
        return type == that.type && Objects.equals(key, that.key)
                && Objects.equals(this.keyAttributes, that.keyAttributes);
    }

    @Override
    public int hashCode() {
        //        return Objects.hash(type, key, this.keyAttributes);
        return hashcode;
    }
}
