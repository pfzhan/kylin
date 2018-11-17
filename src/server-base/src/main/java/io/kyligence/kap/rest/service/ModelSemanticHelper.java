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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import lombok.var;

@Component
public class ModelSemanticHelper {

    public NDataModel convertToDataModel(ModelRequest modelRequest) throws IOException {
        List<SimplifiedMeasure> simplifiedMeasures = modelRequest.getSimplifiedMeasures();
        NDataModel dataModel = JsonUtil.readValue(JsonUtil.writeValueAsString(modelRequest), NDataModel.class);
        dataModel.setUuid(UUID.randomUUID().toString());
        dataModel.setAllMeasures(convertMeasure(simplifiedMeasures));
        dataModel.setAllNamedColumns(convertNamedColumns(modelRequest.getProject(), dataModel));
        return dataModel;
    }

    private List<NDataModel.NamedColumn> convertNamedColumns(String project, NDataModel dataModel) {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        List<JoinTableDesc> allTables = Lists.newArrayList();
        val rootFactTable = new JoinTableDesc();
        rootFactTable.setTable(dataModel.getRootFactTableName());
        rootFactTable.setAlias(dataModel.getRootFactTableAlias());
        rootFactTable.setKind(NDataModel.TableKind.FACT);
        allTables.add(rootFactTable);
        allTables.addAll(dataModel.getJoinTables());

        List<NDataModel.NamedColumn> simplifiedColumns = dataModel.getAllNamedColumns();
        Map<String, NDataModel.NamedColumn> dimensionNameMap = Maps.newHashMap();
        for (NDataModel.NamedColumn namedColumn : simplifiedColumns) {
            dimensionNameMap.put(namedColumn.getAliasDotColumn(), namedColumn);
        }

        int id = 0;
        List<NDataModel.NamedColumn> columns = Lists.newArrayList();
        for (JoinTableDesc joinTable : allTables) {
            val tableDesc = tableManager.getTableDesc(joinTable.getTable());
            boolean isFact = joinTable.getKind() == NDataModel.TableKind.FACT;
            val alias = StringUtils.isEmpty(joinTable.getAlias()) ? tableDesc.getName() : joinTable.getAlias();
            val tableRef = new TableRef(dataModel, alias, tableDesc, !isFact);
            for (TblColRef column : tableRef.getColumns()) {
                val namedColumn = new NDataModel.NamedColumn();
                namedColumn.setId(id++);
                namedColumn.setName(column.getName());
                namedColumn.setAliasDotColumn(alias + "." + column.getName());
                namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
                val dimension = dimensionNameMap.get(namedColumn.getAliasDotColumn());
                if (dimension != null) {
                    namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                    namedColumn.setName(dimension.getName());
                }
                columns.add(namedColumn);
            }
        }
        for (ComputedColumnDesc computedColumnDesc : dataModel.getComputedColumnDescs()) {
            NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
            namedColumn.setId(id++);
            namedColumn.setName(computedColumnDesc.getColumnName());
            namedColumn.setAliasDotColumn(computedColumnDesc.getFullName());
            namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
            if (dataModel.getAllNamedColumns().stream()
                    .anyMatch(c -> c.getAliasDotColumn().equals(namedColumn.getAliasDotColumn()))) {
                // cc already used as named column
                continue;
            }
            columns.add(namedColumn);
        }
        return columns;
    }

    public void updateModelColumns(NDataModel originModel, ModelRequest request) throws IOException {
        val expectedModel = convertToDataModel(request);
        originModel.setJoinTables(expectedModel.getJoinTables());
        originModel.setCanvas(expectedModel.getCanvas());
        originModel.setRootFactTableName(expectedModel.getRootFactTableName());
        originModel.setRootFactTableAlias(expectedModel.getRootFactTableAlias());
        originModel.setPartitionDesc(expectedModel.getPartitionDesc());

        // compare measures
        Function<List<NDataModel.Measure>, Map<SimplifiedMeasure, NDataModel.Measure>> toMeasureMap = allCols -> allCols
                .stream().filter(m -> !m.tomb)
                .collect(Collectors.toMap(SimplifiedMeasure::fromMeasure, Function.identity()));
        val newMeasures = Lists.<NDataModel.Measure> newArrayList();
        var maxMeasureId = originModel.getAllMeasures().stream().map(NDataModel.Measure::getId).mapToInt(i -> i).max()
                .orElse(NDataModel.MEASURE_ID_BASE - 1);

        compareAndUpdateColumns(toMeasureMap.apply(originModel.getAllMeasures()),
                toMeasureMap.apply(expectedModel.getAllMeasures()), newMeasures::add,
                (oldMeasure) -> oldMeasure.tomb = true,
                (oldMeasure, newMeasure) -> oldMeasure.setName(newMeasure.getName()));
        // one measure in expectedModel but not in originModel then add one
        for (NDataModel.Measure measure : newMeasures) {
            maxMeasureId++;
            measure.id = maxMeasureId;
            originModel.getAllMeasures().add(measure);
        }

        // check CC and add new namedColumn
        int maxColumnId = originModel.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max()
                .orElse(-1) + 1;
        Map<String, NDataModel.NamedColumn> currentNamedColumns = originModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isExist).collect(Collectors.toMap(c -> c.getAliasDotColumn(), c -> c));
        Set<String> newComputedColumns = Sets.newHashSet();
        for (ComputedColumnDesc computedColumnDesc : request.getComputedColumnDescs()) {
            newComputedColumns.add(computedColumnDesc.getFullName());
            if (currentNamedColumns.containsKey(computedColumnDesc.getFullName())) {
                continue;
            }
            // create named column for new CC
            newComputedColumns.add(computedColumnDesc.getFullName());
            NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
            namedColumn.setId(maxColumnId++);
            namedColumn.setName(computedColumnDesc.getColumnName());
            namedColumn.setAliasDotColumn(computedColumnDesc.getFullName());
            namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
            originModel.getAllNamedColumns().add(namedColumn);
        }

        Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toExistMap = allCols -> allCols
                .stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(k -> k.getAliasDotColumn(), Function.identity()));

        // compare originModel and expectedModel's existing allNamedColumn
        val originExistMap = toExistMap.apply(originModel.getAllNamedColumns());
        val newCols = Lists.<NDataModel.NamedColumn> newArrayList();
        compareAndUpdateColumns(originExistMap, toExistMap.apply(expectedModel.getAllNamedColumns()), newCols::add,
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.TOMB), (olCol, newCol) -> olCol.setName(newCol.getName()));
        int maxId = originModel.getAllNamedColumns().stream().map(c -> c.getId()).mapToInt(i -> i).max().orElse(-1);
        for (NDataModel.NamedColumn newCol : newCols) {
            maxId++;
            newCol.setId(maxId);
            originModel.getAllNamedColumns().add(newCol);
        }

        // compare originModel and expectedModel's dimensions
        Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toDimensionMap = allCols -> allCols
                .stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toMap(k -> k.getAliasDotColumn(), Function.identity()));
        val originDimensionMap = toDimensionMap.apply(originModel.getAllNamedColumns());
        compareAndUpdateColumns(originDimensionMap, toDimensionMap.apply(expectedModel.getAllNamedColumns()),
                newCol -> originExistMap.get(newCol.getAliasDotColumn()).setStatus(NDataModel.ColumnStatus.DIMENSION),
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.EXIST), (olCol, newCol) -> olCol.setName(newCol.getName()));

        // Move deleted computed column to TOMB status
        Set<String> currentComputedColumns = originModel.getComputedColumnDescs().stream()
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toSet());
        originModel.getAllNamedColumns().stream()
                .filter(column -> currentComputedColumns.contains(column.getAliasDotColumn()))
                .filter(column -> !newComputedColumns.contains(column.getAliasDotColumn()))
                .forEach(unusedColumn -> unusedColumn.setStatus(NDataModel.ColumnStatus.TOMB));
        originModel.setComputedColumnDescs(request.getComputedColumnDescs());

        //Move unused named column to EXIST status
        originModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .filter(column -> request.getDimensions().stream()
                        .noneMatch(dimension -> dimension.getAliasDotColumn().equals(column.getAliasDotColumn())))
                .forEach(c -> c.setStatus(NDataModel.ColumnStatus.EXIST));
    }

    private <K, T> void compareAndUpdateColumns(Map<K, T> origin, Map<K, T> target, Consumer<T> onlyInTarget,
            Consumer<T> onlyInOrigin, BiConsumer<T, T> inBoth) {
        for (Map.Entry<K, T> entry : target.entrySet()) {
            // change name does not matter
            val matched = origin.get(entry.getKey());
            if (matched == null) {
                onlyInTarget.accept(entry.getValue());
            } else {
                inBoth.accept(matched, entry.getValue());
                //                matched.name = entry.getValue().name;
            }
        }
        for (Map.Entry<K, T> entry : origin.entrySet()) {
            val matched = target.get(entry.getKey());
            if (matched == null) {
                onlyInOrigin.accept(entry.getValue());
            }
        }

    }

    private List<NDataModel.Measure> convertMeasure(List<SimplifiedMeasure> simplifiedMeasures) {
        List<NDataModel.Measure> measures = new ArrayList<>();
        boolean hasCount = false;
        int id = NDataModel.MEASURE_ID_BASE;
        if (simplifiedMeasures == null) {
            simplifiedMeasures = Lists.newArrayList();
        }
        for (SimplifiedMeasure simplifiedMeasure : simplifiedMeasures) {
            // TODO just check count(*), update this logic when count(col) is implemented
            val measure = simplifiedMeasure.toMeasure();
            measure.id = id;
            measures.add(measure);
            val functionDesc = measure.getFunction();
            if (functionDesc.isCount()) {
                hasCount = true;
            }
            id++;
        }
        if (!hasCount) {
            FunctionDesc functionDesc = new FunctionDesc();
            ParameterDesc parameterDesc = new ParameterDesc();
            parameterDesc.setType("constant");
            parameterDesc.setValue("1");
            functionDesc.setParameter(parameterDesc);
            functionDesc.setExpression("COUNT");
            functionDesc.setReturnType("bigint");
            NDataModel.Measure measure = CubeUtils.newMeasure(functionDesc, "COUNT_ALL", id);
            measures.add(measure);
        }
        return measures;
    }
}
