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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Service;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.smart.util.CubeUtils;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ModelSemanticHelper extends BasicService {

    public NDataModel convertToDataModel(ModelRequest modelRequest) {
        try {
            List<SimplifiedMeasure> simplifiedMeasures = modelRequest.getSimplifiedMeasures();
            NDataModel dataModel = JsonUtil.readValue(JsonUtil.writeValueAsString(modelRequest), NDataModel.class);
            dataModel.setUuid(modelRequest.getUuid() != null ? modelRequest.getUuid() : UUID.randomUUID().toString());
            dataModel.setAllMeasures(convertMeasure(simplifiedMeasures));
            dataModel.setAllNamedColumns(convertNamedColumns(modelRequest.getProject(), dataModel));
            return dataModel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            val dimension = dimensionNameMap.get(namedColumn.getAliasDotColumn());
            if (dimension != null) {
                namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                namedColumn.setName(dimension.getName());
            }
            columns.add(namedColumn);
        }
        return columns;
    }

    public void updateModelColumns(NDataModel originModel, ModelRequest request) {
        val expectedModel = convertToDataModel(request);
        originModel.setJoinTables(expectedModel.getJoinTables());
        originModel.setCanvas(expectedModel.getCanvas());
        originModel.setRootFactTableName(expectedModel.getRootFactTableName());
        originModel.setRootFactTableAlias(expectedModel.getRootFactTableAlias());
        originModel.setPartitionDesc(expectedModel.getPartitionDesc());

        // handle computed column updates
        List<ComputedColumnDesc> currentComputedColumns = originModel.getComputedColumnDescs();
        List<ComputedColumnDesc> newComputedColumns = expectedModel.getComputedColumnDescs();
        Set<String> removedOrUpdatedComputedColumns = currentComputedColumns.stream()
                .filter(cc -> !newComputedColumns.contains(cc)).map(ComputedColumnDesc::getFullName)
                .collect(Collectors.toSet());
        // move deleted CC's named column to TOMB
        originModel.getAllNamedColumns().stream()
                .filter(column -> removedOrUpdatedComputedColumns.contains(column.getAliasDotColumn()))
                .forEach(unusedColumn -> unusedColumn.setStatus(NDataModel.ColumnStatus.TOMB));
        // move deleted CC's measure to TOMB
        List<Measure> currentMeasures = originModel.getEffectiveMeasures().values().asList();
        currentMeasures.stream().filter(measure -> {
            List<TblColRef> params = measure.getFunction().getParameter().getColRefs();
            if (CollectionUtils.isEmpty(params)) {
                return false;
            }
            return params.stream().map(TblColRef::getIdentity).anyMatch(removedOrUpdatedComputedColumns::contains);
        }).forEach(unusedMeasure -> unusedMeasure.tomb = true);
        originModel.setComputedColumnDescs(expectedModel.getComputedColumnDescs());

        // compare measures
        Function<List<NDataModel.Measure>, Map<SimplifiedMeasure, NDataModel.Measure>> toMeasureMap = allCols -> allCols
                .stream().filter(m -> !m.tomb)
                .collect(Collectors.toMap(SimplifiedMeasure::fromMeasure, Function.identity()));
        val newMeasures = Lists.<NDataModel.Measure> newArrayList();
        var maxMeasureId = originModel.getAllMeasures().stream().map(NDataModel.Measure::getId).mapToInt(i -> i).max()
                .orElse(NDataModel.MEASURE_ID_BASE - 1);

        compareAndUpdateColumns(toMeasureMap.apply(originModel.getAllMeasures()),
                toMeasureMap.apply(expectedModel.getAllMeasures()), newMeasures::add,
                oldMeasure -> oldMeasure.tomb = true,
                (oldMeasure, newMeasure) -> oldMeasure.setName(newMeasure.getName()));
        // one measure in expectedModel but not in originModel then add one
        for (NDataModel.Measure measure : newMeasures) {
            maxMeasureId++;
            measure.id = maxMeasureId;
            originModel.getAllMeasures().add(measure);
        }

        Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toExistMap = allCols -> allCols
                .stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, Function.identity()));

        // compare originModel and expectedModel's existing allNamedColumn
        val originExistMap = toExistMap.apply(originModel.getAllNamedColumns());
        val newCols = Lists.<NDataModel.NamedColumn> newArrayList();
        compareAndUpdateColumns(originExistMap, toExistMap.apply(expectedModel.getAllNamedColumns()), newCols::add,
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.TOMB),
                (olCol, newCol) -> olCol.setName(newCol.getName()));
        int maxId = originModel.getAllNamedColumns().stream().map(NamedColumn::getId).mapToInt(i -> i).max().orElse(-1);
        for (NDataModel.NamedColumn newCol : newCols) {
            maxId++;
            newCol.setId(maxId);
            originModel.getAllNamedColumns().add(newCol);
        }

        // compare originModel and expectedModel's dimensions
        Function<List<NDataModel.NamedColumn>, Map<String, NDataModel.NamedColumn>> toDimensionMap = allCols -> allCols
                .stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, Function.identity()));
        val originDimensionMap = toDimensionMap.apply(originModel.getAllNamedColumns());
        compareAndUpdateColumns(originDimensionMap, toDimensionMap.apply(expectedModel.getAllNamedColumns()),
                newCol -> originExistMap.get(newCol.getAliasDotColumn()).setStatus(NDataModel.ColumnStatus.DIMENSION),
                oldCol -> oldCol.setStatus(NDataModel.ColumnStatus.EXIST),
                (olCol, newCol) -> olCol.setName(newCol.getName()));

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
        boolean hasCountAll = false;
        int id = NDataModel.MEASURE_ID_BASE;
        if (simplifiedMeasures == null) {
            simplifiedMeasures = Lists.newArrayList();
        }
        for (SimplifiedMeasure simplifiedMeasure : simplifiedMeasures) {
            val measure = simplifiedMeasure.toMeasure();
            measure.id = id;
            measures.add(measure);
            val functionDesc = measure.getFunction();
            if (functionDesc.isCount() && !functionDesc.isCountOnColumn()) {
                hasCountAll = true;
            }
            id++;
        }
        if (!hasCountAll) {
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

    public void handleSemanticUpdate(String project, String model, NDataModel originModel) {
        val config = KylinConfig.getInstanceFromEnv();
        val indePlanManager = NIndexPlanManager.getInstance(config, project);
        val modelMgr = NDataModelManager.getInstance(config, project);
        val dataflowManager = NDataflowManager.getInstance(config, project);

        val indexPlan = indePlanManager.getIndexPlan(model);
        val newModel = modelMgr.getDataModelDesc(model);

        if (isSignificantChange(originModel, newModel)) {
            handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasureMap().keySet(), IndexPlan::setRuleBasedIndex,
                    indePlanManager);
            // do not need to fire this event, the follow logic will clear all segments
            removeUselessDimensions(indexPlan, newModel.getEffectiveDimenionsMap().keySet(), false, config);
            modelMgr.updateDataModel(newModel.getUuid(),
                    copyForWrite -> copyForWrite.setSemanticVersion(copyForWrite.getSemanticVersion() + 1));
            handleReloadData(newModel, originModel, dataflowManager, config, project);
            return;
        }
        val dimensionsOnlyAdded = newModel.getEffectiveDimenionsMap().keySet()
                .containsAll(originModel.getEffectiveDimenionsMap().keySet());
        val measuresNotChanged = CollectionUtils.isEqualCollection(newModel.getEffectiveMeasureMap().keySet(),
                originModel.getEffectiveMeasureMap().keySet());
        if (dimensionsOnlyAdded && measuresNotChanged) {
            return;
        }
        // measure changed: does not matter to auto created cuboids' data, need refresh rule based cuboids
        if (!measuresNotChanged) {
            val oldRule = indexPlan.getRuleBasedIndex();
            handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasureMap().keySet(), (copyForWrite, rule) -> {
                copyForWrite.setRuleBasedIndex(rule);
                handleCubeUpdateRule(project, model, oldRule, rule);
            }, indePlanManager);
        }
        // dimension deleted: previous step is remove dimensions in rule,
        //   so we only remove the auto created cuboids
        if (!dimensionsOnlyAdded) {
            removeUselessDimensions(indexPlan, newModel.getEffectiveDimenionsMap().keySet(), true, config);
        }
    }

    // if partitionDesc, mpCol, joinTable changed, we need reload data from datasource
    private boolean isSignificantChange(NDataModel originModel, NDataModel newModel) {
        return !Objects.equals(originModel.getPartitionDesc(), newModel.getPartitionDesc())
                || !Objects.equals(originModel.getMpColStrs(), newModel.getMpColStrs())
                || !Objects.equals(originModel.getJoinTables(), newModel.getJoinTables());
    }

    private boolean handleMeasuresChanged(IndexPlan cube, Set<Integer> measures,
            BiConsumer<IndexPlan, NRuleBasedIndex> descConsumer, NIndexPlanManager indexPlanManager) {
        val savedCube = indexPlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream().filter(cuboid -> {
                val allMeasures = cuboid.getMeasures();
                return measures.containsAll(allMeasures);
            }).collect(Collectors.toList()));
            if (copyForWrite.getRuleBasedIndex() == null) {
                return;
            }
            try {
                val newRule = JsonUtil.deepCopy(copyForWrite.getRuleBasedIndex(), NRuleBasedIndex.class);
                newRule.setMeasures(Lists.newArrayList(measures));
                newRule.setLayoutIdMapping(Lists.newArrayList());
                descConsumer.accept(copyForWrite, newRule);
            } catch (IOException e) {
                log.warn("copy rule failed ", e);
            }
        });
        return savedCube.getRuleBasedIndex() != null;
    }

    private void removeUselessDimensions(IndexPlan cube, Set<Integer> availableDimensions, boolean triggerEvent,
            KylinConfig config) {
        val indexPlanManager = NIndexPlanManager.getInstance(config, cube.getProject());
        val dataflowManager = NDataflowManager.getInstance(config, cube.getProject());
        val layoutIds = cube.getWhitelistLayouts().stream()
                .filter(layout -> !layout.getIndex().isTableIndex())
                .filter(layout -> layout.getColOrder().stream()
                        .anyMatch(col -> col < NDataModel.MEASURE_ID_BASE && !availableDimensions.contains(col)))
                .map(LayoutEntity::getId).collect(Collectors.toSet());
        if (layoutIds.isEmpty()) {
            return;
        }
        if (triggerEvent) {
            indexPlanManager.updateIndexPlan(cube.getUuid(),
                    copyForWrite -> copyForWrite.removeLayouts(layoutIds, LayoutEntity::equals, false, true));
            val df = dataflowManager.getDataflow(cube.getUuid());
            dataflowManager.removeLayouts(df, layoutIds);
        } else {
            indexPlanManager.updateIndexPlan(cube.getUuid(),
                    copy -> copy.setIndexes(copy.getIndexes().stream()
                            .filter(cuboid -> availableDimensions.containsAll(cuboid.getDimensions()))
                            .collect(Collectors.toList())));
        }
    }

    private void handleReloadData(NDataModel model, NDataModel oriModel, NDataflowManager dataflowManager,
            KylinConfig config, String project) {
        var df = dataflowManager.getDataflow(model.getUuid());
        val segments = df.getFlatSegments();
        df = dataflowManager.updateDataflow(df.getUuid(), copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        List<SegmentRange> ranges = Lists.newArrayList();
        if (model.getPartitionDesc() == null) {
            //full load
            ranges.add(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        } else if (Objects.equals(model.getPartitionDesc(), oriModel.getPartitionDesc())) {
            for (val seg : segments) {
                ranges.add(seg.getSegRange());
            }
        }

        dataflowManager.fillDfManually(df, ranges);

        EventManager eventManager = EventManager.getInstance(config, project);

        AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setModelId(model.getUuid());
        addCuboidEvent.setJobId(UUID.randomUUID().toString());
        addCuboidEvent.setOwner(getUsername());
        eventManager.post(addCuboidEvent);

        PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
        postAddCuboidEvent.setModelId(model.getUuid());
        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
        postAddCuboidEvent.setOwner(getUsername());
        eventManager.post(postAddCuboidEvent);
    }

    public void handleCubeUpdateRule(String project, String model, NRuleBasedIndex oldRule, NRuleBasedIndex newRule) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val eventManager = EventManager.getInstance(kylinConfig, project);

        val originLayouts = oldRule == null ? Sets.<LayoutEntity> newHashSet() : oldRule.genCuboidLayouts();
        val targetLayouts = newRule.genCuboidLayouts();

        val difference = Maps.difference(Maps.asMap(originLayouts, Functions.identity()),
                Maps.asMap(targetLayouts, Functions.identity()));

        // new cuboid
        if (difference.entriesOnlyOnRight().size() > 0) {
            AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
            addCuboidEvent.setModelId(model);
            addCuboidEvent.setJobId(UUID.randomUUID().toString());
            addCuboidEvent.setOwner(getUsername());
            eventManager.post(addCuboidEvent);

            PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
            postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
            postAddCuboidEvent.setModelId(model);
            postAddCuboidEvent.setOwner(getUsername());
            eventManager.post(postAddCuboidEvent);
        }

    }

}
