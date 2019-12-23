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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.SourceFactory;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.ModifyTableNameSqlVisitor;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
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
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
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
            dataModel.setProject(modelRequest.getProject());
            dataModel.setAllMeasures(convertMeasure(simplifiedMeasures));
            dataModel.setAllNamedColumns(convertNamedColumns(modelRequest.getProject(), dataModel, modelRequest));
            return dataModel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<NDataModel.NamedColumn> convertNamedColumns(String project, NDataModel dataModel,
            ModelRequest modelRequest) {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        List<JoinTableDesc> allTables = Lists.newArrayList();
        val rootFactTable = new JoinTableDesc();
        rootFactTable.setTable(dataModel.getRootFactTableName());
        rootFactTable.setAlias(dataModel.getRootFactTableAlias());
        rootFactTable.setKind(NDataModel.TableKind.FACT);
        allTables.add(rootFactTable);
        allTables.addAll(dataModel.getJoinTables());

        List<NDataModel.NamedColumn> simplifiedColumns = modelRequest.getSimplifiedDimensions();
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
            for (ColumnDesc column : modelRequest.getColumnsFetcher().apply(tableDesc, !isFact)) {
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

    private void updateModelColumnForTableAliasModify(NDataModel model, Map<String, String> matchAlias) {
        val recommendationManager = OptimizeRecommendationManager.getInstance(getConfig(), model.getProject());
        String modelId = model.getUuid();
        for (val kv : matchAlias.entrySet()) {
            String oldAliasName = kv.getKey();
            String newAliasName = kv.getValue();
            if (oldAliasName.equalsIgnoreCase(newAliasName)) {
                continue;
            }

            model.getAllNamedColumns().stream().filter(NamedColumn::isExist)
                    .forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));
            model.getAllMeasures().stream().filter(x -> !x.isTomb())
                    .forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));
            model.getComputedColumnDescs().forEach(x -> x.changeTableAlias(oldAliasName, newAliasName));

            recommendationManager.handleTableAliasModify(modelId, oldAliasName, newAliasName);

            String filterCondition = model.getFilterCondition();
            if (StringUtils.isNotEmpty(filterCondition)) {
                SqlVisitor<Object> modifyAlias = new ModifyTableNameSqlVisitor(oldAliasName, newAliasName);
                SqlNode sqlNode = CalciteParser.getExpNode(filterCondition);
                sqlNode.accept(modifyAlias);
                String newFilterCondition = sqlNode.toSqlString(HiveSqlDialect.DEFAULT).toString();
                model.setFilterCondition(newFilterCondition);
            }
        }
    }

    private Map<String, String> getAliasTransformMap(NDataModel originModel, NDataModel expectModel) {
        Map<String, String> matchAlias = Maps.newHashMap();
        boolean match = originModel.getJoinsGraph().match(expectModel.getJoinsGraph(), matchAlias);
        if (!match) {
            matchAlias.clear();
        }
        return matchAlias;
    }

    public void updateModelColumns(NDataModel originModel, ModelRequest request) {
        val expectedModel = convertToDataModel(request);

        val allTables = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject())
                .getAllTablesMap();
        val initedAllTables = expectedModel.getExtendedTables(allTables);
        expectedModel.init(KylinConfig.getInstanceFromEnv(), initedAllTables);
        Map<String, String> matchAlias = getAliasTransformMap(originModel, expectedModel);
        updateModelColumnForTableAliasModify(expectedModel, matchAlias);

        expectedModel.init(KylinConfig.getInstanceFromEnv(), allTables,
                getDataflowManager(request.getProject()).listUnderliningDataModels(), request.getProject());

        originModel.setJoinTables(expectedModel.getJoinTables());
        originModel.setCanvas(expectedModel.getCanvas());
        originModel.setRootFactTableName(expectedModel.getRootFactTableName());
        originModel.setRootFactTableAlias(expectedModel.getRootFactTableAlias());
        originModel.setPartitionDesc(expectedModel.getPartitionDesc());
        originModel.setFilterCondition(expectedModel.getFilterCondition());
        updateModelColumnForTableAliasModify(originModel, matchAlias);

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
            List<TblColRef> params = measure.getFunction().getColRefs();
            if (CollectionUtils.isEmpty(params)) {
                return false;
            }
            return params.stream().map(TblColRef::getIdentity).anyMatch(removedOrUpdatedComputedColumns::contains);
        }).forEach(unusedMeasure -> unusedMeasure.setTomb(true));
        originModel.setComputedColumnDescs(expectedModel.getComputedColumnDescs());

        // compare measures
        Function<List<NDataModel.Measure>, Map<SimplifiedMeasure, NDataModel.Measure>> toMeasureMap = allCols -> allCols
                .stream().filter(m -> !m.isTomb())
                .collect(Collectors.toMap(SimplifiedMeasure::fromMeasure, Function.identity(), (u, v) -> {
                    throw new IllegalArgumentException(
                            String.format(MsgPicker.getMsg().getDUPLICATE_MEASURE_DEFINITION(), v.getName()));
                }));
        val newMeasures = Lists.<NDataModel.Measure> newArrayList();
        var maxMeasureId = originModel.getAllMeasures().stream().map(NDataModel.Measure::getId).mapToInt(i -> i).max()
                .orElse(NDataModel.MEASURE_ID_BASE - 1);

        compareAndUpdateColumns(toMeasureMap.apply(originModel.getAllMeasures()),
                toMeasureMap.apply(expectedModel.getAllMeasures()), newMeasures::add,
                oldMeasure -> oldMeasure.setTomb(true),
                (oldMeasure, newMeasure) -> oldMeasure.setName(newMeasure.getName()));
        // one measure in expectedModel but not in originModel then add one
        for (NDataModel.Measure measure : newMeasures) {
            maxMeasureId++;
            measure.setId(maxMeasureId);
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
                .filter(column -> request.getSimplifiedDimensions().stream()
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
            measure.setId(id);
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
            functionDesc.setParameters(Lists.newArrayList(parameterDesc));
            functionDesc.setExpression("COUNT");
            functionDesc.setReturnType("bigint");
            NDataModel.Measure measure = CubeUtils.newMeasure(functionDesc, "COUNT_ALL", id);
            measures.add(measure);
        }
        return measures;
    }

    public void handleSemanticUpdate(String project, String model, NDataModel originModel, String start, String end) {
        val config = KylinConfig.getInstanceFromEnv();
        val indePlanManager = NIndexPlanManager.getInstance(config, project);
        val modelMgr = NDataModelManager.getInstance(config, project);
        val recommendationManager = OptimizeRecommendationManager.getInstance(config, project);

        val indexPlan = indePlanManager.getIndexPlan(model);
        val newModel = modelMgr.getDataModelDesc(model);

        if (isSignificantChange(originModel, newModel)) {
            log.info("model { " + originModel.getAlias() + " } reload data from datasource");
            val savedIndexPlan = handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasureMap().keySet(),
                    indePlanManager);
            removeUselessDimensions(savedIndexPlan, newModel.getEffectiveDimenionsMap().keySet(), false, config);
            modelMgr.updateDataModel(newModel.getUuid(),
                    copyForWrite -> copyForWrite.setSemanticVersion(copyForWrite.getSemanticVersion() + 1));
            handleReloadData(newModel, originModel, project, start, end);
            recommendationManager.cleanAll(model);
            return;
        } else {
            // check agg group contains removed dimensions
            val rule = indexPlan.getRuleBasedIndex();
            if (rule != null) {
                if (!newModel.getEffectiveDimenionsMap().keySet().containsAll(rule.getDimensions())) {
                    val allDimensions = rule.getDimensions();
                    val dimensionNames = allDimensions.stream()
                            .filter(id -> !newModel.getEffectiveDimenionsMap().containsKey(id))
                            .map(originModel::getColumnNameByColumnId).collect(Collectors.toList());

                    throw new IllegalStateException("model " + indexPlan.getModel().getUuid()
                            + "'s agg group still contains dimensions " + StringUtils.join(dimensionNames, ","));
                }

                for (NAggregationGroup agg : rule.getAggregationGroups()) {
                    if (!newModel.getEffectiveMeasureMap().keySet().containsAll(Sets.newHashSet(agg.getMeasures()))) {
                        val measureNames = Arrays.stream(agg.getMeasures())
                                .filter(measureId -> !newModel.getEffectiveMeasureMap().containsKey(measureId))
                                .map(originModel::getMeasureNameByMeasureId).collect(Collectors.toList());

                        throw new IllegalStateException("model " + indexPlan.getModel().getUuid()
                                + "'s agg group still contains measures " + measureNames);
                    }

                }
            }
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
            handleMeasuresChanged(indexPlan, newModel.getEffectiveMeasureMap().keySet(), indePlanManager);
            val newIndexPlan = indePlanManager.getIndexPlan(indexPlan.getId());
            if (newIndexPlan.getRuleBasedIndex() != null) {
                handleIndexPlanUpdateRule(project, model, oldRule, newIndexPlan.getRuleBasedIndex(), false);
            }
        }
        // dimension deleted: previous step is remove dimensions in rule,
        //   so we only remove the auto created cuboids
        if (!dimensionsOnlyAdded) {
            removeUselessDimensions(indexPlan, newModel.getEffectiveDimenionsMap().keySet(), true, config);
        }
    }

    public boolean isFilterConditonNotChange(String oldFilterCondition, String newFilterCondition) {
        oldFilterCondition = oldFilterCondition == null ? "" : oldFilterCondition;
        newFilterCondition = newFilterCondition == null ? "" : newFilterCondition;
        return StringUtils.trim(oldFilterCondition).equals(StringUtils.trim(newFilterCondition));
    }

    // if partitionDesc, mpCol, joinTable, FilterCondition changed, we need reload data from datasource
    private boolean isSignificantChange(NDataModel originModel, NDataModel newModel) {
        return !Objects.equals(originModel.getPartitionDesc(), newModel.getPartitionDesc())
                || !Objects.equals(originModel.getMpColStrs(), newModel.getMpColStrs())
                || !originModel.getJoinsGraph().match(newModel.getJoinsGraph(), Maps.newHashMap())
                || !isFilterConditonNotChange(originModel.getFilterCondition(), newModel.getFilterCondition());
    }

    private IndexPlan handleMeasuresChanged(IndexPlan indexPlan, Set<Integer> measures,
            NIndexPlanManager indexPlanManager) {
        return indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream()
                    .filter(index -> measures.containsAll(index.getMeasures())).collect(Collectors.toList()));
            if (copyForWrite.getRuleBasedIndex() == null) {
                return;
            }
            val newRule = JsonUtil.deepCopyQuietly(copyForWrite.getRuleBasedIndex(), NRuleBasedIndex.class);
            newRule.setLayoutIdMapping(Lists.newArrayList());

            if (newRule.getAggregationGroups() != null) {
                for (NAggregationGroup aggGroup : newRule.getAggregationGroups()) {
                    val aggMeasures = Sets.newHashSet(aggGroup.getMeasures());
                    aggGroup.setMeasures(Sets.intersection(aggMeasures, measures).toArray(new Integer[0]));
                }
            }

            copyForWrite.setRuleBasedIndex(newRule);
        });
    }

    private void removeUselessDimensions(IndexPlan indexPlan, Set<Integer> availableDimensions, boolean onlyDataflow,
            KylinConfig config) {
        val dataflowManager = NDataflowManager.getInstance(config, indexPlan.getProject());
        val deprecatedLayoutIds = indexPlan.getIndexes().stream().filter(index -> !index.isTableIndex())
                .filter(index -> !availableDimensions.containsAll(index.getDimensions()))
                .flatMap(index -> index.getLayouts().stream().map(LayoutEntity::getId)).collect(Collectors.toSet());
        val toBeDeletedLayoutIds = indexPlan.getToBeDeletedIndexes().stream().filter(index -> !index.isTableIndex())
                .filter(index -> !availableDimensions.containsAll(index.getDimensions()))
                .flatMap(index -> index.getLayouts().stream().map(LayoutEntity::getId)).collect(Collectors.toSet());
        deprecatedLayoutIds.addAll(toBeDeletedLayoutIds);
        if (deprecatedLayoutIds.isEmpty()) {
            return;
        }
        if (onlyDataflow) {
            val df = dataflowManager.getDataflow(indexPlan.getUuid());
            dataflowManager.removeLayouts(df, deprecatedLayoutIds);
            if (CollectionUtils.isNotEmpty(toBeDeletedLayoutIds)) {
                val indexPlanManager = NIndexPlanManager.getInstance(config, indexPlan.getProject());
                indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                    copyForWrite.removeLayoutsFromToBeDeletedList(deprecatedLayoutIds, LayoutEntity::equals, true,
                            true);
                });
            }
        } else {
            val indexPlanManager = NIndexPlanManager.getInstance(config, indexPlan.getProject());
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                copyForWrite.removeLayouts(deprecatedLayoutIds, LayoutEntity::equals, true, true);
                copyForWrite.removeLayoutsFromToBeDeletedList(deprecatedLayoutIds, LayoutEntity::equals, true, true);
            });
        }
    }

    public SegmentRange getSegmentRangeByModel(String project, String modelId, String start, String end) {
        TableRef tableRef = getDataModelManager(project).getDataModelDesc(modelId).getRootFactTable();
        TableDesc tableDesc = getTableManager(project).getTableDesc(tableRef.getTableIdentity());
        return SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
    }

    private void handleReloadData(NDataModel newModel, NDataModel oriModel, String project, String start, String end) {
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        var df = dataflowManager.getDataflow(newModel.getUuid());
        val segments = df.getFlatSegments();

        dataflowManager.updateDataflow(df.getUuid(), copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });

        String modelId = newModel.getUuid();

        if (!Objects.equals(oriModel.getPartitionDesc(), newModel.getPartitionDesc())) {

            // from having partition to no partition
            if (newModel.getPartitionDesc() == null) {
                dataflowManager.fillDfManually(df,
                        Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
                // change partition column and from no partition to having partition
            } else if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
                dataflowManager.fillDfManually(df,
                        Lists.newArrayList(getSegmentRangeByModel(project, modelId, start, end)));
            }
        } else {
            List<SegmentRange> segmentRanges = Lists.newArrayList();
            segments.forEach(segment -> segmentRanges.add(segment.getSegRange()));
            dataflowManager.fillDfManually(df, segmentRanges);
        }

        EventManager.getInstance(config, project).postAddCuboidEvents(modelId, getUsername());
    }

    public BuildIndexResponse handleIndexPlanUpdateRule(String project, String model, NRuleBasedIndex oldRule,
            NRuleBasedIndex newRule, boolean forceFireEvent) {
        log.debug("handle indexPlan udpate rule {} {}", project, model);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(model);
        val readySegs = df.getSegments();
        if (readySegs.isEmpty()) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
        }
        val eventManager = EventManager.getInstance(kylinConfig, project);

        val originLayouts = oldRule == null ? Sets.<LayoutEntity> newHashSet() : oldRule.genCuboidLayouts();
        val targetLayouts = newRule.genCuboidLayouts();

        val difference = Sets.difference(targetLayouts, originLayouts);

        // new cuboid
        if (difference.size() > 0 || forceFireEvent) {
            eventManager.postAddCuboidEvents(model, getUsername());
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD);
        }

        return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);

    }

    public IndexPlan addRuleBasedIndexBlackListLayouts(IndexPlan indexPlan, Collection<Long> blackListLayoutIds) {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), indexPlan.getProject());
        return indexPlanManager.updateIndexPlan(indexPlan.getId(), indexPlanCopy -> {
            indexPlanCopy.addRuleBasedBlackList(blackListLayoutIds);
        });
    }
}
