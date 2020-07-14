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

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.Graphs;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

@Data
public class AffectedModelContext {

    @NonNull
    private final Set<SchemaNode> updatedNodes;
    private final Set<SchemaNode> shouldDeleteNodes;
    private final String project;
    private final String modelId;

    private final boolean isBroken;
    @Getter
    private Set<Long> updatedLayouts = Sets.newHashSet();
    @Getter
    private Set<Long> shouldDeleteLayouts = Sets.newHashSet();
    @Getter
    private Set<Long> addLayouts = Sets.newHashSet();

    private Set<Integer> dimensions = Sets.newHashSet();
    private Set<Integer> measures = Sets.newHashSet();
    private Set<Integer> columns = Sets.newHashSet();
    private Set<String> computedColumns = Sets.newHashSet();

    private Map<Integer, NDataModel.Measure> updateMeasureMap = Maps.newHashMap();

    public AffectedModelContext(String project, String modelId, Set<SchemaNode> updatedNodes, boolean isDelete) {
        this(project, modelId, updatedNodes, Sets.newHashSet(), isDelete);
    }

    public AffectedModelContext(String project, String modelId, Set<SchemaNode> updatedNodes,
            Set<Pair<Integer, NDataModel.Measure>> updateMeasures, boolean isDelete) {
        this.project = project;
        this.modelId = modelId;
        this.updatedNodes = updatedNodes;
        this.updateMeasureMap = updateMeasures.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        this.shouldDeleteNodes = calcShouldDeletedNodes(isDelete);
        isBroken = updatedNodes.stream().anyMatch(SchemaNode::isCauseModelBroken);

        updatedLayouts = filterIndexFromNodes(updatedNodes);
        shouldDeleteLayouts = filterIndexFromNodes(shouldDeleteNodes);
        columns = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_COLUMN)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        dimensions = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_DIMENSION)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        measures = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        computedColumns = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_CC)
                .map(SchemaNode::getDetail).collect(Collectors.toSet());
    }

    public AffectedModelContext(String project, IndexPlan originIndexPlan, Set<SchemaNode> updatedNodes,
            Set<Pair<Integer, NDataModel.Measure>> updateMeasures, boolean isDelete) {
        this(project, originIndexPlan.getId(), updatedNodes, updateMeasures, isDelete);
        val indexPlanCopy = originIndexPlan.copy();
        shrinkIndexPlan(indexPlanCopy);
        val originLayoutIds = originIndexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        addLayouts = indexPlanCopy.getAllLayouts().stream().map(LayoutEntity::getId)
                .filter(id -> !originLayoutIds.contains(id)).collect(Collectors.toSet());
    }

    public void shrinkIndexPlan(IndexPlan indexPlan) {
        UnaryOperator<Integer[]> dimFilter = input -> Stream.of(input).filter(i -> !dimensions.contains(i))
                .toArray(Integer[]::new);
        UnaryOperator<Integer[]> meaFilter = input -> Stream.of(input).map(i -> {
            if (updateMeasureMap.containsKey(i)) {
                i = updateMeasureMap.get(i).getId();
            }
            return i;
        }).filter(i -> !measures.contains(i)).toArray(Integer[]::new);
        indexPlan.removeLayouts(shouldDeleteLayouts, true, true);

        val overrideIndexes = Maps.newHashMap(indexPlan.getIndexPlanOverrideIndexes());
        columns.forEach(overrideIndexes::remove);
        indexPlan.setIndexPlanOverrideIndexes(overrideIndexes);

        if (indexPlan.getDictionaries() != null) {
            indexPlan.setDictionaries(indexPlan.getDictionaries().stream().filter(d -> !columns.contains(d.getId()))
                    .collect(Collectors.toList()));
        }

        if (indexPlan.getRuleBasedIndex() == null) {
            return;
        }
        val rule = JsonUtil.deepCopyQuietly(indexPlan.getRuleBasedIndex(), NRuleBasedIndex.class);
        rule.setLayoutIdMapping(Lists.newArrayList());
        rule.setDimensions(
                rule.getDimensions().stream().filter(d -> !dimensions.contains(d)).collect(Collectors.toList()));
        rule.setMeasures(rule.getMeasures().stream().map(i -> {
            if (updateMeasureMap.containsKey(i)) {
                i = updateMeasureMap.get(i).getId();
            }
            return i;
        }).filter(i -> !measures.contains(i)).collect(Collectors.toList()));
        val newAggGroups = rule.getAggregationGroups().stream().peek(group -> {
            group.setIncludes(dimFilter.apply(group.getIncludes()));
            group.setMeasures(meaFilter.apply(group.getMeasures()));
            group.getSelectRule().mandatoryDims = dimFilter.apply(group.getSelectRule().mandatoryDims);
            group.getSelectRule().hierarchyDims = Stream.of(group.getSelectRule().hierarchyDims).map(dimFilter)
                    .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
            group.getSelectRule().jointDims = Stream.of(group.getSelectRule().jointDims).map(dimFilter)
                    .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
        }).filter(group -> ArrayUtils.isNotEmpty(group.getIncludes())).collect(Collectors.toList());

        rule.setAggregationGroups(newAggGroups);
        indexPlan.setRuleBasedIndex(rule);
        if (updatedNodes.contains(SchemaNodeType.INDEX_AGG_SHARD.withKey(indexPlan.getId()))) {
            indexPlan.setAggShardByColumns(Lists.newArrayList());
        }
        if (updatedNodes.contains(SchemaNodeType.INDEX_AGG_EXTEND_PARTITION.withKey(indexPlan.getId()))) {
            indexPlan.setExtendPartitionColumns(Lists.newArrayList());
        }

    }

    Predicate<SchemaNode> deletableIndexPredicate = node -> node.getType() == SchemaNodeType.WHITE_LIST_INDEX
            || node.getType() == SchemaNodeType.RULE_BASED_INDEX
            || node.getType() == SchemaNodeType.TO_BE_DELETED_INDEX;

    private Set<Long> filterIndexFromNodes(Set<SchemaNode> nodes) {
        return nodes.stream()
                .filter(deletableIndexPredicate)
                .map(node -> Long.parseLong(node.getDetail())).collect(Collectors.toSet());
    }

    private Set<SchemaNode> calcShouldDeletedNodes(boolean isDelete) {
        Set<SchemaNode> result = Sets.newHashSet();

        if (isDelete) {
            return updatedNodes;
        }

        Graph<SchemaNode> schemaNodeGraph = SchemaUtil.dependencyGraph(project);

        updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE)
                .filter(node -> !updateMeasureMap.containsKey(Integer.parseInt(node.getDetail()))).forEach(node -> {
                    // add should delete measure
                    result.add(node);
                    // add should delete measure affected index schema nodes
                    Graphs.reachableNodes(schemaNodeGraph, node)
                            .stream()
                            .filter(deletableIndexPredicate)
                            .forEach(result::add);
                });

        // add updated measure affect index schema nodes
        updateMeasureMap.keySet().forEach(originalMeasureId -> {
            Graphs.reachableNodes(schemaNodeGraph,
                    SchemaNodeType.MODEL_MEASURE.withKey(modelId + "/" + originalMeasureId))
                    .stream()
                    .filter(deletableIndexPredicate)
                    .forEach(result::add);
        });

        return result;
    }
}
