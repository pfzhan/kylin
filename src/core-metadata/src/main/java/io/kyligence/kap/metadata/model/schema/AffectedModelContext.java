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

import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.JsonUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

@Data
public class AffectedModelContext {

    @NonNull
    private final Set<SchemaNode> updatedNodes;
    private final String modelId;

    private final boolean isBroken;
    @Getter
    private Set<Long> updatedLayouts = Sets.newHashSet();
    @Getter
    private Set<Long> addLayouts = Sets.newHashSet();

    private Set<Integer> dimensions = Sets.newHashSet();
    private Set<Integer> measures = Sets.newHashSet();
    private Set<Integer> columns = Sets.newHashSet();
    private Set<String> computedColumns = Sets.newHashSet();

    public AffectedModelContext(String modelId, Set<SchemaNode> updatedNodes) {
        this.modelId = modelId;
        this.updatedNodes = updatedNodes;
        isBroken = updatedNodes.stream().anyMatch(SchemaNode::isCauseModelBroken);

        updatedLayouts = filterIndexFromNodes(updatedNodes);
        columns = updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_COLUMN)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        dimensions = updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_DIMENSION)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        measures = updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        computedColumns = updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_CC)
                .map(SchemaNode::getDetail).collect(Collectors.toSet());
    }

    public AffectedModelContext(IndexPlan originIndexPlan, Set<SchemaNode> updatedNodes) {
        this(originIndexPlan.getId(), updatedNodes);
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
        UnaryOperator<Integer[]> meaFilter = input -> Stream.of(input).filter(i -> !measures.contains(i))
                .toArray(Integer[]::new);
        indexPlan.removeLayouts(updatedLayouts, true, true);

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
        rule.setMeasures(rule.getMeasures().stream().filter(m -> !measures.contains(m)).collect(Collectors.toList()));
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

    private Set<Long> filterIndexFromNodes(Set<SchemaNode> nodes) {
        return nodes.stream()
                .filter(node -> node.getType() == SchemaNodeType.WHITE_LIST_INDEX
                        || node.getType() == SchemaNodeType.RULE_BASED_INDEX
                        || node.getType() == SchemaNodeType.TO_BE_DELETED_INDEX)
                .map(node -> Long.parseLong(node.getDetail())).collect(Collectors.toSet());
    }

}
