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

package io.kyligence.kap.metadata.model.schema.strategy;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.Graphs;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;

public interface SchemaChangeStrategy extends IKeep {
    List<SchemaNodeType> supportedSchemaNodeTypes();

    default List<SchemaChangeCheckResult.ChangedItem> missingItemFunction(SchemaUtil.SchemaDifference difference,
                                                                          Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                          Set<String> originalModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> missingItems(SchemaUtil.SchemaDifference difference,
                                                                   Set<String> importModels, Set<String> originalModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
                                                                      Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                      Set<String> originalModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> newItems(SchemaUtil.SchemaDifference difference,
                                                               Set<String> importModels, Set<String> originalModels) {
        return difference.getNodeDiff().entriesOnlyOnRight().entrySet().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.getKey().getType()))
                .map(entry -> newItemFunction(difference, entry, importModels, originalModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
                                                                         MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels) {
        String modelAlias = diff.rightValue().getSubject();
        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, hasSameName(modelAlias, originalModels), true, true, overwritable));
    }

    default List<SchemaChangeCheckResult.UpdatedItem> updateItems(SchemaUtil.SchemaDifference difference,
                                                                  Set<String> importModels, Set<String> originalModels) {
        return difference.getNodeDiff().entriesDiffering().values().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.leftValue().getType()))
                .map(diff -> updateItemFunction(difference, diff, importModels, originalModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
                                                                         Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                         Set<String> originalModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> reduceItems(SchemaUtil.SchemaDifference difference,
                                                                  Set<String> importModels, Set<String> originalModels) {
        return difference.getNodeDiff().entriesOnlyOnLeft().entrySet().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.getKey().getType()))
                .map(entry -> reduceItemFunction(difference, entry, importModels, originalModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<String> areEqual(SchemaUtil.SchemaDifference difference, Set<String> importModels) {
        return difference.getNodeDiff().entriesInCommon().values().stream()
                .filter(schemaNode -> schemaNode.getType() == SchemaNodeType.MODEL_FACT)
                .filter(schemaChange -> importModels.contains(schemaChange.getSubject())).map(SchemaNode::getSubject)
                .collect(Collectors.toList());
    }

    default boolean overwritable(Set<String> importModels, Set<String> originalModels, String modelAlias) {
        return importModels.contains(modelAlias) && originalModels.contains(modelAlias);
    }

    default boolean hasSameName(String modelAlias, Set<String> originalModels) {
        return originalModels.contains(modelAlias);
    }

    default Set<String> reachableModel(Graph<SchemaNode> graph, SchemaNode schemaNode) {
        return Graphs.reachableNodes(graph, schemaNode).stream().filter(SchemaNode::isModelNode)
                .map(SchemaNode::getSubject).collect(Collectors.toSet());
    }

}
