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

import static io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.DIFFERENT_CC_NAME_HAS_SAME_EXPR;
import static io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.SAME_CC_NAME_HAS_DIFFERENT_EXPR;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Objects;

import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;
import lombok.val;

public class ComputedColumnStrategy implements SchemaChangeStrategy {

    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.MODEL_CC);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels) {
        List<SchemaNode> allComputedColumns = difference.getSourceGraph().nodes().stream()
                .filter(schemaNode -> supportedSchemaNodeTypes().contains(schemaNode.getType()))
                .collect(Collectors.toList());

        String modelAlias = entry.getValue().getSubject();

        // same cc name with different expression
        if (hasComputedColumnNameWithDifferentExpression(entry.getValue(), allComputedColumns)) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), SAME_CC_NAME_HAS_DIFFERENT_EXPR, null,
                    hasSameName(modelAlias, originalModels)));
        }

        // different cc name with same expression
        val optional = hasExpressionWithDifferentComputedColumn(entry.getValue(), allComputedColumns);
        if (optional.isPresent()) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), DIFFERENT_CC_NAME_HAS_SAME_EXPR,
                    optional.get().getDetail(), hasSameName(modelAlias, originalModels)));
        }

        if (overwritable(importModels, originalModels, modelAlias)) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        } else {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        }
    }

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels) {
        List<SchemaNode> allComputedColumns = difference.getSourceGraph().nodes().stream()
                .filter(schemaNode -> supportedSchemaNodeTypes().contains(schemaNode.getType()))
                .collect(Collectors.toList());

        SchemaNode schemaNode = diff.rightValue();
        String modelAlias = diff.rightValue().getSubject();
        // same cc name with different expression
        if (hasComputedColumnNameWithDifferentExpression(schemaNode, allComputedColumns)) {
            return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                    diff.rightValue(), modelAlias, SAME_CC_NAME_HAS_DIFFERENT_EXPR, null,
                    hasSameName(modelAlias, originalModels), false, false, false));
        }

        // different cc name with same expression
        val optional = hasExpressionWithDifferentComputedColumn(schemaNode, allComputedColumns);
        if (optional.isPresent()) {
            return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                    diff.rightValue(), modelAlias, DIFFERENT_CC_NAME_HAS_SAME_EXPR, optional.get().getDetail(),
                    hasSameName(modelAlias, originalModels), false, false, false));
        }

        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, hasSameName(modelAlias, originalModels), true, true, overwritable));
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels) {
        String modelAlias = entry.getValue().getSubject();
        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        if (overwritable) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        } else {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        }
    }

    /**
     * check same cc name with different expression
     * @param node
     * @param allComputedColumns
     * @return
     */
    private boolean hasComputedColumnNameWithDifferentExpression(SchemaNode node, List<SchemaNode> allComputedColumns) {
        String ccName = node.getDetail();
        String expression = (String) node.getAttributes().get("expression");

        return allComputedColumns.stream()
                .anyMatch(schemaNode -> Objects.equal(node.getAttributes().get("fact_table"),
                        schemaNode.getAttributes().get("fact_table")) && schemaNode.getDetail().equals(ccName)
                        && !schemaNode.getAttributes().get("expression").equals(expression));
    }

    /**
     * different same cc name with same expression
     * @param node
     * @param allComputedColumns
     * @return
     */
    private Optional<SchemaNode> hasExpressionWithDifferentComputedColumn(SchemaNode node,
            List<SchemaNode> allComputedColumns) {
        String ccName = node.getDetail();
        String expression = (String) node.getAttributes().get("expression");

        return allComputedColumns.stream()
                .filter(schemaNode -> Objects.equal(node.getAttributes().get("fact_table"),
                        schemaNode.getAttributes().get("fact_table")) && !schemaNode.getDetail().equals(ccName)
                        && schemaNode.getAttributes().get("expression").equals(expression))
                .findAny();
    }

}
