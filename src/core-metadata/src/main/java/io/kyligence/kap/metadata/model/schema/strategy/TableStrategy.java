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

import static io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.MISSING_TABLE;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;

public class TableStrategy implements SchemaChangeStrategy {
    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.MODEL_TABLE);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> missingItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels) {
        return difference.getNodeDiff().entriesOnlyOnRight().entrySet().stream()
                .filter(pair -> supportedSchemaNodeTypes().contains(pair.getKey().getType()))
                .map(pair -> missingItemFunction(difference, pair, importModels, originalModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> missingItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels) {
        return reachableModel(difference.getTargetGraph(), entry.getValue()).stream()
                .map(modelAlias -> SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                        entry.getKey().getType(), entry.getValue(), modelAlias, MISSING_TABLE,
                        entry.getValue().getDetail(), hasSameName(modelAlias, originalModels)))
                .collect(Collectors.toList());
    }
}
