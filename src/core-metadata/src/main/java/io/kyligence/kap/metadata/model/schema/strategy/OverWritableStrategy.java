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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;

public class OverWritableStrategy implements SchemaChangeStrategy {

    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Arrays.asList(SchemaNodeType.MODEL_DIMENSION, SchemaNodeType.MODEL_MEASURE,
                SchemaNodeType.RULE_BASED_INDEX, SchemaNodeType.WHITE_LIST_INDEX, SchemaNodeType.TO_BE_DELETED_INDEX);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
                                                                     Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                     Set<String> originalModels) {
        return createSchemaChange(difference, entry, importModels, originalModels);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
                                                                        Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                        Set<String> originalModels) {
        return createSchemaChange(difference, entry, importModels, originalModels);
    }

    private List<SchemaChangeCheckResult.ChangedItem> createSchemaChange(SchemaUtil.SchemaDifference difference,
                                                                         Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
                                                                         Set<String> originalModels) {
        String modelAlias = entry.getValue().getSubject();
        if (overwritable(importModels, originalModels, modelAlias)) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        } else {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels)));
        }
    }
}
