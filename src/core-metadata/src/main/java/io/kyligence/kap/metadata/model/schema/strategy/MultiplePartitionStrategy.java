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

import static io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.MULTIPLE_PARTITION_COLUMN_CHANGED;
import static io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.NONE;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;
import io.kyligence.kap.metadata.model.util.MultiPartitionUtil;

public class MultiplePartitionStrategy extends UnOverWritableStrategy {
    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.MODEL_MULTIPLE_PARTITION);
    }

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels) {
        String modelAlias = diff.rightValue().getSubject();

        boolean overwritable = overwritable(diff);

        // columns equals
        if (overwritable) {
            List<List<String>> leftPartitions = (List<List<String>>) diff.leftValue().getAttributes().get("partitions");
            List<List<String>> rightPartitions = (List<List<String>>) diff.rightValue().getAttributes().get("partitions");

            if (leftPartitions.size() == rightPartitions.size()) {
                // ignore orders
                List<String[]> duplicateValues = MultiPartitionUtil.findDuplicateValues(
                        leftPartitions.stream().map(item -> item.toArray(new String[0])).collect(Collectors.toList()),
                        rightPartitions.stream().map(item -> item.toArray(new String[0])).collect(Collectors.toList()));

                if (duplicateValues.size() == rightPartitions.size()) {
                    return Collections.emptyList();
                }
            }
        }

        SchemaChangeCheckResult.UN_IMPORT_REASON reason = NONE;
        if (!overwritable) {
            reason = MULTIPLE_PARTITION_COLUMN_CHANGED;
        }

        return Collections
                .singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(), diff.rightValue(),
                        modelAlias, reason, hasSameName(modelAlias, originalModels), true, true, overwritable));
    }

    /**
     * 
     * @param diff
     * @return
     */
    public boolean overwritable(MapDifference.ValueDifference<SchemaNode> diff) {
        return Objects.equals(diff.leftValue().getAttributes().get("columns"),
                diff.rightValue().getAttributes().get("columns"));
    }
}
