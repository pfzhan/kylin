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

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.experimental.Delegate;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class SchemaNode {

    @Delegate
    SchemaNodeType type;

    String key;

    static SchemaNode ofTableColumn(ColumnDesc columnDesc) {
        return new SchemaNode(SchemaNodeType.TABLE_COLUMN, columnDesc.getIdentity());
    }

    static SchemaNode ofModelColumn(NDataModel.NamedColumn namedColumn, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_COLUMN, modelId + "/" + namedColumn.getId());
    }

    static SchemaNode ofModelCC(ComputedColumnDesc cc, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_CC, modelId + "/" + cc.getColumnName());
    }

    static SchemaNode ofDimension(NDataModel.NamedColumn namedColumn, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_DIMENSION, modelId + "/" + namedColumn.getId());
    }

    static SchemaNode ofMeasure(NDataModel.Measure measure, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_MEASURE, modelId + "/" + measure.getId());
    }

    static SchemaNode ofPartition(PartitionDesc partitionDesc, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_PARTITION,
                modelId + "/" + partitionDesc.getPartitionDateColumn());
    }

    static SchemaNode ofJoin(NDataModel.NamedColumn namedColumn, String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_JOIN, modelId + "/" + namedColumn.getId());
    }

    static SchemaNode ofFilter(String modelId) {
        return new SchemaNode(SchemaNodeType.MODEL_FILTER, modelId);
    }

    static SchemaNode ofIndex(SchemaNodeType type, LayoutEntity layout, String modelId) {
        return new SchemaNode(type, modelId + "/" + layout.getId());
    }

    public String getSubject() {
        if (type == SchemaNodeType.TABLE_COLUMN) {
            return key.split("\\.")[0];
        } else {
            return key.split("/")[0];
        }
    }

    public String getDetail() {
        if (type == SchemaNodeType.TABLE_COLUMN) {
            return key.split("\\.")[1];
        } else {
            val words = key.split("/");
            if (words.length == 2) {
                return words[1];
            }
            return type.toString();
        }
    }

}
