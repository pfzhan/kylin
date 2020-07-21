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

package io.kyligence.kap.query.engine;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import io.kyligence.kap.metadata.query.StructField;

/**
 * extract table and column metadata from context
 */
public class RelColumnMetaDataExtractor {

    public static List<StructField> getColumnMetadata(RelDataType rowType) {
        return rowType.getFieldList().stream()
                .map(type -> relDataTypeToStructField(type.getKey(), type.getValue())).collect(Collectors.toList());
    }

    public static List<StructField> getColumnMetadata(RelNode rel) {
        return getColumnMetadata(rel.getRowType());
    }

    private static StructField relDataTypeToStructField(String fieldName, RelDataType relDataType) {
        return new StructField(
                fieldName,
                relDataType.getSqlTypeName().getJdbcOrdinal(),
                relDataType.getSqlTypeName().getName(),
                getPrecision(relDataType),
                getScale(relDataType),
                relDataType.isNullable());
    }

    private static int getScale(RelDataType type) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }

    private static int getPrecision(RelDataType type) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }

}
