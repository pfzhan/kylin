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

package io.kyligence.kap.storage.parquet.format.file;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;

// TODO: Tuning slab size, abstract it to file its own class
// TODO: Enable Dictionary Page Writer
public enum Encoding {
    PLAIN {
        @Override
        public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
            switch (descriptor.getType()) {
            case BOOLEAN:
                return new BooleanPlainValuesWriter();
            case FIXED_LEN_BYTE_ARRAY:
                return new FixedLenByteArrayPlainValuesWriter(descriptor.getTypeLength(), descriptor.getTypeLength() * count, descriptor.getTypeLength() * count);
            default:
                return new PlainValuesWriter(count * 4, count * 20);
            }
        }
    },

    RLE {
        @Override
        public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
            int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
            return new RunLengthBitPackingHybridValuesWriter(bitWidth, byteBoundUp(bitWidth, count), byteBoundUp(bitWidth, count));
        }
    },

    DELTA_BINARY_PACKED {
        @Override
        public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
            if (descriptor.getType() != INT32) {
                throw new ParquetEncodingException("Encoding DELTA_BINARY_PACKED is only supported for type INT32");
            }
            return new DeltaBinaryPackingValuesWriter(count * 2, count * 2);
        }
    },

    DELTA_LENGTH_BYTE_ARRAY {
        @Override
        public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
            if (descriptor.getType() != BINARY) {
                throw new ParquetEncodingException("Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
            }
            return new DeltaLengthByteArrayValuesWriter(count * 10, count * 10);
        }
    },

    DELTA_BYTE_ARRAY {
        @Override
        public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
            if (descriptor.getType() != BINARY && descriptor.getType() != FIXED_LEN_BYTE_ARRAY) {
                throw new ParquetEncodingException("Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
            }
            return new DeltaByteArrayWriter(count * 10, count * 10);
        }
    };

    int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType) {
        int maxLevel;
        switch (valuesType) {
        case REPETITION_LEVEL:
            maxLevel = descriptor.getMaxRepetitionLevel();
            break;
        case DEFINITION_LEVEL:
            maxLevel = descriptor.getMaxDefinitionLevel();
            break;
        case VALUES:
            if (descriptor.getType() == BOOLEAN) {
                maxLevel = 1;
                break;
            }
        default:
            throw new ParquetEncodingException("Unsupported encoding for values: " + this);
        }
        return maxLevel;
    }

    int byteBoundUp(int bitWidth, int count) {
        return (bitWidth * count + 7) / 8;
    }

    public ValuesWriter getValuesWriter(ColumnDescriptor descriptor, ValuesType valuesType, int count) {
        throw new UnsupportedOperationException("Error encoding" + descriptor + ". " + this.name() + " is dictionary based");
    }
}
