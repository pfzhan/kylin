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

package io.kyligence.kap.storage.parquet.format.filter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BinaryCompareFilter implements BinaryFilter {
    public static final Logger logger = LoggerFactory.getLogger(BinaryCompareFilter.class);

    private List<byte[]> operandVal;
    private int operandOff;
    private int operandLen;
    private FilterOperatorEnum type;
    private Set<ByteArray> candidate = null;

    public BinaryCompareFilter(FilterOperatorEnum type) {
        this.type = type;
    }

    public BinaryCompareFilter(FilterOperatorEnum type, List<byte[]> operandVal, int operandOff, int operandLen) {
        this.type = type;
        this.operandVal = operandVal;
        if (operandVal == null && type != FilterOperatorEnum.ISNULL && type != FilterOperatorEnum.ISNOTNULL) {
            throw new IllegalArgumentException("operandVal should not be null when type is " + type);
        }
        this.operandOff = operandOff;
        this.operandLen = operandLen;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        BytesUtil.writeUTFString(type.name(), buffer);

        if (operandVal == null) {
            BytesUtil.writeVInt(-1, buffer);
        } else {
            BytesUtil.writeVInt(operandVal.size(), buffer);
            for (byte[] val : operandVal) {
                BytesUtil.writeByteArray(val, buffer);
            }
        }

        BytesUtil.writeVInt(operandOff, buffer);
        BytesUtil.writeVInt(operandLen, buffer);
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        // "type" should be deserialized first to create an instance
        int operandValSize = BytesUtil.readVInt(buffer);
        if (operandValSize != -1) {
            operandVal = Lists.newArrayList();
            for (int i = 0; i < operandValSize; i++) {
                operandVal.add(BytesUtil.readByteArray(buffer));
            }
        }

        operandOff = BytesUtil.readVInt(buffer);
        operandLen = BytesUtil.readVInt(buffer);
    }

    @Override
    public boolean isMatch(ByteArray value) {
        if (bytesIsNull(value, operandOff, operandLen)) {
            if (type == FilterOperatorEnum.ISNULL) {
                return true;
            } else {
                return false;
            }
        } else {
            if (type == FilterOperatorEnum.ISNOTNULL)
                return true;
            else if (type == FilterOperatorEnum.ISNULL)
                return false;
        }

        switch (type) {
        case EQ:
            if (operandVal.size() > 0) {
                return bytesEqual(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case NEQ:
            if (operandVal.size() > 0) {
                return !bytesEqual(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case LT:
            if (operandVal.size() > 0) {
                return bytesLessThan(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case GT:
            if (operandVal.size() > 0) {
                return bytesGreaterThan(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case LTE:
            if (operandVal.size() > 0) {
                return bytesLessThanEqual(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case GTE:
            if (operandVal.size() > 0) {
                return bytesGreaterThanEqual(value, operandOff, operandLen, operandVal.get(0), 0, operandLen);
            }
            break;
        case IN:
            return bytesIn(value, operandOff, operandLen, operandVal, 0, operandLen);
        case NOTIN:
            return !bytesIn(value, operandOff, operandLen, operandVal, 0, operandLen);
        default:
            throw new IllegalArgumentException("Type " + type + " is not supported");
        }

        throw new IllegalArgumentException("operandVal should contain at least one value, its size is 0 now");
    }

    @Override
    public FilterOperatorEnum getOperator() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("type : " + type + ",");
        builder.append("operandOff : " + operandOff + ",");
        builder.append("operandLen : " + operandLen + ",");
        builder.append("operandVal : [");
        if (operandVal != null) {
            for (byte[] val : operandVal) {
                for (byte v : val) {
                    builder.append(String.valueOf(v) + "_");
                }
                builder.append(",");
            }
        }
        builder.append("]");
        builder.append("}");
        return builder.toString();
    }

    // Duplicate code for performance
    private boolean bytesEqual(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset,
            int rightLength) {
        if (leftLength != rightLength) {
            return false;
        }
        for (int i = 0; i < leftLength; i++) {
            if (left.get(leftOffset + i) != right[rightOffset + i]) {
                return false;
            }
        }

        // equal
        return true;
    }

    private boolean bytesLessThan(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return bytesLessConsiderEqual(left, leftOffset, leftLength, right, rightOffset, rightLength, false);
    }

    private boolean bytesLessThanEqual(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return bytesLessConsiderEqual(left, leftOffset, leftLength, right, rightOffset, rightLength, true);
    }

    private boolean bytesLessConsiderEqual(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength, boolean equalRetVal) {
        if (leftLength != rightLength) {
            return false;
        }

        for (int i = 0; i < leftLength; i++) {
            if (left.get(leftOffset + i) != right[rightOffset + i]) {
                if (toUnsignedInt(left.get(leftOffset + i)) < toUnsignedInt(right[rightOffset + i])) {
                    return true;
                } else {
                    return false;
                }
            }
        }

        return equalRetVal;
    }

    private boolean bytesGreaterThan(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return byteGreaterConsiderEqual(left, leftOffset, leftLength, right, rightOffset, rightLength, false);
    }

    private boolean bytesGreaterThanEqual(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return byteGreaterConsiderEqual(left, leftOffset, leftLength, right, rightOffset, rightLength, true);
    }

    private boolean byteGreaterConsiderEqual(ByteArray left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength, boolean equalRetVal) {
        if (leftLength != rightLength) {
            return false;
        }

        for (int i = 0; i < leftLength; i++) {
            if (left.get(leftOffset + i) != right[rightOffset + i]) {
                if (toUnsignedInt(left.get(leftOffset + i)) > toUnsignedInt(right[rightOffset + i])) {
                    return true;
                } else {
                    return false;
                }
            }
        }

        return equalRetVal;
    }

    private boolean bytesIn(ByteArray left, int leftOffset, int leftLength, List<byte[]> right, int rightOffset, int rightLength) {
        if (candidate == null) {
            candidate = Sets.newHashSet();
            for (byte[] r : right) {
                candidate.add(new ByteArray(r, rightOffset, rightLength));
            }
        }

        return candidate.contains(new ByteArray(left.array(), left.offset() + leftOffset, leftLength));
    }

    private boolean bytesIsNull(ByteArray left, int leftOffset, int leftLength) {
        for (int i = 0; i < leftLength; i++) {
            if (left.get(leftOffset + i) != (byte) 0xff) {
                return false;
            }
        }
        return true;
    }

    private int toUnsignedInt(byte b) {
        return (((int) b) & 0xff);
    }
}
