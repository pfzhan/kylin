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

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryLogicalFilter implements BinaryFilter {
    public static final Logger logger = LoggerFactory.getLogger(BinaryLogicalFilter.class);

    private final TupleFilter.FilterOperatorEnum type;
    private BinaryFilter[] children;

    public BinaryLogicalFilter(TupleFilter.FilterOperatorEnum type, BinaryFilter... children) {
        this.type = type;
        this.children = children;
    }

    public void setChildren(BinaryFilter[] children) {
        this.children = children;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        BytesUtil.writeUTFString(type.name(), buffer);
        BytesUtil.writeVInt(children.length, buffer);
        for (BinaryFilter child : children) {
            child.serialize(buffer);
        }
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        int childrenCnt = BytesUtil.readVInt(buffer);
        BinaryFilter[] children = new BinaryFilter[childrenCnt];
        for (int i = 0; i < childrenCnt; i++) {
            children[i] = BinaryFilterSerializer.deserialize(buffer);
        }
        this.setChildren(children);
    }

    @Override
    public boolean isMatch(ByteArray value) {
        if (children.length == 0) {
            return true;
        }

        switch (type) {
        case AND:
            for (BinaryFilter child : children) {
                if (!child.isMatch(value)) {
                    return false;
                }
            }
            return true;

        case OR:
            for (BinaryFilter child : children) {
                if (child.isMatch(value)) {
                    return true;
                }
            }
            return false;

        default:
            logger.warn("This type is not supported {}", type);
            return false;
        }
    }

    @Override
    public TupleFilter.FilterOperatorEnum getOperator() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("type : " + type + ",");
        builder.append("children : ");
        builder.append("[");
        for (BinaryFilter c : children) {
            builder.append(c.toString());
            builder.append(",");
        }
        builder.append("]");
        builder.append("}");
        return builder.toString();
    }
}
