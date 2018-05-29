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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryFilterSerializer {
    private static final Logger logger = LoggerFactory.getLogger(BinaryFilterSerializer .class);
    private static final int BUFFER_SIZE = 65536;

    public static byte[] serialize(BinaryFilter filter) {
        ByteBuffer buffer;
        int bufferSize = BUFFER_SIZE;
        while (true) {
            try {
                buffer = ByteBuffer.allocate(bufferSize);
                innerSerialize(buffer, filter);
                break;
            } catch (BufferOverflowException e) {
                logger.info("Buffer size {} cannot hold the filter, resizing to 4 times", bufferSize);
                bufferSize *= 4;
            }
        }
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    public static void innerSerialize(ByteBuffer buffer, BinaryFilter filter) {
        filter.serialize(buffer);
    }

    public static BinaryFilter deserialize(ByteBuffer buffer) {
        TupleFilter.FilterOperatorEnum op = TupleFilter.FilterOperatorEnum.valueOf(BytesUtil.readUTFString(buffer));

        BinaryFilter filter;
        switch (op) {
            case AND:
            case OR:
                filter = new BinaryLogicalFilter(op, null);
                break;
            case CONSTANT:
                filter = new BinaryConstantFilter();
                break;
            default:
                filter = new BinaryCompareFilter(op);
                break;
        }

        filter.deserialize(buffer);
        return filter;
    }
}
