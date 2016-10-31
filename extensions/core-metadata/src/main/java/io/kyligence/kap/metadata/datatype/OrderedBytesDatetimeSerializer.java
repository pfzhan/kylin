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

package io.kyligence.kap.metadata.datatype;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.types.OrderedInt64;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;

public class OrderedBytesDatetimeSerializer extends OrderedBytesSerializer<Long> {

    public OrderedBytesDatetimeSerializer(DataType type) {
        super(type);
        orderedBytesBase = OrderedInt64.ASCENDING;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 9;
    }

    @Override
    public int maxLength() {
        return 9;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 9;
    }

    public Long valueOf(String str) {
        if (str == null)
            return 0L;
        else
            return DateFormat.stringToMillis(str);
    }
}
