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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.hbase.orderedbytes;

import io.kyligence.kap.hbase.orderedbytes.util.Order;
import io.kyligence.kap.hbase.orderedbytes.util.OrderedBytes;
import io.kyligence.kap.hbase.orderedbytes.util.PositionedByteRange;

/**
 * A {@code long} of 64-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt64(PositionedByteRange, long, Order)}.
 */
public class OrderedInt64 extends OrderedBytesBase<Long> {

    public static final OrderedInt64 ASCENDING = new OrderedInt64(Order.ASCENDING);
    public static final OrderedInt64 DESCENDING = new OrderedInt64(Order.DESCENDING);

    protected OrderedInt64(Order order) {
        super(order);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public int encodedLength(Long val) {
        return 9;
    }

    @Override
    public Class<Long> encodedClass() {
        return Long.class;
    }

    @Override
    public Long decode(PositionedByteRange src) {
        return OrderedBytes.decodeInt64(src);
    }

    @Override
    public int encode(PositionedByteRange dst, Long val) {
        if (null == val)
            throw new IllegalArgumentException("Null values not supported.");
        return OrderedBytes.encodeInt64(dst, val, order);
    }

    /**
     * Read a {@code long} value from the buffer {@code src}.
     */
    public long decodeLong(PositionedByteRange src) {
        return OrderedBytes.decodeInt64(src);
    }

    /**
     * Write instance {@code val} into buffer {@code dst}.
     */
    public int encodeLong(PositionedByteRange dst, long val) {
        return OrderedBytes.encodeInt64(dst, val, order);
    }
}
