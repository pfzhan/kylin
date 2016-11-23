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
 * A {@code short} of 16-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt16(PositionedByteRange, short, Order)}.
 */
public class OrderedInt16 extends OrderedBytesBase<Short> {

    public static final OrderedInt16 ASCENDING = new OrderedInt16(Order.ASCENDING);
    public static final OrderedInt16 DESCENDING = new OrderedInt16(Order.DESCENDING);

    protected OrderedInt16(Order order) {
        super(order);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public int encodedLength(Short val) {
        return 3;
    }

    @Override
    public Class<Short> encodedClass() {
        return Short.class;
    }

    @Override
    public Short decode(PositionedByteRange src) {
        return OrderedBytes.decodeInt16(src);
    }

    @Override
    public int encode(PositionedByteRange dst, Short val) {
        if (null == val)
            throw new IllegalArgumentException("Null values not supported.");
        return OrderedBytes.encodeInt16(dst, val, order);
    }

    /**
     * Read a {@code short} value from the buffer {@code src}.
     */
    public short decodeShort(PositionedByteRange src) {
        return OrderedBytes.decodeInt16(src);
    }

    /**
     * Write instance {@code val} into buffer {@code dst}.
     */
    public int encodeShort(PositionedByteRange dst, short val) {
        return OrderedBytes.encodeInt16(dst, val, order);
    }
}
