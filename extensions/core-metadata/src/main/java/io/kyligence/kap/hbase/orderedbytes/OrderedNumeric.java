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

import java.math.BigDecimal;
import java.math.BigInteger;

import io.kyligence.kap.hbase.orderedbytes.util.Order;
import io.kyligence.kap.hbase.orderedbytes.util.OrderedBytes;
import io.kyligence.kap.hbase.orderedbytes.util.PositionedByteRange;
import io.kyligence.kap.hbase.orderedbytes.util.SimplePositionedByteRange;

/**
 * An {@link Number} of arbitrary precision and variable-length encoding. The
 * resulting length of encoded values is determined by the numerical (base
 * 100) precision, not absolute value. Use this data type anywhere you would
 * expect to use a {@code DECIMAL} type, a {@link BigDecimal}, a
 * {@link BigInteger}, or any time you've parsed floating precision values
 * from text. Built on {@link OrderedBytes#encodeNumeric(PositionedByteRange, BigDecimal, Order)}.
 */
public class OrderedNumeric extends OrderedBytesBase<Number> {

    public static final OrderedNumeric ASCENDING = new OrderedNumeric(Order.ASCENDING);
    public static final OrderedNumeric DESCENDING = new OrderedNumeric(Order.DESCENDING);

    protected OrderedNumeric(Order order) {
        super(order);
    }

    @Override
    public int encodedLength(Number val) {
        // TODO: this could be done better.
        PositionedByteRange buff = new SimplePositionedByteRange(100);
        return encode(buff, val);
    }

    @Override
    public Class<Number> encodedClass() {
        return Number.class;
    }

    @Override
    public Number decode(PositionedByteRange src) {
        if (OrderedBytes.isNumericInfinite(src) || OrderedBytes.isNumericNaN(src)) {
            return OrderedBytes.decodeNumericAsDouble(src);
        }
        return OrderedBytes.decodeNumericAsBigDecimal(src);
    }

    @Override
    public int encode(PositionedByteRange dst, Number val) {
        if (null == val) {
            return OrderedBytes.encodeNull(dst, order);
        } else if (val instanceof BigDecimal) {
            return OrderedBytes.encodeNumeric(dst, (BigDecimal) val, order);
        } else if (val instanceof BigInteger) {
            return OrderedBytes.encodeNumeric(dst, new BigDecimal((BigInteger) val), order);
        } else if (val instanceof Double || val instanceof Float) {
            return OrderedBytes.encodeNumeric(dst, val.doubleValue(), order);
        } else {
            // TODO: other instances of Numeric to consider?
            return OrderedBytes.encodeNumeric(dst, val.longValue(), order);
        }
    }

    /**
     * Read a {@code long} value from the buffer {@code src}.
     */
    public long decodeLong(PositionedByteRange src) {
        return OrderedBytes.decodeNumericAsLong(src);
    }

    /**
     * Write instance {@code val} into buffer {@code dst}.
     */
    public int encodeLong(PositionedByteRange dst, long val) {
        return OrderedBytes.encodeNumeric(dst, val, order);
    }

    /**
     * Read a {@code double} value from the buffer {@code src}.
     */
    public double decodeDouble(PositionedByteRange src) {
        return OrderedBytes.decodeNumericAsLong(src);
    }

    /**
     * Write instance {@code val} into buffer {@code dst}.
     */
    public int encodeDouble(PositionedByteRange dst, double val) {
        return OrderedBytes.encodeNumeric(dst, val, order);
    }
}
