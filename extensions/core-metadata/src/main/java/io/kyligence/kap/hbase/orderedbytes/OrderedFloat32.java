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
 * A {@code float} of 32-bits using a fixed-length encoding. Based on
 * {@link OrderedBytes#encodeFloat32(PositionedByteRange, float, Order)}.
 */
public class OrderedFloat32 extends OrderedBytesBase<Float> {

    public static final OrderedFloat32 ASCENDING = new OrderedFloat32(Order.ASCENDING);
    public static final OrderedFloat32 DESCENDING = new OrderedFloat32(Order.DESCENDING);

    protected OrderedFloat32(Order order) {
        super(order);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public int encodedLength(Float val) {
        return 5;
    }

    @Override
    public Class<Float> encodedClass() {
        return Float.class;
    }

    @Override
    public Float decode(PositionedByteRange src) {
        return OrderedBytes.decodeFloat32(src);
    }

    @Override
    public int encode(PositionedByteRange dst, Float val) {
        if (null == val)
            throw new IllegalArgumentException("Null values not supported.");
        return OrderedBytes.encodeFloat32(dst, val, order);
    }

    /**
     * Read a {@code float} value from the buffer {@code dst}.
     */
    public float decodeFloat(PositionedByteRange dst) {
        return OrderedBytes.decodeFloat32(dst);
    }

    /**
     * Write instance {@code val} into buffer {@code buff}.
     */
    public int encodeFloat(PositionedByteRange dst, float val) {
        return OrderedBytes.encodeFloat32(dst, val, order);
    }
}
