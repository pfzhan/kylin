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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public interface IGTCodeSystem {

    void init(GTInfo info);

    IGTComparator getComparator();

    /** Return the length of code starting at the specified buffer, buffer position must not change after return */
    int codeLength(int col, ByteBuffer buf);

    /** Return the max possible length of a column */
    int maxCodeLength(int col);

    /** Return a DimensionEncoding if the underlying column is backed by a cube dimension, return null otherwise */
    DimensionEncoding getDimEnc(int col);

    /**
     * Encode a value into code.
     * 
     * @throws IllegalArgumentException if the value is not in dictionary
     */
    void encodeColumnValue(int col, Object value, ByteBuffer buf) throws IllegalArgumentException;

    /**
     * Encode a value into code, with option to floor rounding -1, no rounding 0, or ceiling rounding 1
     * 
     * @throws IllegalArgumentException
     * - if rounding=0 and the value is not in dictionary
     * - if rounding=-1 and there's no equal or smaller value in dictionary
     * - if rounding=1 and there's no equal or bigger value in dictionary
     */
    void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) throws IllegalArgumentException;

    /** Decode a code into value */
    Object decodeColumnValue(int col, ByteBuffer buf);

    /** Return aggregators for metrics */
    MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions);

    /** Return specific DataTypeSerializer */
    DataTypeSerializer<?> getSerializer(int col);
}
