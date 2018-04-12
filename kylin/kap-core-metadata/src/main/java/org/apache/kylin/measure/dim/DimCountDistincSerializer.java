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

package org.apache.kylin.measure.dim;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.util.KryoUtils;

public class DimCountDistincSerializer extends DataTypeSerializer<DimCountDistinctCounter> {
    // called by reflection
    public DimCountDistincSerializer(DataType type) {
    }
    
    @Override
    public void serialize(DimCountDistinctCounter value, ByteBuffer out) {
        byte[] serialize = KryoUtils.serialize(value.getContainer());
        out.putInt(4 + 4 + 4 + serialize.length);
        out.putInt(value.getMAX_CARD());
        out.putInt(serialize.length);
        out.put(serialize);
    }

    @Override
    public DimCountDistinctCounter deserialize(ByteBuffer in) {
        int totalLength = in.getInt();
        int maxCard = in.getInt();
        int arrayLength = in.getInt();
        byte[] data = new byte[arrayLength];
        in.get(data);
        return new DimCountDistinctCounter(KryoUtils.deserialize(data, Set.class), maxCard);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int ret = in.getInt();
        in.position(mark);
        return ret;
    }

    @Override
    public int maxLength() {
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 1024;
    }
}
