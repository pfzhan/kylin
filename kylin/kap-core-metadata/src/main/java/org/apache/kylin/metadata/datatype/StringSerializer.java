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

package org.apache.kylin.metadata.datatype;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;

public class StringSerializer extends DataTypeSerializer<String> {

    final DataType type;
    final int maxLength;

    public StringSerializer(DataType type) {
        this.type = type;
        // see serialize(): 2 byte length, rest is String.toBytes()
        this.maxLength = 2 + type.getPrecision();
    }

    @Override
    public void serialize(String value, ByteBuffer out) {
        int start = out.position();

        BytesUtil.writeUTFString(value, out);

        if (out.position() - start > maxLength)
            throw new IllegalArgumentException("'" + value + "' exceeds the expected length for type " + type);
    }

    @Override
    public String deserialize(ByteBuffer in) {
        return BytesUtil.readUTFString(in);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return BytesUtil.peekByteArrayLength(in);
    }

    @Override
    public int maxLength() {
        return maxLength;
    }

    @Override
    public int getStorageBytesEstimate() {
        return maxLength;
    }

    @Override
    public String valueOf(String str) {
        return str;
    }
}
