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

package org.apache.kylin.common.persistence;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;

import lombok.val;

/**
 * overall, RawResource is immutable
 */
public class RawResource {

    private String resPath;
    @JsonSerialize(using = ByteSourceSerializer.class)
    @JsonDeserialize(using = BytesourceDeserializer.class)
    private ByteSource byteSource;
    private long timestamp;
    private long mvcc;

    public RawResource(String resPath, ByteSource byteSource, long timestamp, long mvcc) {
        this.resPath = resPath;
        this.byteSource = byteSource;
        this.timestamp = timestamp;
        this.mvcc = mvcc;
    }

    public String getResPath() {
        return this.resPath;
    }

    public ByteSource getByteSource() {
        return this.byteSource;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public long getMvcc() {
        return this.mvcc;
    }

    private static class ByteSourceSerializer extends JsonSerializer<ByteSource> {
        @Override
        public void serialize(ByteSource value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            val bytes = value.read();
            gen.writeBinary(bytes);
        }
    }

    private static class BytesourceDeserializer extends JsonDeserializer<ByteSource> {
        @Override
        public ByteSource deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            val bytes = p.getBinaryValue();
            return ByteStreams.asByteSource(bytes);
        }
    }
}
