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

package org.apache.kylin.measure.bitmap.intersect;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.BitmapSerializer;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

@Deprecated
public class IntersectSerializer extends DataTypeSerializer<IntersectBitmapCounter> {

    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    private static final BitmapCounter DELEGATE = factory.newBitmap();
    private static final int IS_RESULT_FLAG = 1;
    private static final int RESULT_SIZE = 12;
    private DataTypeSerializer<BitmapCounter> defaultSerializer;

    // called by reflection
    public IntersectSerializer(DataType type) {
        defaultSerializer = new BitmapSerializer(type);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return defaultSerializer.peekLength(in);
    }

    @Override
    public int maxLength() {
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return defaultSerializer.getStorageBytesEstimate();
    }

    @Override
    public void serialize(IntersectBitmapCounter value, ByteBuffer out) {
        Map<Object, BitmapCounter> map = value.getMap();
        out.putInt(map.size());
        for (Map.Entry<Object, BitmapCounter> entry : map.entrySet()) {
            byte[] src = Bytes.toBytes(entry.getKey().toString());
            out.putInt(src.length);
            out.put(src);
            defaultSerializer.serialize(entry.getValue(), out);
        }
    }

    @Override
    public IntersectBitmapCounter deserialize(ByteBuffer in) {
        IntersectBitmapCounter intersectBitmapCounter = new IntersectBitmapCounter();
        Map<Object, BitmapCounter> map = intersectBitmapCounter.getMap();
        int anInt = in.getInt();
        for (int i = 0; i < anInt; i++) {
            int keyLength = in.getInt();
            byte[] bytes = new byte[keyLength];
            in.get(bytes, 0, keyLength);
            String s = new String(bytes);
            BitmapCounter deserialize = defaultSerializer.deserialize(in);
            map.put(s, deserialize);
        }

        return intersectBitmapCounter;
    }

    @Override
    public boolean supportDirectReturnResult() {
        return true;
    }

    @Override
    public ByteBuffer getFinalResult(ByteBuffer in) {
        ByteBuffer out = ByteBuffer.allocate(RESULT_SIZE);
        try {
            BitmapCounter counter = factory.newBitmap(in);
            out.putInt(IS_RESULT_FLAG);
            out.putLong(counter.getCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        out.flip();
        return out;
    }
}
