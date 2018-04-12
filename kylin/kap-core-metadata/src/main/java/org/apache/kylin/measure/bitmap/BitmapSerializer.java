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

package org.apache.kylin.measure.bitmap;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BitmapSerializer extends DataTypeSerializer<BitmapCounter> {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    private static final BitmapCounter DELEGATE = factory.newBitmap();

    private static final int IS_RESULT_FLAG = 1;
    private static final int RESULT_SIZE = 12;

    // called by reflection
    public BitmapSerializer(DataType type) {
    }

    @Override
    public void serialize(BitmapCounter value, ByteBuffer out) {
        try {
            value.write(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BitmapCounter deserialize(ByteBuffer in) {
        try {
            //The length of RoaringBitmap is larger than 12
            if (peekLength(in) == RESULT_SIZE) {
                int flag = in.getInt();
                return factory.newBitmap(in.getLong());
            } else {
                return factory.newBitmap(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int peekLength(ByteBuffer in) {
        ByteBuffer buffer = in.slice();
        if (buffer.getInt(0) == IS_RESULT_FLAG) {
            return RESULT_SIZE;
        } else {
            return DELEGATE.peekLength(in);
        }
    }

    @Override
    public int maxLength() {
        // the bitmap is non-fixed length, and we just assume 8MB here, maybe change it later
        // some statistics for bitmap: 
        // 1 million distinct keys takes about 2MB storage
        // 5 million takes 10MB
        // 10 million takes 12MB
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        // It's difficult to decide the size before data was ingested, comparing with HLLCounter(16) as 64KB, here is assumption
        return 8 * 1024;
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
