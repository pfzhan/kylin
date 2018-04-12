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

package org.apache.kylin.common.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class BytesSplitter {
    private static final Logger logger = LoggerFactory.getLogger(BytesSplitter.class);

    private static final int[] COMMON_DELIMS = new int[] { "\177".codePointAt(0), "|".codePointAt(0), "\t".codePointAt(0), ",".codePointAt(0) };

    private SplittedBytes[] splitBuffers;
    private int bufferSize;

    public SplittedBytes[] getSplitBuffers() {
        return splitBuffers;
    }

    public SplittedBytes getSplitBuffer(int index) {
        return splitBuffers[index];
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public BytesSplitter(int splitLen, int bytesLen) {
        this.splitBuffers = new SplittedBytes[splitLen];
        for (int i = 0; i < splitLen; i++) {
            this.splitBuffers[i] = new SplittedBytes(bytesLen);
        }
        this.bufferSize = 0;
    }

    public int split(byte[] bytes, int byteLen, byte delimiter) {
        this.bufferSize = 0;
        int offset = 0;
        int length = 0;
        for (int i = 0; i < byteLen; i++) {
            if (bytes[i] == delimiter) {
                SplittedBytes split = this.splitBuffers[this.bufferSize++];
                if (length > split.value.length) {
                    length = split.value.length;
                }
                System.arraycopy(bytes, offset, split.value, 0, length);
                split.length = length;
                offset = i + 1;
                length = 0;
            } else {
                length++;
            }
        }
        SplittedBytes split = this.splitBuffers[this.bufferSize++];
        if (length > split.value.length) {
            length = split.value.length;
        }
        System.arraycopy(bytes, offset, split.value, 0, length);
        split.length = length;

        return bufferSize;
    }

    public void setBuffers(byte[][] buffers) {
        for (int i = 0; i < buffers.length; i++) {
            splitBuffers[i].value = buffers[i];
            splitBuffers[i].length = buffers[i].length;
        }
        this.bufferSize = buffers.length;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[");
        for (int i = 0; i < bufferSize; i++) {
            if (i > 0)
                buf.append(", ");

            buf.append(Bytes.toString(splitBuffers[i].value, 0, splitBuffers[i].length));
        }
        return buf.toString();
    }

    public static List<String> splitToString(byte[] bytes, int offset, byte delimiter) {
        List<String> splitStrings = new ArrayList<String>();
        int splitOffset = 0;
        int splitLength = 0;
        for (int i = offset; i < bytes.length; i++) {
            if (bytes[i] == delimiter) {
                String str = Bytes.toString(bytes, splitOffset, splitLength);
                splitStrings.add(str);
                splitOffset = i + 1;
                splitLength = 0;
            } else {
                splitLength++;
            }
        }
        String str = Bytes.toString(bytes, splitOffset, splitLength);
        splitStrings.add(str);
        return splitStrings;
    }

}
