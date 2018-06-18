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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class BytesUtilTest {
    @Test
    public void test() {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        int[] x = new int[] { 1, 2, 3 };
        BytesUtil.writeIntArray(x, buffer);
        buffer.flip();

        byte[] buf = new byte[buffer.limit()];
        System.arraycopy(buffer.array(), 0, buf, 0, buffer.limit());

        ByteBuffer newBuffer = ByteBuffer.wrap(buf);
        int[] y = BytesUtil.readIntArray(newBuffer);
        Assert.assertEquals(y[2], 3);
    }

    @Test
    public void testBooleanArray() {
        ByteBuffer buffer = ByteBuffer.allocate(10000);
        boolean[] x = new boolean[] { true, false, true };
        BytesUtil.writeBooleanArray(x, buffer);
        buffer.flip();
        boolean[] y = BytesUtil.readBooleanArray(buffer);
        Assert.assertEquals(y[2], true);
        Assert.assertEquals(y[1], false);
    }

    @Test
    public void testWriteReadUnsignedInt() {
        testWriteReadUnsignedInt(735033, 3);
        testWriteReadUnsignedInt(73503300, 4);
    }

    public void testWriteReadUnsignedInt(int testInt, int length) {
        ByteArray ba = new ByteArray(new byte[length]);
        BytesUtil.writeUnsigned(testInt, length, ba.asBuffer());

        byte[] newBytes = new byte[length];
        System.arraycopy(ba.array(), 0, newBytes, 0, length);
        int value = BytesUtil.readUnsigned(new ByteArray(newBytes).asBuffer(), length);

        Assert.assertEquals(value, testInt);

        byte[] anOtherNewBytes = new byte[length];
        BytesUtil.writeUnsigned(testInt, anOtherNewBytes, 0, length);

        Assert.assertTrue(Arrays.equals(anOtherNewBytes, ba.array()));
    }

    @Test
    public void testReadable() {
        String x = "\\x00\\x00\\x00\\x00\\x00\\x01\\xFC\\xA8";
        byte[] bytes = BytesUtil.fromReadableText(x);
        String y = BytesUtil.toHex(bytes);
        Assert.assertEquals(x, y);
    }

}
