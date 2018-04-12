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

package org.apache.kylin.dimension;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.junit.Assert;
import org.junit.Test;

public class FixedLenHexDimEncTest {

    @Test
    public void testConstructor() {
        try {
            new FixedLenHexDimEnc(0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expect
        }

        new FixedLenHexDimEnc(8);
    }

    @Test
    public void testNull() {
        for (int i = 1; i < 9; i++) {
            FixedLenHexDimEnc enc = new FixedLenHexDimEnc(i);

            byte[] buf = new byte[enc.getLengthOfEncoding()];
            enc.encode(null, buf, 0);
            Assert.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            String decode = enc.decode(buf, 0, buf.length);
            Assert.assertEquals(null, decode);

            buf = new byte[enc.getLengthOfEncoding()];
            DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
            ser.serialize(null, ByteBuffer.wrap(buf));
            Assert.assertTrue(DimensionEncoding.isNull(buf, 0, buf.length));
            decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
            Assert.assertEquals(null, decode);
        }
    }

    @Test
    public void testEncodeDecode() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(4);
        testEncodeDecode(enc, "AF12");
        testEncodeDecode(enc, "0000");
        testEncodeDecode(enc, "FFF0");
        try {
            testEncodeDecode(enc, "abcd");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<[abcd]> but was:<[ABCD]>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFF> but was:<null>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFF");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFF[]> but was:<FFF[0]>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF0");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFF0> but was:<null>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFF10");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFF1[0]> but was:<FFF1[]>", e.getMessage());
        }
    }


    @Test
    public void testEncodeDecode2() {
        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(5);
        testEncodeDecode(enc, "AF121");
        testEncodeDecode(enc, "00000");
        
        //with a little extra room all F is supported
        testEncodeDecode(enc, "FFFFF");
        
        try {
            testEncodeDecode(enc, "FFF");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFF[]> but was:<FFF[00]>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFFF0");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFFF[0]> but was:<FFFFF[]>", e.getMessage());
        }
        try {
            testEncodeDecode(enc, "FFFF10");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFF1[0]> but was:<FFFF1[]>", e.getMessage());
        }

    }

    private void testEncodeDecode(FixedLenHexDimEnc enc, String value) {
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = value;
        enc.encode(valueStr, buf, 0);
        String decode = enc.decode(buf, 0, buf.length);
        Assert.assertEquals(valueStr, decode);
    }

    @Test
    public void testSerDes() {

        FixedLenHexDimEnc enc = new FixedLenHexDimEnc(4);
        testSerDes(enc, "AF12");
        testSerDes(enc, "0000");
        testSerDes(enc, "FFF0");
        try {
            testSerDes(enc, "FFFF");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFF> but was:<null>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFF");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFF[]> but was:<FFF[0]>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFFF0");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFFF0> but was:<null>", e.getMessage());
        }
        try {
            testSerDes(enc, "FFF10");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("expected:<FFF1[0]> but was:<FFF1[]>", e.getMessage());
        }
    }

    private void testSerDes(FixedLenHexDimEnc enc, String value) {
        DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        String valueStr = value;
        ser.serialize(valueStr, ByteBuffer.wrap(buf));
        String decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
        Assert.assertEquals(valueStr, decode);
    }

}
