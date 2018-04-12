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

public class BooleanDimEncTest {

    @Test
    public void testNull() {
        BooleanDimEnc enc = new BooleanDimEnc();

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

    @Test
    public void testEncodeDecode() {
        BooleanDimEnc enc = new BooleanDimEnc();

        for (String x : BooleanDimEnc.ALLOWED_VALUES) {
            testEncodeDecode(enc, x);
        }

        try {
            testEncodeDecode(enc, "FAlse");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("Value 'FAlse' is not a recognized boolean value.", e.getMessage());
        }
    }

    private void testEncodeDecode(BooleanDimEnc enc, String valueStr) {
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        enc.encode(valueStr, buf, 0);
        String decode = enc.decode(buf, 0, buf.length);
        Assert.assertEquals(valueStr, decode);
    }

    @Test
    public void testSerDes() {
        BooleanDimEnc enc = new BooleanDimEnc();
        for (String x : BooleanDimEnc.ALLOWED_VALUES) {
            testSerDes(enc, x);
        }

        try {
            testSerDes(enc, "FAlse");
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertEquals("Value 'FAlse' is not a recognized boolean value.", e.getMessage());
        }
    }

    private void testSerDes(BooleanDimEnc enc, String valueStr) {
        DataTypeSerializer<Object> ser = enc.asDataTypeSerializer();
        byte[] buf = new byte[enc.getLengthOfEncoding()];
        ser.serialize(valueStr, ByteBuffer.wrap(buf));
        String decode = (String) ser.deserialize(ByteBuffer.wrap(buf));
        Assert.assertEquals(valueStr, decode);
    }

}
