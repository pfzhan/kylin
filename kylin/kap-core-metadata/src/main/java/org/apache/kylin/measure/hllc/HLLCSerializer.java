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

package org.apache.kylin.measure.hllc;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author yangli9
 * 
 */
public class HLLCSerializer extends DataTypeSerializer<HLLCounter> {

    private int precision;

    public HLLCSerializer(DataType type) {
        this.precision = type.getPrecision();
    }

    @Override
    public void serialize(HLLCounter value, ByteBuffer out) {
        try {
            value.writeRegisters(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HLLCounter current() {
        HLLCounter hllc = (HLLCounter) current.get();
        if (hllc == null) {
            hllc = new HLLCounter(precision);
            current.set(hllc);
        }
        return hllc;
    }

    @Override
    public HLLCounter deserialize(ByteBuffer in) {
        HLLCounter hllc = new HLLCounter(precision);
        try {
            hllc.readRegisters(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hllc;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return current().peekLength(in);
    }

    @Override
    public int maxLength() {
        return current().maxLength();
    }

    @Override
    public int getStorageBytesEstimate() {
        return current().maxLength();
    }

}
