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

package org.apache.kylin.cube.common;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.BytesSplitter;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
public class BytesSplitterTest {

    @Test
    public void test() {
        BytesSplitter bytesSplitter = new BytesSplitter(10, 15);
        byte[] input = "2013-02-17Collectibles".getBytes();
        bytesSplitter.split(input, input.length, (byte) 127);

        assertEquals(2, bytesSplitter.getBufferSize());
        assertEquals("2013-02-17", new String(bytesSplitter.getSplitBuffers()[0].value, 0, bytesSplitter.getSplitBuffers()[0].length));
        assertEquals("Collectibles", new String(bytesSplitter.getSplitBuffers()[1].value, 0, bytesSplitter.getSplitBuffers()[1].length));
    }

    @Test
    public void testNullValue() {
        BytesSplitter bytesSplitter = new BytesSplitter(10, 15);
        byte[] input = "2013-02-17Collectibles".getBytes();
        bytesSplitter.split(input, input.length, (byte) 127);

        assertEquals(3, bytesSplitter.getBufferSize());
        assertEquals("2013-02-17", new String(bytesSplitter.getSplitBuffers()[0].value, 0, bytesSplitter.getSplitBuffers()[0].length));
        assertEquals("", new String(bytesSplitter.getSplitBuffers()[1].value, 0, bytesSplitter.getSplitBuffers()[1].length));
        assertEquals("Collectibles", new String(bytesSplitter.getSplitBuffers()[2].value, 0, bytesSplitter.getSplitBuffers()[2].length));
    }
}
