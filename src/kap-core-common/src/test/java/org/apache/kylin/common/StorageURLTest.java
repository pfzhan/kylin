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

package org.apache.kylin.common;

import org.junit.Assert;
import org.junit.Test;

public class StorageURLTest {

    @Test
    public void testBasic() {
        {
            StorageURL id = new StorageURL("hello@hbase");
            Assert.assertEquals("hello", id.getIdentifier());
            Assert.assertEquals("hbase", id.getScheme());
            Assert.assertEquals(0, id.getAllParameters().size());
            Assert.assertEquals("hello@hbase", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@hbase,a=b,c=d");
            Assert.assertEquals("hello", id.getIdentifier());
            Assert.assertEquals("hbase", id.getScheme());
            Assert.assertEquals(2, id.getAllParameters().size());
            Assert.assertEquals("b", id.getParameter("a"));
            Assert.assertEquals("d", id.getParameter("c"));
            Assert.assertEquals("hello@hbase,a=b,c=d", id.toString());
        }
        {
            StorageURL o = new StorageURL("hello@hbase,c=d");
            StorageURL o2 = new StorageURL("hello@hbase,a=b");
            StorageURL id = o.copy(o2.getAllParameters());
            Assert.assertEquals("hello", id.getIdentifier());
            Assert.assertEquals("hbase", id.getScheme());
            Assert.assertEquals(1, id.getAllParameters().size());
            Assert.assertEquals("b", id.getParameter("a"));
            Assert.assertEquals("hello@hbase,a=b", id.toString());
            Assert.assertEquals("hello@hbase,c=d", o.toString());
            Assert.assertEquals("hello@hbase,a=b", o2.toString());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullInput() {
        new StorageURL(null);
    }

    @Test
    public void testEdgeCases() {
        {
            StorageURL id = new StorageURL("");
            Assert.assertEquals("kylin_metadata", id.getIdentifier());
            Assert.assertEquals("", id.getScheme());
            Assert.assertEquals(0, id.getAllParameters().size());
            Assert.assertEquals("kylin_metadata", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@");
            Assert.assertEquals("hello", id.getIdentifier());
            Assert.assertEquals("", id.getScheme());
            Assert.assertEquals(0, id.getAllParameters().size());
            Assert.assertEquals("hello", id.toString());
        }
        {
            StorageURL id = new StorageURL("hello@hbase,a");
            Assert.assertEquals("hello", id.getIdentifier());
            Assert.assertEquals("hbase", id.getScheme());
            Assert.assertEquals(1, id.getAllParameters().size());
            Assert.assertEquals("", id.getParameter("a"));
            Assert.assertEquals("hello@hbase,a", id.toString());
        }
    }
    
    @Test
    public void testValueOfCache() {
        StorageURL id1 = StorageURL.valueOf("hello@hbase");
        StorageURL id2 = StorageURL.valueOf("hello@hbase");
        StorageURL id3 = StorageURL.valueOf("hello @ hbase");
        StorageURL id4 = StorageURL.valueOf("hello@hbase,a=b");
        Assert.assertTrue(id1 == id2);
        Assert.assertTrue(id1 != id3);
        Assert.assertTrue(id1.equals(id3));
        Assert.assertTrue(id2 != id4);
        Assert.assertTrue(!id2.equals(id4));
    }
}
