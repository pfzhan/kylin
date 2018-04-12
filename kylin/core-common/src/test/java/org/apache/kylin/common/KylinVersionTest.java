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

public class KylinVersionTest {
    @Test
    public void testNormal() {
        KylinVersion ver1 = new KylinVersion("2.1.0");
        Assert.assertEquals(2, ver1.major);
        Assert.assertEquals(1, ver1.minor);
        Assert.assertEquals(0, ver1.revision);
    }

    @Test
    public void testNoRevision() {
        KylinVersion ver1 = new KylinVersion("2.1");
        Assert.assertEquals(2, ver1.major);
        Assert.assertEquals(1, ver1.minor);
        Assert.assertEquals(0, ver1.revision);
    }

    @Test
    public void testToString() {
        KylinVersion ver1 = new KylinVersion("2.1.7.321");
        Assert.assertEquals(2, ver1.major);
        Assert.assertEquals(1, ver1.minor);
        Assert.assertEquals(7, ver1.revision);
        Assert.assertEquals(321, ver1.internal);
        Assert.assertEquals("2.1.7.321", ver1.toString());
    }
    
    @Test
    public void testCompare() {
        Assert.assertEquals(true, KylinVersion.isBefore200("1.9.9"));
        Assert.assertEquals(false, KylinVersion.isBefore200("2.0.0"));
        Assert.assertEquals(true, new KylinVersion("2.1.0").compareTo(new KylinVersion("2.1.0.123")) < 0);
    }
}