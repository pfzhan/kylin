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

package org.apache.kylin.metadata.cachesync;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.Broadcaster.Listener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BroadcasterTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasics() throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(KylinConfig.getInstanceFromEnv());
        final AtomicInteger i = new AtomicInteger(0);

        broadcaster.registerStaticListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(2, i.incrementAndGet());
            }
        }, "test");

        broadcaster.registerListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(1, i.incrementAndGet());
            }
        }, "test");

        broadcaster.notifyListener("test", Event.UPDATE, "");

        Broadcaster.staticListenerMap.clear();
    }

    @Test
    public void testNotifyNonStatic() throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(KylinConfig.getInstanceFromEnv());
        final AtomicInteger i = new AtomicInteger(0);

        broadcaster.registerStaticListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                throw new IllegalStateException("Should not notify static listener.");
            }
        }, "test");

        broadcaster.registerListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(1, i.incrementAndGet());
            }
        }, "test");

        broadcaster.notifyNonStaticListener("test", Event.UPDATE, "");

        Broadcaster.staticListenerMap.clear();
    }
}
