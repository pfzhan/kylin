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

package org.apache.kylin.test.service;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.service.BadQueryDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BadQueryDetectorTest {

    @Before
    public void setup() {
        System.setProperty("KYLIN_CONF", "../examples/test_case_data/localmeta");
    }

    @After
    public void after() throws Exception {
        System.clearProperty("KYLIN_CONF");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test() throws InterruptedException {
        int alertMB = BadQueryDetector.getSystemAvailMB() * 2;
        int alertRunningSec = 5;
        String mockSql = "select * from just_a_test";
        final ArrayList<String[]> alerts = new ArrayList<>();

        BadQueryDetector badQueryDetector = new BadQueryDetector(alertRunningSec * 1000, alertMB, alertRunningSec,
                1000);
        badQueryDetector.registerNotifier(new BadQueryDetector.Notifier() {
            @Override
            public void badQueryFound(String adj, float runningSec, long startTime, String project, String sql,
                    String user, Thread t) {
                alerts.add(new String[] { adj, sql });
            }
        });
        badQueryDetector.start();

        {
            Thread.sleep(1000);

            SQLRequest sqlRequest = new SQLRequest();
            sqlRequest.setSql(mockSql);
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, "user");

            // make sure bad query check happens twice
            Thread.sleep((alertRunningSec * 2 + 1) * 1000);

            badQueryDetector.queryEnd(Thread.currentThread(), BadQueryEntry.ADJ_PUSHDOWN);
        }

        badQueryDetector.interrupt();

        assertEquals(2, alerts.size());
        // second check founds a Slow
        assertArrayEquals(new String[] { BadQueryEntry.ADJ_SLOW, mockSql }, alerts.get(0));
        // end notifies a Pushdown
        assertArrayEquals(new String[] { BadQueryEntry.ADJ_PUSHDOWN, mockSql }, alerts.get(1));
    }
}
