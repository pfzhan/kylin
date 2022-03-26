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

package org.apache.kylin.query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class SlowQueryDetectorTest {

    private SlowQueryDetector slowQueryDetector = new SlowQueryDetector(500, 10 * 1000);
    private int interruptNum = 0;

    @Before
    public void setUp() throws Exception {
        slowQueryDetector.start();
        interruptNum = 0;
    }

    @After
    public void after() throws Exception {
        slowQueryDetector.interrupt();
        interruptNum = 0;
    }

    @Test
    public void testCheckStopByUser() throws InterruptedException {

        Thread rt1 = new Thread() {

            private Thread t;
            private String threadName = "testCheckStopByUser";

            public void run() {
                slowQueryDetector.queryStart("123");
                int i = 0;
                while (i <= 5) {
                    try {
                        // will be interrupted and throw InterruptedException
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        interruptNum++;
                        // assert trigger InterruptedException
                        // Assert.assertNotNull(e);
                    } finally {
                        i++;
                    }
                }
                slowQueryDetector.queryEnd();
            }

            public void start() {
                if (t == null) {
                    t = new Thread(this, threadName);
                    t.start();
                }
            }

        };

        rt1.start();
        TimeUnit.SECONDS.sleep(1);

        for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
            e.setStopByUser(true);
            e.getThread().interrupt();
        }

        TimeUnit.SECONDS.sleep(2);
        // query is running
        Assert.assertEquals(1, SlowQueryDetector.getRunningQueries().values().size());
        // interrupt Number > 1
        Assert.assertTrue(interruptNum > 1);

        TimeUnit.SECONDS.sleep(2);
        // query is stop
        Assert.assertEquals(0, SlowQueryDetector.getRunningQueries().values().size());

    }

}
