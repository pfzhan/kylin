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
package io.kyligence.kap.spark.common.logging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class AbstractHdfsLogAppenderTestBase extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    protected boolean RunBenchmark(final AbstractHdfsLogAppender hdfsLogAppender, final int threadNumber,
            final int size, final long timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        hdfsLogAppender.setLayout(new Layout() {
            @Override
            public String format(LoggingEvent loggingEvent) {
                return loggingEvent.toString() + "\n";
            }

            @Override
            public boolean ignoresThrowable() {
                return false;
            }

            @Override
            public void activateOptions() {

            }
        });

        hdfsLogAppender.activateOptions();

        final LoggingEvent loggingEvent = new LoggingEvent("1", Logger.getLogger("HdfsLogAppender"), 10, Level.ERROR,
                "RunBenchmark", null);
        final AtomicLong atomicLong = new AtomicLong();
        final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);
        final Semaphore semaphore = new Semaphore(size);
        final long l = System.currentTimeMillis();
        for (int i = 0; i < threadNumber; i++) {
            new Thread(() -> {
                try {
                    while (true) {
                        if (semaphore.tryAcquire(10L, TimeUnit.MILLISECONDS)) {
                            hdfsLogAppender.append(loggingEvent);
                            atomicLong.incrementAndGet();
                            if (atomicLong.get() % 100000 == 0) {
                                System.out.println(atomicLong.get() / ((System.currentTimeMillis() - l + 1000) / 1000));
                            }
                        } else {
                            break;
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        while (hdfsLogAppender.getLogBufferQue().size() > 0) {
            if ((System.currentTimeMillis() - start) > timeout) {
                return false;
            }
        }
        hdfsLogAppender.close();
        return true;
    }
}
