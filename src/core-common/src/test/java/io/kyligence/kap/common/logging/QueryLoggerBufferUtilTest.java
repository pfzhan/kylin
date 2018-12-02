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

package io.kyligence.kap.common.logging;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryLoggerBufferUtilTest {

    private final String LOGGER_NAME = "io.kyligence.kap.query.*";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void teardown() {
        QueryContext.reset();
        QueryLoggerBufferUtil.clear();
    }

    @Test
    public void testMark() {
        final QueryContext queryContext = QueryContext.current();

        Assert.assertFalse(QueryLoggerBufferUtil.isMark());

        QueryLoggerBufferUtil.mark(queryContext.getQueryId());
        Assert.assertTrue(QueryLoggerBufferUtil.isMark());

        QueryLoggerBufferUtil.clear();
        Assert.assertFalse(QueryLoggerBufferUtil.isMark());
    }

    @Test
    public void testBasic() throws IOException {
        final int arg1 = 0;
        final boolean arg2 = true;
        final String arg3 = "arg3";
        final Throwable throwable = new RuntimeException("error message");

        final Logger mockOutput = Mockito.mock(Logger.class);

        final QueryContext queryContext = QueryContext.current();

        {
            final String infoMsg = "info level simple message.";
            QueryLoggerBufferUtil.mark(queryContext.getQueryId());
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.INFO.toString(), infoMsg);
            QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

            Mockito.verify(mockOutput, Mockito.only()).info(infoMsg);
            Mockito.reset(mockOutput);
        }

        {
            final String warnMsg = "warn level message with arguments [{}, {}, {}]";
            QueryLoggerBufferUtil.mark(queryContext.getQueryId());
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.WARN.toString(), warnMsg, arg1, arg2, arg3);
            QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

            Mockito.verify(mockOutput, Mockito.only()).warn(warnMsg, arg1, arg2, arg3);
            Mockito.reset(mockOutput);
        }

        {
            final String errorMsg = "error level message with exception";
            QueryLoggerBufferUtil.mark(queryContext.getQueryId());
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.ERROR.toString(), errorMsg, throwable);
            QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

            Mockito.verify(mockOutput, Mockito.only()).error(errorMsg, throwable);
            Mockito.reset(mockOutput);

        }

        {
            final String errorMsg = "error level message with arguments [{}, {}, {}] and exception";
            QueryLoggerBufferUtil.mark(queryContext.getQueryId());
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.ERROR.toString(), errorMsg, arg1, arg2, arg3,
                    throwable);
            QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

            Mockito.verify(mockOutput, Mockito.only()).error(errorMsg, arg1, arg2, arg3, throwable);
            Mockito.reset(mockOutput);
        }

        {
            QueryLoggerBufferUtil.mark(queryContext.getQueryId());
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.INFO.toString(), "message1");
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.WARN.toString(), "message2");
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.ERROR.toString(), "message3");
            QueryLoggerBufferUtil.buffer(queryContext.getQueryId(), Level.ERROR.toString(), "message3");
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.DEBUG.toString(), "message4");
            QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.TRACE.toString(), "message5");
            QueryLoggerBufferUtil.buffer(queryContext.getQueryId(), Level.TRACE.toString(), "message5");
            QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

            Mockito.verify(mockOutput, Mockito.times(1)).info("message1");
            Mockito.verify(mockOutput, Mockito.times(1)).warn("message2");
            Mockito.verify(mockOutput, Mockito.times(2)).error("message3");
            Mockito.verify(mockOutput, Mockito.times(1)).debug("message4");
            Mockito.verify(mockOutput, Mockito.times(2)).trace("message5");
        }

    }

    @Test
    public void testWithSubthread() throws InterruptedException {
        final int threadCount = 5;
        final CountDownLatch countDown = new CountDownLatch(threadCount);
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount,
                new NamedThreadFactory(QueryLoggerBufferUtilTest.class.getSimpleName()));

        final Logger mockOutput = Mockito.mock(Logger.class);

        final QueryContext queryContext = QueryContext.current();

        QueryLoggerBufferUtil.mark(queryContext.getQueryId());
        QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.INFO.toString(), "start to multithreading logging");

        for (int i = 1; i <= threadCount; i++) {
            final int flag = i;
            executorService.execute(() -> {
                QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.INFO.toString(), "{} logging",
                        Thread.currentThread().getName());

                if (flag == threadCount) {
                    QueryLoggerBufferUtil.buffer(queryContext.getQueryId(), Level.INFO.toString(),
                            "logging amount is {}", threadCount);
                }

                countDown.countDown();
            });
        }

        countDown.await();
        QueryLoggerBufferUtil.bufferIfNecessary(LOGGER_NAME, Level.INFO.toString(), "multithreading logging finished");
        QueryLoggerBufferUtil.dump(queryContext.getQueryId(), mockOutput);

        // start & end
        Mockito.verify(mockOutput, Mockito.times(2)).info(Mockito.anyString());
        // subthread logging
        Mockito.verify(mockOutput, Mockito.times(5)).info(Mockito.anyString(), new Object[] { Mockito.anyString() });
        Mockito.verify(mockOutput, Mockito.times(1)).info(Mockito.anyString(), new Object[] { Mockito.anyInt() });

        executorService.shutdownNow();
    }

}
